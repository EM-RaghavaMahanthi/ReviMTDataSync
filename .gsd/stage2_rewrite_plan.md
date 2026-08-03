# Lambda 2 rewrite — account-independent, 9 tables, one shape

Supersedes the "fix the gaps" approach in [stage2_plan_and_gaps.md](stage2_plan_and_gaps.md).
Those seven gaps are all symptoms of the same thing: `stg_to_main_bulk` is the onboarding
pipeline with `location_id` deleted, so it is still per-account and still carries the
onboarding's bespoke per-table bookkeeping. Rewriting to one shape removes most of them
outright rather than patching each.

---

## The shape

**One pass, all accounts, no `account_id` anywhere.** Stage 1 already loads staging for
every account at once; stage 2 should insert the same way.

Per table, six numbers and nothing else:

| | |
|---|---|
| `staged` | rows in staging |
| `duplicates` | staging rows sharing a business key — **expect 0**, stage 1 dedups and the unique index enforces it |
| `present` | staged rows already in main |
| `missing` | staged rows not in main → what we intend to insert |
| `inserted` | what the INSERT actually wrote |
| `blocked` | `missing − inserted` — rows whose parent could not be resolved |

**No pre-computed "why not" counts, and no raising on mismatch.** Today each processor
counts `invalid_customer_records`, `missing_dependencies`, `unnecessary_records`,
`null_mandatory_names` … then subtracts them and raises if the arithmetic disagrees. That
arithmetic is where gap 3 lives — the sets overlap, a row that is both "customer missing"
and "already exists" is subtracted twice, and it raises on healthy data. Insert first, then
report `blocked` as the residue. Same information, no double-counting, no false failures.

This collapses **110 step functions across 9028 lines** into one generic runner plus 13
INSERT statements.

---

## The 9 tables

Dependency order — parents before children, because each INSERT resolves its `*_ref_id` by
joining the parent's target table.

| # | table | resolves against | inserts |
|---|---|---|---|
| 1 | `class_sessions` | — | 1 |
| 2 | `membership_instances` | — | 1 |
| 3 | `credit_transactions` | customers | 1 |
| 4 | `credit_transactions_orders` | customers | 1 |
| 5 | `membership_transactions` | customers, membership_instances | 1 |
| 6 | `membership_transactions_orders` | customers, membership_instances | 1 |
| 7 | `orders` | customers | 1 |
| 8 | `order_lines` | orders, credit_transactions_orders, membership_transactions_orders | **3** — credit / membership / other |
| 9 | `reservations` | customers, class_sessions, credit_transactions, membership_transactions | **3** — credit / membership / no_transactions |

13 INSERT statements over 9 tables. `order_lines` and `reservations` stay split because the
join differs per variant; the counting wrapper around them is identical.

**`customers` is a dependency of six of the nine and we never insert it.** It is
`cfg.RDS_OWNED` — back-synced to RDS on its own ~3 min cadence. So a row whose customer has
not yet landed in RDS is `blocked`, not lost, and clears on the next run. That is the single
most likely source of a non-zero `blocked` count, and a reason to run promote *after* giving
the customer sync a few minutes.

---

## One post-pass, at the end

**`first_timer`** — keep `step_3_recalculate_first_timer`, drop the account predicate so it
runs once for everything. Delete `step_1_calculate_first_timer_field_with_pandas` entirely:
it pulls staging into pandas to pre-set the flag "~99% correct at insert time", and the SQL
recalculates all of it anyway. The recalc already picks the correct row per
`(account_id, customer_id)` with `DISTINCT ON` and touches only rows where `first_timer IS
DISTINCT FROM` the computed value — a small number. Deleting step 1 removes the pandas
dependency and a batched 1000-row update loop.

It reads `reservations` and `class_sessions`, so it runs after all 13 inserts.

**`customer_class_dates` comes out.** `_update_customer_class_dates` is deleted, not
generalised — there is already a script that updates class dates for every account at once,
and that is the authority. This also settles the open question about stage 2 writing to
`customers`: after this, neither lambda writes to that table at all.

---

## What this removes

- Every `account_id` parameter and predicate in the processors.
- `step_5_calculate_expected_ready` and every count that feeds it — 6 variants of the
  subtraction, plus `step_1b_count_unnecessary_records`, `step_3_count_records_with_missing_dependencies`,
  `step_4_count_orders_with_missing_customers`, `step_3b_count_null_names`,
  `step_3c_count_null_notes` and the rest of the bespoke per-table counting.
- The `raise` on count mismatch (gap 3).
- `step_1_calculate_first_timer_field_with_pandas` and the pandas import.
- The three orphaned processors — `customers_01`, `customer_notes_01a`, `customer_tags_01b`
  come out of `PROCESSING_ORDER` with gap 1. Their modules can stay on disk.

Gaps 1, 2 and 3 from the review disappear as a consequence rather than needing individual
fixes. Gap 7 (176 hardcoded staging names) is worth doing in the same pass — derive from
`cfg.staging_name()` so a stage 1 config change cannot silently orphan a processor again.

---

## Duplicate parents — fix the join, do not choose how to fail

The problem, concretely. Every child insert resolves its parent by joining on the business
key:

```sql
INSERT INTO public.orders (..., customer_ref_id)
SELECT stg.order_id, ..., c.id
FROM stg_orders_bulk stg
INNER JOIN customers c ON stg.customer_id = c.customer_id AND c.account_id = stg.account_id
```

The onboarding pipeline scopes its "does this customer exist" check per **location**, so it
can create two `customers` rows for one `customer_id` under two locations of one account.
When that happens this join returns two rows for one staging row, and the order is inserted
**twice**. `_check_duplicate_parents` exists to detect that and refuse the account.

Reconcile confirmed the duplicates are real — 10 rows in a 60-account window.

Refusing is the wrong answer for an account-independent run: one duplicate anywhere would
block all 60 accounts. Make the lookup return exactly one parent instead:

```sql
INNER JOIN (
  SELECT DISTINCT ON (account_id, customer_id) id, account_id, customer_id
  FROM customers
  ORDER BY account_id, customer_id, id      -- lowest id wins, deterministic
) c ON c.customer_id = stg.customer_id AND c.account_id = stg.account_id
```

One row per `(account_id, business key)` by construction, so the fan-out cannot happen and
nothing needs refusing. This is the alternative [review.md](review.md) #1 already suggests.

Apply it to every parent lookup in the 13 inserts. `_check_duplicate_parents` then stops
being a gate and becomes a **report** — still worth running and surfacing, because duplicate
parents are a real data problem someone should fix backend-side, but no longer something
that blocks a promote.

## Dry run

`"insert": false` — confirmed wanted. Nearly free in this shape: everything up to `inserted`
is already read-only, so the flag simply stops before the INSERT and still reports `staged`,
`duplicates`, `present` and `missing`. `inserted` and `blocked` come back as `null` rather
than `0`, so a dry run cannot be mistaken for a real one that inserted nothing.

Default `false`, matching stage 1's `update` flag, with the same precedence: event field
wins, then an env var, then false.

---

## Order of work

1. Generic runner + the 6-number report; one table end to end (`class_sessions` — no deps).
2. Port the other 12 inserts into it, in dependency order.
3. Drop the account predicate from the first_timer recalc; delete the pandas step and
   `_update_customer_class_dates`.
4. Trim `PROCESSING_ORDER`, derive it from `cfg.STAGING_ORDER`.
5. Rewrite `deploy_stg_to_main_bulk.sh` the way stage 1's was — create-if-missing, clone
   layers/VPC, `revi-dlk-gold-lambda-exec`, and `--role` on update as well as create.
6. `insert: false` against one account, then all; then the real thing.
7. Step Function last, once both functions exist and have ARNs.
