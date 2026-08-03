# Lambda 2 — `stg_to_main_bulk`: what it is, and what has to change

Companion to [plan_and_gaps.md](plan_and_gaps.md), which covers Lambda 1. Nothing here has
been executed — `revi-bulk-stg-to-main` does not exist as a function yet, and the backend is
live, so this is a static review only.

---

## What it does

Stage 1 loads Silver into staging and **UPDATEs** rows that already exist. Stage 2 is the
other half: it **INSERTs** the rows that do not. Between them they cover the two shortfalls
`reconcile` reports — `differing` for stage 1, `missing` for stage 2.

Three actions ([handlers/stg_to_main_bulk.py](../handlers/stg_to_main_bulk.py)):

| action | what it does |
|---|---|
| `promote` | one account: run every processor in dependency order, inserting what is absent |
| `vacuum` | one `VACUUM ANALYZE` pass over the ten target tables |
| `verify` | read-only: report duplicate parents for an account |

It is a copy of `stg_db_services/`, generalised from `(account_id, location_id, engine)` to
`(account_id, engine)`. The mapping is 1:1 — every original has a counterpart, each a little
shorter for the removed location predicates — and the transformation is mechanical: drop
`location_id` from the signature, rename the staging table, delete the location clauses.

Two structural differences from the onboarding version, both deliberate:

- **No location scoping.** Bulk staging is unique on `(account_id, business key)`, so a delta
  spanning several locations needs no per-location loop. 193 predicates removed.
- **The credit/membership pairs read their own staging tables**, because Silver ships them
  split. The onboarding `isin_order_line` / `isin_reservation` filters are gone with them.

### Verified sound

- `_base/dedup.py` is correctly account-scoped in **both** its count and its `DELETE` — no
  cross-account deletion.
- `_check_duplicate_parents` covers all 8 parent tables and reports per table, so a promote
  refuses rather than silently double-inserting. This is the guard for
  [review.md](review.md) #1, and reconcile has since confirmed duplicates are real —
  10 of them in a 60-account window.
- 75 of the 78 staging counts across the processors are correctly scoped to `account_id`.

---

## Gaps

### 1 — BLOCKER: three processors read staging tables that no longer exist

`PROCESSING_ORDER` runs 12 processors. Stage 1 now creates **9** staging tables — the four
`cfg.RDS_OWNED` tables were dropped when the pipeline narrowed. Three processors still read
the tables that went with them:

| processor | reads | exists? |
|---|---|---|
| `customers_01` | `stg_customers_bulk` | **no** |
| `customer_notes_01a` | `stg_user_notes_bulk` | **no** |
| `customer_tags_01b` | `stg_user_tags_bulk`, `stg_customer_tags_bulk` | **no** |

`customers_01` is *first* in the order, so `promote` fails on its opening query with
`relation "stg_customers_bulk" does not exist` — before touching anything else.

**Fix:** drop those three entries from `PROCESSING_ORDER`. The remaining nine map exactly
onto the nine tables stage 1 stages, which is the right invariant — ideally derive the order
from `cfg.STAGING_ORDER` rather than repeating it, so the two cannot drift again. The three
processor modules can stay on disk for whenever those tables come back.

### 2 — Unscoped staging count makes the validation raise for every account

`class_sessions` and `membership_instances` call `step_1_count_staging_total(account_id,
engine)`, which runs:

```sql
SELECT COUNT(*) FROM public.stg_<table>_bulk     -- no WHERE; account_id is ignored
```

Correct when staging held one account. It now holds all of them, and the count feeds a
strict validation against per-account numbers:

```python
expected_ready = total_staging - already_exist_in_main   # all accounts − this account
if ready_to_insert != expected_ready: raise
```

So both raise for every account as soon as more than one is staged. Both are in the nine.
(`customers` has the same bug and is moot under gap 1.)

**Fix:** add `WHERE account_id = :account_id`. Three lines.

### 3 — Overlapping subtraction: the strict validations can raise on healthy data

[review.md](review.md) #5 flagged this as a hazard; it is confirmed. Several processors
compute the expected insert count by subtracting sets that are **not disjoint**:

```python
# credit_transactions.py:283
expected_ready = total_staging - unnecessary_records \
                 - missing_customer_transactions - already_exist_in_main
```

`missing_customer_transactions` counts staging rows whose customer is absent
(`c.customer_id IS NULL`); `already_exist_in_main` counts rows already in the target. **A row
can be both** — and is then subtracted twice, making `expected_ready` too low, so
`ready_to_insert != expected_ready` and the processor raises on data that is perfectly fine.

Same shape in `credit_transactions_orders:303`, `customer_notes:212` and
`membership_transactions_orders:150`. It exists in the onboarding pipeline too, but delta
windows are dominated by already-existing rows, so it is far likelier to fire here.

**Fix:** count the union once — a single query with the disjunction — rather than
subtracting independently-counted sets.

### 4 — `_update_customer_class_dates` writes to a table RDS now owns

The post-promote step recomputes `customers.last_class_date` / `next_class_date` from the
promoted reservations. Its own comment explains those columns sit in the customers spec's
`no_update` set so the stale pass will not fight it — but `customers` is now in
`cfg.RDS_OWNED` and is not staged, updated or reconciled by this pipeline at all.

**Needs a decision, not a fix:** either it stays (this pipeline still owns those two derived
columns even though it owns nothing else on `customers`), or it goes with the rest. Leaving
it undecided means a step writing to a table the docs say we do not touch.

### 5 — `is_valid = TRUE` filters are inert

The three `order_lines` processors filter `stg.is_valid = TRUE` in six places. Stage 1 stages
`is_valid` as a **constant `TRUE`** because Silver carries neither `is_valid` nor
`child_orders`, so every one of those filters passes unconditionally.

Not a bug — the filters are harmless and correct to keep for when the column becomes real —
but it means deferred-payment placeholder `order_lines` cannot be detected in the bulk path.
Invalidation only ever arrives as `deleted_at` through the stale update. Worth stating
plainly so nobody reads the filter and assumes protection that is not there.

### 6 — Deployment does not exist

- `revi-bulk-stg-to-main` is not a Lambda function.
- [`deploy_stg_to_main_bulk.sh`](../deployments/deploy_stg_to_main_bulk.sh) is the old
  update-only shape: `update-function-code` only, no create, no role, no layers, no VPC. It
  will fail exactly the way stage 1's did.

**Fix:** the same treatment `deploy_s3_to_stg_bulk.sh` got — create-if-missing, clone
runtime/arch/layers/VPC from a reference function, `revi-dlk-gold-lambda-exec` as the role,
and pass `--role` on update as well as create (the trap that cost hours on stage 1).

### 7 — Hardcoded staging names

The processors hardcode `stg_*_bulk` as **176 string literals across 22 files** rather than
calling `cfg.staging_name()`. That is how gap 1 became possible: stage 1 narrowed its table
set and nothing in stage 2 noticed. Low urgency on its own, high value alongside gap 1 since
the edit sites overlap.

---

## Suggested order

1. **Gap 1** — trim `PROCESSING_ORDER` to the nine, ideally deriving it from
   `cfg.STAGING_ORDER`. Nothing else can be tested until `promote` gets past its first step.
2. **Gap 2** — three-line fix, removes a guaranteed failure.
3. **Gap 6** — rewrite the deploy script; needed before anything runs at all.
4. **Gap 3** — the union-count fix. Do it before the first real promote, since it raises on
   healthy data and would look like a real failure.
5. **Gap 4** — decide on the class-dates step.
6. **Gap 7** — route through `cfg.staging_name()` while gap 1 is open in the same files.
7. Then the Step Function, which needs both functions to exist and to have ARNs.

## Open questions

1. **Does `_update_customer_class_dates` stay?** (gap 4) It is the only thing in either
   lambda that writes to `customers`.
2. **What should `promote` do when `_check_duplicate_parents` fires?** It refuses today, and
   reconcile says duplicates exist in live data — so on a real rollover some accounts will be
   refused. Is `allow_duplicate_parents: true` the intended escape hatch, or should the
   duplicates be resolved backend-side first?
3. **Does stage 2 need a reconcile-style dry run?** Stage 1 got `update: false`; stage 2 has
   no equivalent, so the first execution of the insert path is against production.
