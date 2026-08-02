# Lambda 1 — `s3_to_stg_bulk`: what was asked, what was built, what is missing

Scope: **first lambda only**. `stg_to_main_bulk` is the second lambda (a copy of `stg_to_main`
generalised to bulk accounts) and is not assessed here.

---

## Status — all gaps closed

| Gap | Change | Where |
|---|---|---|
| G1 | `update_stale` is bulk: every statement scoped `account_id = ANY(:account_ids)`. **30 statements per run regardless of account count**, was ~50 per account. Per-account counts kept via `GROUP BY`; per-account failure isolation via `_with_fallback`. | [update_stale.py](../s3_to_stg_bulk/update_stale.py) |
| G2 | Option B — onboarding's invariant ported: **4 ref repairs, was 14**; 9 business keys frozen; `LATERAL` → plain equi-join; parent resolved from the **target's** key, not staging's. | [config.py](../s3_to_stg_bulk/config.py), [update_stale.py](../s3_to_stg_bulk/update_stale.py) |
| G3 | `resolve_accounts()` runs **before** the load, so an invalid id never reaches Athena or staging. | [staging.py](../s3_to_stg_bulk/staging.py) |
| G4 | Predicate is `status='ACTIVE' AND crm_api_end_point IS NOT NULL`; `crm_config` dropped. | [staging.py](../s3_to_stg_bulk/staging.py) |
| G5 | `start_datetime` / `end_datetime`; `BULK_DELTA_MINUTES` 60 → **10**, parameter still wins. | [handlers](../handlers/s3_to_stg_bulk.py), [bulk_config.py](../core/bulk_config.py) |
| G6 | Response reports `requested` / `resolved` / `processed` / `no_rows_in_window` / `rejected`, each rejection with a reason. | [handlers](../handlers/s3_to_stg_bulk.py) |
| G7 | The standalone `update_stale` action goes through the same `resolve_accounts` path. | [handlers](../handlers/s3_to_stg_bulk.py) |
| — | `dry_run` → **`update`** (default `False` = dry run), matching `db_services/update_stale`. | throughout |

Staging table names stay `stg_*_bulk` — existing convention kept, no rename.

**Verified statically** (no DB/AWS access): all four repair statements and all 22 count/update
statements render; parens balance; no scalar `:account_id` survives; every SET column is staged,
non-frozen and has a Silver source; every NOT NULL column is `COALESCE`-guarded on both sides; the
recency gate picks `silver_inserted_at` only where `updated_at` has no Silver source; the
ref/frozen-key invariant holds in both directions. ASL still parses; all changed modules compile.
**Nothing has been executed against a database.**

---

## The spec as stated

1. Input is `account_ids`, `start_datetime`, `end_datetime`.
2. First step: fetch all active accounts, intersect with the supplied `account_ids` (which may
   contain invalid ones), and process **only** that filtered set.
3. Staging is loaded **directly** from the Silver tables in S3 — no formatting / transform layer,
   unlike the onboarding pipeline.
4. `update_stale` is the step that differs: it must run **in bulk, not account by account**.
5. `update_stale` covers **extra columns**, along with `updated_by` / `updated_at`.
6. **Do not** touch DB internals — no `ref_id`, no `id` resolution.

---

## Done and matching the spec

| # | Requirement | Where | Notes |
|---|---|---|---|
| 3 | Direct Silver → staging, no formatting layer | [staging.py:181](../s3_to_stg_bulk/staging.py#L181) | Athena → S3 result CSV → `COPY … FROM STDIN`, streamed through [`_SkipHeader`](../s3_to_stg_bulk/staging.py#L143) so nothing is materialised in the Lambda. Correct call — `credit_transactions` is ~500k rows for a single account. |
| 3 | Single source of truth for the shape | [config.py](../s3_to_stg_bulk/config.py) | 13 table specs; staging DDL, Athena SELECT list, COPY column order and the stale column set all derive from them. The steps cannot drift apart. |
| 3 | Dedup to one row per key | [staging.py:103-140](../s3_to_stg_bulk/staging.py#L103-L140) | `ROW_NUMBER() … PARTITION BY account_id, <business key> ORDER BY silver_inserted_at DESC`. Valid because Silver is append-only. Unique index on `(account_id, *key)` backs it. |
| 5 | Extra columns | [config.mutable_columns](../s3_to_stg_bulk/config.py#L675) | **119 mutable columns across 11 tables**, vs **34 across 9** in [db_services/update_stale.py](../db_services/update_stale.py). Per table: customers 24, reservations 18, credit_transactions_orders 11, class_sessions 10, credit_transactions 10, order_lines 10, membership_instances 9, membership_transactions_orders 9, orders 8, membership_transactions 8, user_notes 2. |
| 5 | `updated_by` / `updated_at` stamped | [update_stale.py:115](../s3_to_stg_bulk/update_stale.py#L115) | `updated_by = 1`, `updated_at = now()` appended to every SET list — same actor id as onboarding. |
| — | Location scoping dropped | [update_stale.py:133](../s3_to_stg_bulk/update_stale.py#L133) | Join is `account_id` + business key only. Correct for bulk. |
| — | NULL-safety | [update_stale.py:98](../s3_to_stg_bulk/update_stale.py#L98), [:109](../s3_to_stg_bulk/update_stale.py#L109) | NOT NULL columns use `COALESCE(s.col, t.col)` and are excluded from the diff test when staging is NULL, so a NULL from Silver cannot wipe a live value. |
| — | Spec/schema drift is survivable | [update_stale.py:54](../s3_to_stg_bulk/update_stale.py#L54) | Spec columns are intersected with `information_schema` once per cold start; a missing column logs a warning instead of failing the UPDATE. |
| — | Concurrency control | [staging.py:38](../s3_to_stg_bulk/staging.py#L38) | `stg_bulk_run` slot row (not `pg_advisory_lock`, which would not survive across Lambda invocations). Slot older than 6h can be taken over. |

---

## Gaps

### G1 — `update_stale` is still account-by-account (blocker; this is requirement 4)

[`update_stale_data_for_accounts`](../s3_to_stg_bulk/update_stale.py#L371) is a Python loop:

```python
for account_id in account_ids:
    result = update_stale_data(engine, account_id, dry_run)
```

and every statement underneath is parameterised `WHERE t.account_id = :account_id`
([`_count`](../s3_to_stg_bulk/update_stale.py#L162), [`_update`](../s3_to_stg_bulk/update_stale.py#L181),
[`_repair_ref`](../s3_to_stg_bulk/update_stale.py#L240)). Per account that is 11 counts + 11 updates
+ 28 ref statements ≈ **50 round-trips**; for N accounts, 50N. This is exactly the shape the spec
says not to build.

**Fix.** The join already carries `s.account_id = t.account_id`, so the account predicate is
redundant for correctness — it exists only to scope the batch. Drop the parameter and scope the
whole statement instead:

```sql
UPDATE customers t
SET …
FROM "stg_customers_bulk" s
JOIN filtered_accounts fa ON fa.account_id = s.account_id   -- the validated set from G3
WHERE s.account_id = t.account_id
  AND s.customer_id = t.customer_id
  AND <recency gate> AND <diff predicate>
```

That collapses 50N statements to **22 total** (11 count + 11 update), one per table, still in
dependency order.

**Trade-off to confirm:** the per-account loop currently gives per-account failure isolation and a
per-account result breakdown (`per_account` in the response). Bulk statements mean one table's
failure fails the whole run for every account. If the per-account rollup is still wanted for
reporting, it can be a single `GROUP BY account_id` count query rather than N separate passes.

### G2 — ref_id handling: the built version broke the invariant the onboarding one holds

Your premise is right as far as it goes: staging carries no `*_ref_id`, the RDS row already has its
ref_id from insert time, and stage 2 resolves ref_ids for rows it newly inserts. So for a row whose
parent pointer does not move, there is nothing to resolve.

**The exception is the case the onboarding `update_stale` exists to handle.** A ref_id goes stale
when the update *itself* changes the business key that the ref mirrors. If
`reservations.credit_transactions_id` is updated 100 → 200, then `credit_transactions_ref_id` still
points at the `credit_transactions` row for 100. The row is now internally inconsistent, and nothing
downstream fixes it — stage 2 only touches rows it inserts.

#### What the onboarding version actually does

It repairs a ref_id **if and only if it updates the business key that ref mirrors.** That is a tight,
deliberate invariant, not an oversight:

| Table | Business keys it updates | ref_ids it repairs |
|---|---|---|
| [`membership_transactions`](../db_services/update_stale.py#L298) | `membership_instances_id` | `membership_instances_ref_id` |
| [`membership_transactions_orders`](../db_services/update_stale.py#L362) | `membership_instances_id` | `membership_instances_ref_id` |
| [`reservations`](../db_services/update_stale.py#L498) | `credit_transactions_id`, `membership_transactions_id` | `credit_transactions_ref_id`, `membership_transactions_ref_id` |
| [`order_lines`](../db_services/update_stale.py#L429) | none (only `title`, `transaction_type`) | none |
| `orders`, `class_sessions`, `credit_transactions*` | none | none |

`reservations` updates neither `customer_id` nor `class_session_id`, and repairs neither
`customer_ref_id` nor `class_session_ref_id`. `order_lines` never touches `order_id`, so
`order_ref_id` is never repaired. **4 repairs across 3 tables** — exactly matching the 3 keys it moves.

So the commit's claim that onboarding "misses" `order_ref_id`, `class_session_ref_id` and
`customer_ref_id` is a misreading. Onboarding does not repair them because it never moves the keys
they mirror.

#### What the bulk version did

It broke the invariant on both sides at once. Widening the column set made **every** parent business
key mutable, and then 14 general-purpose repairs were added to compensate:

| ref column | mirrors | mutable in bulk spec? |
|---|---|---|
| `orders.customer_ref_id` | `customer_id` | yes |
| `credit_transactions.customer_ref_id` | `customer_id` | yes |
| `credit_transactions_orders.customer_ref_id` | `customer_id` | yes |
| `membership_transactions.customer_ref_id` / `.membership_instances_ref_id` | `customer_id` / `membership_instances_id` | yes |
| `membership_transactions_orders.membership_instances_ref_id` | `membership_instances_id` | yes |
| `order_lines.order_ref_id` / `.credit_transactions_ref_id` / `.membership_transactions_ref_id` | `order_id` / `credit_transactions_id` / `membership_transactions_id` | yes |
| `reservations.customer_ref_id` / `.class_session_ref_id` / `.credit_transactions_ref_id` / `.membership_transactions_ref_id` | `customer_id` / `class_session_id` / `credit_transactions_id` / `membership_transactions_id` | yes |
| `user_notes.customer_ref_id` | `customer_id` | no — part of the join key |

13 of the 14 refs mirror a key the bulk spec now makes mutable. The repair pass is not gratuitous
given that column set — it is load-bearing. Deleting it *without* also freezing the keys would leave
every one of those ref columns able to drift.

#### The two coherent options — needs your call

**Option A — freeze the parent business keys, then drop the repairs entirely.**
Move `customer_id`, `class_session_id`, `order_id`, `credit_transactions_id`,
`membership_transactions_id`, `membership_instances_id` into `no_update` (13 columns of the 119).
Stage 1 becomes a pure attribute refresh — names, dates, status, amounts, `deleted_at` — and no
ref_id can go stale, so there is genuinely nothing to check. This is the literal reading of "don't
touch internals like ref_ids and id", and it removes 28 of the ~50 statements per account plus open
risk #1 in [review.md](review.md) (duplicate-parent fan-out was only reachable through the parent joins).
**Cost:** re-parenting stops propagating — including
`reservations.credit_transactions_id`, which onboarding *does* maintain today. That is a real
regression against current behaviour if a reservation's credit genuinely changes in the CRM.

**Option B — port the onboarding invariant exactly.**
Keep the 3 business keys onboarding updates and the 4 repairs that pair with them; freeze the other
10 keys and drop the other 10 repairs. No regression against today, and the repair count drops from
14 to 4. The 4 survivors can also be written as plain equi-joins on the parent's
`(account_id, business_key)` — the `LEFT JOIN LATERAL … LIMIT 1` shape only exists to tolerate
duplicate parents, which is the thing risk #1 warns about.

**Recommendation: Option B.** It matches the spirit of "don't do ref_id resolution" — no
general-purpose FK repair pass, no `LATERAL` lookups, no duplicate-parent exposure — while keeping
the one narrow case where a ref_id provably goes stale. Option A is cleaner still, but only if you
confirm re-parenting never happens in practice.

### G3 — Accounts are filtered *after* the load, not before (requirement 2)

[`_stage`](../handlers/s3_to_stg_bulk.py#L114) does, in order:

```python
staged   = staging.load_all(engine, start, end, account_ids)   # raw event list, unvalidated
accounts = staging.promotable_account_ids(engine)              # staged ∩ active, computed after
```

Three consequences:

- Invalid or inactive ids in the event are **still scanned in Athena and COPYed into staging**.
  They are only dropped later, at the stale/promote step. Wasted scan, wasted COPY, wasted rows.
- When `account_ids` is empty the load covers **every account in the window** — the commit describes
  this as "accounts are discovered, not passed in", which is the opposite of the stated contract.
- The rejected list is derived from *what landed in staging*
  ([`promotable_account_ids`](../s3_to_stg_bulk/staging.py#L255)), so a requested account with no
  rows in the window is indistinguishable from one that was never requested.

**Fix.** Resolve the set first, then drive everything from it:

```python
requested = [int(a) for a in event["account_ids"]]
active    = staging.active_account_ids(engine)          # already exists
filtered  = [a for a in requested if a in active]
rejected  = [a for a in requested if a not in active]
if not filtered:
    raise ValueError(f"no valid active accounts in {requested}")
staged = staging.load_all(engine, start, end, filtered)  # Athena predicate now the validated list
```

`load_all` → `_select_sql` already emits `account_id IN (…)` when given a list
([staging.py:125-127](../s3_to_stg_bulk/staging.py#L125-L127)), so only the caller changes.

### G4 — Drop `crm_config` from the active-account predicate  ✅ **settled**

[staging.py:246-251](../s3_to_stg_bulk/staging.py#L246-L251) currently reads:

```sql
SELECT id FROM accounts
WHERE status = 'ACTIVE'
  AND crm_config        IS NOT NULL   -- not wanted
  AND crm_api_end_point IS NOT NULL   -- correct, keep
```

Confirmed target:

```sql
SELECT id FROM accounts
WHERE status = 'ACTIVE'
  AND crm_api_end_point IS NOT NULL
```

`crm_api_end_point IS NOT NULL` stays; `crm_config IS NOT NULL` comes out. This deliberately diverges
from [`db_client.get_active_accounts`](../clients/db_client.py#L229-L233), which the onboarding path
uses — worth a comment on the query so the difference is not "fixed" back later.

### G5 — Rename the window fields; `delta_minutes` stays  ✅ **settled**

Event uses `start_time` / `end_time` ([handlers/s3_to_stg_bulk.py:61-64](../handlers/s3_to_stg_bulk.py#L61-L64));
rename to `start_datetime` / `end_datetime`.

`delta_minutes` **stays**, with this precedence — parameter wins, else env var, and the env default
drops from 60 to **10**:

```python
minutes = int(event.get("delta_minutes") or settings.BULK_DELTA_MINUTES)   # already correct
```
```python
BULK_DELTA_MINUTES: int = 10      # was 60 — core/bulk_config.py:24
```

The resolution logic at [handlers/s3_to_stg_bulk.py:67](../handlers/s3_to_stg_bulk.py#L67) already
implements "parameter else env", so only the default value and the two field names change.

### G6 — `account_ids` needs validation and a three-way report

`account_ids` is supplied normally; **empty means all active accounts** ✅ (so the existing
empty-means-all behaviour stays — it just has to run through the validation path in G3 rather than
around it).

What is missing is the reporting. `skipped_inactive_accounts` is derived from what landed in staging,
so it conflates "you asked for an invalid account" with "this account had rows in Silver but is not
active", and cannot see "you asked for an account that had no rows in the window at all". The
response should name all three explicitly:

```json
{
  "requested":  [1410, 1411, 9999],
  "processed":  [1410, 1411],
  "rejected":   [{"account_id": 9999, "reason": "not ACTIVE or crm_api_end_point IS NULL"}],
  "no_rows_in_window": [1411]
}
```

### G8 — Staging table names: `stg_*_bulk` → `dlk_*_bulk`  ✅ **settled**

Target name is `dlk_{table_name}_bulk` — `dlk_customers_bulk`, `dlk_orders_bulk`, … Neither the
current `stg_*_bulk` nor the new `dlk_*_bulk` clashes with the onboarding `mt_*_details_dlk` tables,
but `dlk_` is the convention you want, and both bulk lambdas share these names so the rename has to
land in both at once.

In config it is two constants ([config.py:28-29](../s3_to_stg_bulk/config.py#L28-L29)):

```python
STAGING_PREFIX = "dlk_"     # was "stg_"
STAGING_SUFFIX = "_bulk"
```

**But `stg_to_main_bulk` does not call `cfg.staging_name()` — it hardcodes the names as string
literals: 176 occurrences across 23 files.** A `sed` over `stg_([a-z_]+)_bulk` → `dlk_\1_bulk` does
the rename safely, but the duplication is the real problem: the next rename is another 176-site
change, and a partial one leaves the two lambdas silently disagreeing about which table they are
talking to. Worth routing the second lambda through `cfg.staging_name()` while doing this — it is
the same edit sites either way.

### G7 — The standalone `update_stale` action inherits the same ordering problem

[`_update_stale`](../handlers/s3_to_stg_bulk.py#L163) falls back to
`staging.promotable_account_ids(engine)` when given no ids — i.e. reads back from staging rather
than from the validated set. Same post-hoc filtering as G3.

---

## Built, in spec, but wants a decision

- **`REQUIRE_NEWER_UPDATED_AT` defaults `True`** ([bulk_config.py:32](../core/bulk_config.py#L32)).
  The target's `updated_at` is a backend *write* clock; staging's comes from MarianaTek via Silver
  and is systematically older, so the gate may suppress nearly every update the widened column set
  was added to catch. `_count` reports the suppressed count separately
  ([update_stale.py:141](../s3_to_stg_bulk/update_stale.py#L141)) precisely so a dry run shows the gap
  — read it before the first real run.
- **`user_tags` / `customer_tags` are `stale: False`** — staged but never updated. Silver's
  `customer_tags.deleted_at` tombstone is staged and not propagated, so a tag removed in the CRM
  stays assigned.
- **`customers.state_id` / `last_class_date` / `next_class_date` / `tags` are `no_update`**
  ([config.py:166](../s3_to_stg_bulk/config.py#L166)) — backend owns state assignment; the class
  dates are recomputed by stage 2.
- **`order_lines.is_valid` is staged constant `TRUE`** ([config.py:346](../s3_to_stg_bulk/config.py#L346))
  — Silver has neither `is_valid` nor `child_orders`, so deferred-payment placeholder detection
  cannot run here. Invalidation arrives as `deleted_at` instead.
- **Nothing has been executed.** No DB or AWS access when this was written; the SQL was generated and
  statically checked only.

---

## Granularity of the bulk `update_stale` — recommendation

The concern is fair: collapsing to one statement per table means one bad row anywhere fails the
table for **every** account, and today's loop does not have that problem.

**Recommended: bulk statement per table, one transaction per table, with automatic per-account
fallback on failure.**

```python
for table in STALE_TABLES:                    # 11 tables, one transaction each
    try:
        n = bulk_update(engine, table, filtered_accounts)      # fast path: 1 statement
    except Exception:
        # Slow path, only on failure: isolate the offending account instead of
        # losing the table for everyone.
        n = 0
        for account_id in filtered_accounts:
            try:
                n += bulk_update(engine, table, [account_id])
            except Exception as e:
                failed.append({"table": table, "account_id": account_id, "error": str(e)})
```

Three granularity levels, each where it earns its keep:

| Level | Normal run | On failure |
|---|---|---|
| **Statement** | 11 total (one per table) | the failing table re-runs per account |
| **Failure isolation** | per table | per **account** — the bad one is named, the rest still land |
| **Reporting** | per account, always — a `GROUP BY account_id` rollup, not N separate passes | same |

Why this and not a straight bulk-or-bust: the fallback costs nothing on a healthy run (it never
fires), and when something *does* break you get a named account instead of a dead table. Why not the
current per-account loop: it pays the isolation cost on every run, 50N statements, when failures are
rare.

**Worth knowing: after G2 lands, per-row failures become unlikely anyway.** Staging columns are all
NULLable and typed identically to the target, so `COPY` already rejected anything malformed before
the update runs; NOT NULL columns are `COALESCE`-guarded; and the unique-violation risk lives
entirely in the ref repair that G2 removes. What is left — timeouts, deadlocks, connection loss — is
not account-specific, so the fallback would rarely fire. That is an argument for keeping it cheap
and automatic rather than for making isolation the default path.

---

## Deploying and invoking

### Deploy

```bash
export AWS_PROFILE=revi          # NB: the older scripts still default to a stale "raghava.revi"

# First deploy — creates revi-bulk-s3-to-stg, cloning runtime/layers/VPC from
# revi-crm-db-sync (python3.12, arm64, 10240 MB, 5 layers, 2 subnets + 1 SG).
./deployments/deploy_s3_to_stg_bulk.sh

# Re-deploys — code + config, environment merged not replaced
./deployments/deploy_s3_to_stg_bulk.sh
```

One manual step after the first create: the reference role does not yet carry
`athena:StartQueryExecution` / `GetQueryExecution` / `StopQueryExecution`, `glue:GetTable` /
`GetDatabase` on the Silver namespace, `s3:GetObject` on the Silver bucket, or
`s3:GetObject`+`PutObject` on the Athena output location. Without them the first `stage` invocation
fails with `AccessDenied`. See [deployments/README.md](../deployments/README.md).

### Invoke

`update` defaults to **false** — every command below is a dry run until you add `"update": true`.
Athena is still queried and staging is still written; only the stale UPDATE is withheld.

Resolution order: the event's `update` field wins; absent it, the `BULK_UPDATE` env var
(deployed as `false`); absent both, false. Membership is tested rather than a plain `.get`, so an
explicit `"update": false` still overrides `BULK_UPDATE=true` — the safe direction stays reachable
per invocation. Flipping the env var to `true` makes writing the default for scheduled runs, at the
cost of that being the behaviour of a bare `{"action":"stage"}`.

```bash
export AWS_PROFILE=revi
FN=revi-bulk-s3-to-stg
```

**1 — Pre-flight.** Read-only: checks all 13 target tables exist and Athena is configured.

```bash
aws lambda invoke --function-name $FN \
  --cli-binary-format raw-in-base64-out \
  --payload '{"action":"verify"}' /tmp/verify.json && jq . /tmp/verify.json
```

**2 — Dry run, one account, explicit window.** The number to read is
`stale.tables.<table>.suppressed_by_updated_at` against `.expected` — see the next section.

```bash
aws lambda invoke --function-name $FN \
  --cli-binary-format raw-in-base64-out \
  --payload '{
    "action": "stage",
    "account_ids": [1410],
    "start_datetime": "2026-07-30T00:00:00Z",
    "end_datetime":   "2026-07-31T00:00:00Z"
  }' /tmp/stage.json && jq . /tmp/stage.json
```

**3 — Same window, actually writing.**

```bash
aws lambda invoke --function-name $FN \
  --cli-binary-format raw-in-base64-out \
  --payload '{
    "action": "stage",
    "account_ids": [1410],
    "start_datetime": "2026-07-30T00:00:00Z",
    "end_datetime":   "2026-07-31T00:00:00Z",
    "update": true
  }' /tmp/stage.json && jq . /tmp/stage.json
```

**4 — All active accounts, last 10 minutes.** Omitting `account_ids` means every active account;
omitting the window means `BULK_DELTA_MINUTES` (10) back from now.

```bash
aws lambda invoke --function-name $FN \
  --cli-binary-format raw-in-base64-out \
  --payload '{"action":"stage","update":true}' /tmp/stage.json && jq . /tmp/stage.json
```

**5 — Stage only, defer the stale pass.** Use when `load_elapsed_seconds` approaches the 900 s
ceiling; run `update_stale` separately (or from inside the Step Function Map).

```bash
aws lambda invoke --function-name $FN --cli-binary-format raw-in-base64-out \
  --payload '{"action":"stage","account_ids":[1410,1411],"delta_minutes":90,"run_stale":false}' \
  /tmp/stage.json

aws lambda invoke --function-name $FN --cli-binary-format raw-in-base64-out \
  --payload '{"action":"update_stale","account_ids":[1410,1411],"update":true}' \
  /tmp/stale.json && jq . /tmp/stale.json
```

**6 — Clean up.** Drops the `stg_*_bulk` tables and releases the run slot. The Step Function does
this automatically; run it by hand after a manual `stage`.

```bash
aws lambda invoke --function-name $FN --cli-binary-format raw-in-base64-out \
  --payload '{"action":"cleanup"}' /tmp/cleanup.json && jq . /tmp/cleanup.json
```

**If a previous run died before `cleanup`,** the run slot is held. A slot older than
`STALE_RUN_HOURS` (6) is taken over automatically; before that, add `"force": true` to `stage`.

### Reading the response

```jsonc
{
  "accounts": {
    "requested":         [1410, 1411, 9999],
    "resolved":          [1410, 1411],          // survived the active-account filter
    "processed":         [1410],                // had rows in the window → the Map input
    "no_rows_in_window": [1411],
    "rejected":          [{"account_id": 9999, "reason": "no such account"}]
  },
  "staged":               {"customers": 812, "orders": 4410, "...": 0},
  "load_elapsed_seconds": 41.7,                 // watch against the 900 s ceiling
  "stale": {
    "success": true,
    "tables": {
      "reservations": {
        "expected":                 1204,       // what the UPDATE will touch
        "updated":                  1204,       // 0 on a dry run
        "suppressed_by_updated_at": 88,         // differ, but held back by the recency gate
        "per_account_expected":     {"1410": 1204},
        "failed_accounts":          []
      }
    }
  }
}
```

`status` is `"partial"` when staging succeeded but the stale pass failed for some accounts — the
rows are staged and promotion can still run.

---

## Before the first real run

Nothing here is a code gap — these are the things only a live run can answer.

1. **Dry-run first, and read the suppressed count.** `update` now defaults to `False`, so
   `{"action": "stage", "account_ids": [...], "start_datetime": …, "end_datetime": …}` counts and
   writes nothing. Compare `expected` against `suppressed_by_updated_at` per table: if `suppressed`
   dwarfs `expected`, the `REQUIRE_NEWER_UPDATED_AT` gate is hiding the very updates the widened
   column set was added to catch — the target's `updated_at` is a backend write clock, staging's
   comes from MarianaTek via Silver and is systematically older. Decide on the flag from that
   number, not in advance.
2. **Watch `load_elapsed_seconds` against the 15-minute Lambda ceiling.** 13 Athena queries plus a
   COPY each, then the stale pass. If it runs close, invoke with `"run_stale": false` and add an
   `update_stale` state inside the Map — the action already takes an account list.
3. **Confirm Athena's NULL handling** on a column that holds a genuine empty string. Athena writes
   NULL as an unquoted empty CSV field, so Postgres `COPY … CSV` reads both as NULL. Fine for every
   column staged here, but worth confirming once.
4. **Read the `information_schema` warnings on the first run.** `customer_notes.is_pinned` and
   `author_id` are known-absent and marked `no_update`; anything else the intersection reports is a
   spec bug.

Open risk #1 in [review.md](review.md) — duplicate-parent fan-out — no longer applies to this
lambda: the parent joins that could fan out were the `customer_ref_id` repairs, now deleted. It
still applies to `stg_to_main_bulk`.

---

## Still open

Nothing for lambda 1. What remains is second-lambda work:

- `stg_to_main_bulk` hardcodes the `stg_*_bulk` names as 176 string literals across 22 files rather
  than calling `cfg.staging_name()`. Not urgent now the rename is off the table, but it lets the two
  lambdas drift apart silently.
- `user_tags` / `customer_tags` are insert-only: Silver's `customer_tags.deleted_at` tombstone is
  staged but never propagated, so a tag removed in the CRM stays assigned. Fixing it means a delete
  pass over `customer_tag_assignments` — new behaviour, not a port.
- `is_valid` is staged constant `TRUE` (Silver has neither `is_valid` nor `child_orders`), so
  deferred-payment placeholder detection cannot run in the bulk path. Invalidation arrives as
  `deleted_at` through the stale update.
