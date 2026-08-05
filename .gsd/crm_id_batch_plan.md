# Onboarding Stage 1 — fetch transactions by id, not by tenant

## The problem

`credit_transactions` and `membership_transactions` are `fetch_type: "user"`: download the
**whole tenant's** endpoint page by page, then discard client-side every row whose
`customer_id` is not in this location. Onboarding one location of a 50-location tenant
downloads roughly 50× what it keeps, and takes 3–4 hours — long enough that it should have
covered 30–40 locations instead of one.

An earlier attempt fixed this with `fetch_type: "user_batch"` — 100 customer ids per call via
repeated `&user=`. [crm_sync/config.py](../crm_sync/config.py) records why it was abandoned:

> *"started returning no records at all once a batch's query string got large enough"*

## Why this time is different

Don't filter by user at all. Filter by the transaction ids, which we already know.

`order_lines` and `reservations` are `location` resources — already scoped to the location,
already downloaded — and both carry `credit_transactions_id` and `membership_transactions_id`.
So the id set is a by-product of work already done.

And the request shape is one comma-joined parameter rather than 100 repeated ones, which is
what broke last time:

```
GET /api/credit_transactions?filter[id]=1847,1845
GET /api/users?home_location=48717&page_size=100&filter[id]=35020,36471
```

100 ids of ~7 digits is roughly an 800-character query string — nowhere near the limit that
killed `&user=`.

**Coverage becomes a strict subset**: only transactions referenced by this location's
order_lines or reservations. Confirmed acceptable — webhooks download any dependency whose
record does not exist, so an unreferenced transaction is not lost, it simply arrives by the
other path. Rows where the transaction type is not set and the id is NULL (third-party
orders and reservations) drop out of the id set naturally.

---

## Two-phase planning

The state machine already runs `Stage1_LocationMap` to completion **before**
`Stage1_UserMap`, so the dependency this introduces costs no restructuring — the new states
slot in between.

```
Stage1_Plan            location_shards + the user shards that need no ids
Stage1_LocationMap     customers, orders, order_lines, class_sessions, reservations
   ↓ order_lines + reservations parquet now exist in S3
Stage1_PlanTransactions   NEW — collect distinct ids, write id lists to S3, emit shards
Stage1_TransactionMap     NEW — id_batch shards: filter[id]=… , 100 ids per request
Stage1_UserMap         membership_instances, user_notes, user_tags — unchanged
   ↓
Stage2_S3ToStagingDb
```

`Stage1_UserMap` stays where it is and keeps `MaxConcurrency: 1`; the new Map matches it, so
total request concurrency against the CRM is unchanged.

---

## Changes

### 1. `crm_sync/config.py` — a new fetch type

```python
"credit_transactions": {
    "endpoint": "/credit_transactions",
    "fetch_type": "id_batch",
    "id_sources": [("order_lines", "credit_transactions_id"),
                   ("reservations", "credit_transactions_id")],
    "batch_size": 100,
},
"membership_transactions": {
    "endpoint": "/membership_transactions",
    "fetch_type": "id_batch",
    "id_sources": [("order_lines", "membership_transactions_id"),
                   ("reservations", "membership_transactions_id")],
    "batch_size": 100,
},
```

`membership_instances` stays `fetch_type: "user"` — it is small and has no id source.

### 2. `crm_sync/state.py` — `plan_transaction_shards()`

- Read the `order_lines` and `reservations` parquet for this account/location.
- For each resource, union its `id_sources`, drop NULLs, dedupe, sort.
- Write the id list to S3 once per resource, so a shard reads a slice instead of
  re-reading both parquets. Also keeps the plan payload small — 20 000 ids inline would be
  ~160 KB against Step Functions' 256 KB limit.
- Emit `ceil(len(ids) / ids_per_shard)` shards carrying `{resource, id_offset, id_limit}`.

**Sizing:** `batch_size = 100` ids per request, and a shard is capped at the same 200
requests per Lambda the existing shards use (`_PAGES_PER_SHARD` / `_USER_BATCHES_PER_SHARD`,
both 200). So **20 000 ids per shard** — one new `IDS_PER_SHARD = 200 * 100` derived from the
existing constants rather than a fourth independent knob.

### 3. `handlers/crm_to_s3.py` — two modes

- `plan_transactions` → calls the planner above, returns `transaction_shards`.
- `id_batch_shard` → reads the id slice from S3, chunks it into 100s, and for each chunk
  issues `filter[id]=<comma-joined>`, maps, and writes parquet under a shard-unique tag
  (`f"{location_id}_ib_{id_offset}"`, mirroring the existing `_ub_` tag) so slices of one
  resource cannot collide on S3 keys.

Both mirror `_handle_user_batch_shard`, which already does read-slice → batch → write. That
machinery is the closest thing to what is needed and should be reworked rather than written
fresh.

### 3b. What crosses the state machine, and what goes to S3

Step Functions caps a state payload at **256 KB**, and an execution that trips it fails
mid-run with nothing useful in the output. Measured, so the split is a decision rather than
a guess:

| | size | where |
|---|---|---|
| one shard entry | 180 bytes | — |
| 50 shards | 8 KB | **inline** |
| 500 shards | 87 KB | **inline**, still comfortable |
| 20 000 ids | **156 KB** | **S3** |
| 200 000 ids | **1.5 MB** | **S3** — would fail outright |

So: **id lists always go to S3; shard lists stay inline.** Shards are metadata — a resource
name and an offset — and stay small however big the tenant is. Id lists scale with the
data and are the only thing that can burst the limit.

Concretely, `Stage1_PlanTransactions` writes one object per resource:

```
s3://<bucket>/<account_id>/_idlists/credit_transactions.json
s3://<bucket>/<account_id>/_idlists/membership_transactions.json
```

and returns only `[{resource, id_offset, id_limit}, …]`. Each shard reads its own slice, so
no id ever travels through a state transition, and the plan output stays a few KB no matter
how large the location is. It also makes a failed shard re-runnable on its own — the id list
is still sitting in S3 rather than having to be recomputed from the parquets.

If the shard list itself ever did grow past the limit, the fix is a Distributed Map with
`ItemReader` pointed at an S3 object rather than `ItemsPath`. At 180 bytes a shard that is
thousands of shards away, so it is not worth the change in execution semantics now.

### 4. `utils/s3_writer.py` — id list helpers

`read_customer_ids_from_s3` is the existing pattern. Add the generic pair:
`write_id_list_to_s3(account_id, resource, ids)` / `read_id_list_from_s3(account_id,
resource, offset, limit)`.

### 5. `crm_sync/credit_transactions.py`, `membership_transactions.py`

They already expose `process_resource_batched(ids, …)` from the `user_batch` era. Repoint it
at `filter[id]` instead of repeated `&user=`; the mapping and parquet-writing below it are
unchanged.

### 6. `crm_state_machine.json`

Insert `Stage1_PlanTransactions` (Task) and `Stage1_TransactionMap` (Map,
`ItemsPath: $.transactions_plan.transaction_shards`, `MaxConcurrency: 1`) between
`Stage1_LocationMap` and `Stage1_UserMap`, with the same Retry/Catch shape as the
neighbouring states.

---

## Expected effect

For a 50-location tenant onboarding one location, `credit_transactions` and
`membership_transactions` go from "every page the tenant has" to "one request per 100
referenced ids". The fetch stops scaling with tenant size and starts scaling with the
location's own transaction count — which is the thing that should have driven it all along.

`membership_instances` is untouched, so any remaining tenant-wide cost sits there and is
small by inspection.

---

## Worth checking on the first run

1. **Does `filter[id]` cap the number of ids?** 100 is asserted from the Postman examples,
   not from documentation. If the endpoint truncates silently rather than erroring, a batch
   would come back short — compare `len(ids)` against records returned per chunk on the
   first run and fail loudly if they disagree.
2. **`page_size` interaction.** 100 unique ids return at most 100 records, so one page —
   provided `page_size >= batch_size`. `PAGE_SIZE` defaults to 500, so this holds; worth an
   assertion rather than an assumption.
3. **Ids present in staging but absent from the CRM.** A deleted transaction still
   referenced by an order_line would come back missing. Expected, and the same residue stage
   2 already reports — but the first run should confirm the count is small.
