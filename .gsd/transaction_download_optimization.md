# Downloading transactions: from tenant-wide to by-id

How `credit_transactions` and `membership_transactions` are fetched in Stage 1, what it
replaced, and why the same change is still owed to `revi-dlk-bronze`.

---

## ReviMTDataSync — what changed

### Earlier

Both resources were `fetch_type: "user"`: paginate the **entire tenant** with no useful
filter, then discard client-side everything whose `customer_id` did not belong to the
location being onboarded.

The cost scaled with the tenant, not with the location. A 50-location tenant meant
downloading roughly 50x what was kept — for every single location onboarded. That was the
3-4 hours per location.

Before that it was `fetch_type: "user_batch"`: 100 customer ids per call as repeated
`&user=`. Abandoned because the endpoint returned no records at all once the query string
got long enough. The mode is still documented in `crm_sync/config.py` because it explains
why `id_batch` is shaped the way it is.

### Now

`fetch_type: "id_batch"` — fetch exactly the transactions this location references, by id,
`MAX_IDS` (200) per request via one comma-joined `filter[id]`.

```
credit_transactions:      id_sources = [(order_lines, credit_transactions_id),
                                        (reservations, credit_transactions_id)]
membership_transactions:  id_sources = [(order_lines, membership_transactions_id),
                                        (reservations, membership_transactions_id)]
```

### How it got optimised

The ids were already sitting in data we had just downloaded. `order_lines` and
`reservations` are location-scoped, fetched in the location phase, and both carry
`credit_transactions_id` and `membership_transactions_id`. Collect the distinct non-null
values across both and that **is** the exact set this location needs — no extra API calls
to discover it.

The work went from

```
pages(whole tenant)   ->   ceil(distinct_ids_for_this_location / 200)
```

For a location referencing 20k transactions that is 100 requests, whether the tenant has 1
location or 50. Before, the tenant's total transaction count set the page count and the
location's size was irrelevant.

### Two things that had to change

**A second planning pass.** Those ids do not exist until the location phase has written its
parquet, so these shards cannot be planned at `Stage1_Plan`. The state machine gained
`Stage1_PlanTransactions` between `Stage1_LocationMap` and the new `Stage1_TransactionMap`.

**`filter[id]`, not a user filter.** Multi-value `&user=` filtering does not work on these
endpoints. One comma-joined `filter[id]` is ~1.6KB for 200 ids and is the shape the API
actually supports.

Id lists go to S3 under `_idlists` rather than inline in the Step Functions payload:
measured, a shard descriptor is ~180 bytes but 200k ids inline is 1.5MB against a 256KB
limit. Shard lists stay inline; id lists are read from S3 by the shard.

### Trade-off

Coverage is a **subset** — only transactions referenced by this location's `order_lines` or
`reservations`. Third-party rows carry a NULL transaction id and drop out naturally, and
anything unreferenced arrives via webhooks, which download any dependency whose record does
not exist.

`crm_sync/_base/id_sync.py` fails the shard if a call returns more records than ids
requested. A silently ignored `filter[id]` would look plausible while quietly putting us
back to downloading the whole tenant.

`membership_instances` deliberately stayed on the whole-tenant path — small enough that page
count stays low relative to user count, so the id round-trip would not pay for itself.

---

## revi-dlk-bronze — still on the old shape

Bronze has **not** had this change. Worth being precise about what it does today, because it
is not the same inefficiency ReviMTDataSync had.

`src/crm_sync/config.py` still has both resources as `fetch_type: "user"` with
`params_fn` building `{"user": entity_id, "page": page, "page_size": page_size}` — that is
**one request per user**, all pages per user, batched `USER_BATCH_SIZE` (500) users at a
time (`src/crm_sync/user_sync.py::run_batch` -> `_fetch_all_pages_for_user`).

There is also a `user_sync_mode: "unfiltered"` path
(`src/crm_sync/user_sync_unfiltered.py`) which is the whole-tenant download ReviMTDataSync
used to do. Default is `per_user`.

So bronze's cost is per-user fan-out rather than tenant-wide over-fetch:

| | requests for a location with 20k customers referencing 20k transactions |
|---|---|
| bronze `per_user` (today) | >= 20,000 — at least one page per user |
| bronze `unfiltered` | pages(whole tenant) |
| ReviMTDataSync `id_batch` | **100** — `20000 / 200` |

Same fix, different starting point: both collapse to one request per 200 referenced ids.

### What implementing it in bronze involves

1. **`src/crm_sync/config.py`** — add `fetch_type: "id_batch"` for the two transaction
   resources with `id_sources` pointing at `order_lines` / `reservations`. Note bronze's
   config carries a `params_fn` per resource, which `id_batch` does not use — the id-batch
   fetch builds its own `filter[id]` param.
2. **new `src/crm_sync/id_sync.py`** — port `crm_sync/_base/id_sync.py`, including the
   ignored-filter guard. Bronze writes **raw** Bronze rows rather than mapped ones, so the
   fetch function returns unmapped records; only the orchestration carries over.
3. **`src/crm_sync/handler.py`** — `USER_RESOURCES` currently drives the mode dispatch at
   lines 57-58 and 132-177. Transactions move out of it into an id-batch branch that runs
   after the location resources, since it depends on their parquet.
4. **Ordering** — bronze runs location resources then user resources inside one
   `_run_sync`, so the dependency is already satisfied in-process. It does not need
   ReviMTDataSync's separate `PlanTransactions` state; the ids can be read straight from the
   `order_lines` / `reservations` rows just written.

Point 4 is the one real simplification: bronze does not need the two-phase planning that
Step Functions forced on ReviMTDataSync.

### Caveats to carry over

- `filter[id]` returns an explicit `"meta": null` on non-paginating responses. `.get("meta",
  {})` returns `None` there, because the default only applies when the key is **absent**.
  Same for `"data"`.
- Ids must be normalised to integer strings before joining. `Optional[int]` plus NULLs makes
  pandas infer float64, parquet writes a double, and `filter[id]=1847.0` matches nothing.
- Keep the ignored-filter guard. Without it, a silently dropped filter degrades to a
  full-tenant download that still looks like success.

### Still unverified

`filter[id]` has not been confirmed against `/membership_transactions` — only
`/credit_transactions`. Worth one curl before porting.
