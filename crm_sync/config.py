"""
Per-resource CRM sync metadata — drives the shard planner (crm_sync/state.py) and
the Stage-1 mode dispatch (handlers/crm_to_s3.py).

Unlike revi-dlk-bronze, ReviSync keeps its per-resource field mapping in each
crm_sync/<resource>.py module (the parquet written here feeds Stage 2/3, which
expect the mapped schema). So this config carries only orchestration metadata:

  endpoint     — CRM path, used by the planner to probe page 1
  fetch_type   — how the resource is sharded / fetched:
                   "location"   → paginated by location; fixed page-range shards
                   "user"       → downloaded whole per tenant then filtered client-side by
                                  customer_id; fixed page-range shards. probe_param="location"
                                  scopes the fetch to a location; probe_param=None paginates
                                  the entire endpoint (no filter at all).
                   "id_batch"   → fetch only the ids this location references, via
                                  filter[id] (one comma-joined param), `batch_size` per
                                  call. Ids come from `id_sources` — resources already
                                  downloaded by the location phase — so these shards can
                                  only be planned AFTER Stage1_LocationMap completes.
                   "user_batch" → one shard per resource; customer_ids read from the
                                  customers parquet, sent 100/call via repeated &user=.
                                  No resource uses this any more: the repeated-param form
                                  broke at length. Kept because the planner still knows the
                                  mode and it documents why id_batch is shaped as it is.
                   "tenant"     → tiny tenant-wide lookup; one shard, paginate all, keep all
                                  (no client-side filter)
  probe_param  — query param the planner uses to scope the page-1 probe to this tenant;
                 None = no scoping param (paginate the whole endpoint)
  id_sources   — [(resource, column), …] whose parquet supplies the ids (id_batch only).
                 The union across sources is deduped and NULLs dropped.
  batch_size   — ids per API call (id_batch / user_batch resources only)
"""

LOCATION_RESOURCES = ["customers", "orders", "order_lines", "class_sessions", "reservations"]
USER_RESOURCES     = ["credit_transactions", "membership_instances", "membership_transactions"]
# New (tags & notes). Kept in separate lists so the existing pipeline is untouched until the
# planner/handler wiring is done. user_notes reuses the "user_shard" path (unfiltered + client
# filter); user_tags is a "tenant" lookup; customer tag assignments are derived from the
# customers fetch (no dedicated endpoint).
NOTES_RESOURCES    = ["user_notes"]
TENANT_RESOURCES   = ["user_tags"]
ALL_RESOURCES      = LOCATION_RESOURCES + USER_RESOURCES + NOTES_RESOURCES + TENANT_RESOURCES

RESOURCE_CONFIG: dict = {
    "customers":      {"endpoint": "/users",          "fetch_type": "location", "probe_param": "home_location"},
    "orders":         {"endpoint": "/orders",         "fetch_type": "location", "probe_param": "location"},
    "order_lines":    {"endpoint": "/order_lines",    "fetch_type": "location", "probe_param": "location"},
    "class_sessions": {"endpoint": "/class_sessions", "fetch_type": "location", "probe_param": "location"},
    "reservations":   {"endpoint": "/reservations",   "fetch_type": "location", "probe_param": "location"},

    # Download the whole tenant unfiltered, then filter by customer_id. Fine here: this
    # table is small enough that the page count stays low relative to the user count.
    "membership_instances":    {"endpoint": "/membership_instances",    "fetch_type": "user", "probe_param": "location"},

    # Fetch exactly the transactions this location references, by id.
    #
    # These were "user" (download the whole tenant, filter client-side by customer_id),
    # which meant onboarding one location of a 50-location tenant pulled roughly 50x what
    # it kept — 3-4 hours for a single location. Before that they were "user_batch": 100
    # customer ids per call as repeated &user=, abandoned because the endpoint returned no
    # records at all once the query string got long enough.
    #
    # filter[id] is one comma-joined parameter rather than 100 repeated ones (~800 chars
    # for 100 ids), which is the shape MarianaTek supports. And the ids are free: the
    # id_sources below are location-scoped resources already downloaded by the time these
    # run, so the id set is a by-product rather than extra work.
    #
    # Coverage is deliberately a subset — only transactions referenced by this location's
    # order_lines or reservations. Webhooks download any dependency whose record does not
    # exist, so an unreferenced transaction arrives by that path instead. Third-party rows
    # carry a NULL transaction id and drop out of the id set naturally.
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

    # Notes: whole-tenant download (NO location filter), filtered by customer_id client-side.
    "user_notes": {"endpoint": "/user_notes", "fetch_type": "user", "probe_param": None},
    # Tags: tiny tenant-wide lookup table (id → name/tag_type); keep all rows.
    "user_tags":  {"endpoint": "/user_tags",  "fetch_type": "tenant", "probe_param": None},
}
