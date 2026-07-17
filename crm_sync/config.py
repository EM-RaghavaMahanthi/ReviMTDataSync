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
                   "user_batch" → one shard per resource; customer_ids read from the
                                  customers parquet, sent 100/call via repeated &user=
                   "tenant"     → tiny tenant-wide lookup; one shard, paginate all, keep all
                                  (no client-side filter)
  probe_param  — query param the planner uses to scope the page-1 probe to this tenant;
                 None = no scoping param (paginate the whole endpoint)
  batch_size   — user IDs per API call (user_batch resources only)
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

    # Download the whole tenant unfiltered, then filter by customer_id. credit_transactions
    # and membership_transactions used to be "user_batch" (100 ids/call via repeated &user=),
    # but that started returning no records at all once a batch's query string got large
    # enough — switched to the same unfiltered+filter approach as membership_instances.
    "membership_instances":    {"endpoint": "/membership_instances",    "fetch_type": "user", "probe_param": "location"},
    "credit_transactions":     {"endpoint": "/credit_transactions",     "fetch_type": "user", "probe_param": "location"},
    "membership_transactions": {"endpoint": "/membership_transactions", "fetch_type": "user", "probe_param": "location"},

    # Notes: whole-tenant download (NO location filter), filtered by customer_id client-side.
    "user_notes": {"endpoint": "/user_notes", "fetch_type": "user", "probe_param": None},
    # Tags: tiny tenant-wide lookup table (id → name/tag_type); keep all rows.
    "user_tags":  {"endpoint": "/user_tags",  "fetch_type": "tenant", "probe_param": None},
}
