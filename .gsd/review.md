# REVIEW — open risks before this runs against production

Ordered by how much damage each could do. None is a blocker for review; the first three
deserve a decision before the first non-dry run.

## 1. Duplicate parent rows would fan out (guarded, not solved)

Dropping the location predicate — the approved design — removes what was also
disambiguating the parent joins. The onboarding pipeline scopes its "already exists" checks
per location, so it can create two `customers` rows for one `customer_id` under two
locations of one account; a location-free `INNER JOIN customers` onto that pair inserts the
child row twice.

`stg_to_main_bulk` refuses to promote an account whose parents hold duplicate
`(account_id, business key)` rows, naming the table and count. That converts silent
duplication into a loud stop, but it does not fix the underlying data.

**Decide:** run `{"action": "verify", "account_id": …}` across the account list first. If
duplicates are widespread, either dedupe them backend-side (the unique index the backend
team was adding to `customers` would prevent recurrence) or switch the copied joins to
`DISTINCT ON` parent lookups — the same shape `update_stale._repair_ref` already uses.

## 2. Stage A can approach the 15-minute Lambda limit

`stage` does 13 Athena queries plus a COPY each, then the stale pass for every account in
the window. On a wide window with many accounts that is not obviously under 15 minutes.

Mitigation is already in place, no code change needed: invoke with `"run_stale": false` and
add an `update_stale` state inside the Map (the action exists and takes `account_id`). Worth
measuring `load_elapsed_seconds` against the total on the first real run and deciding.

## 3. The `updated_at` gate may suppress nearly every update

`REQUIRE_NEWER_UPDATED_AT` defaults true to match onboarding behaviour, but the target's
`updated_at` is a backend *write* clock (every insert sets `now()`) while staging's comes
from MarianaTek via Silver, so it is systematically older. The widened column coverage could
therefore look like it changes nothing.

`update_stale` logs the suppressed count next to the differing count precisely so a dry run
shows the gap. Read that before deciding whether to set the flag false.

## 4. Known functional gaps, deliberate

- **`is_valid` / `child_orders`**: Silver has neither, so deferred-payment placeholder
  detection cannot run in the bulk path. `is_valid` is staged constant `TRUE`; invalidation
  arrives as `deleted_at` through the stale update. The onboarding pipeline remains the only
  place that computes it.
- **`user_tags` / `customer_tags` are insert-only**: their targets have no `updated_at`, and
  Silver's `customer_tags.deleted_at` tombstone is staged but not propagated — a tag removed
  in the CRM stays assigned. Fixing it means a delete pass over
  `customer_tag_assignments`, which is new behaviour, not a port.
- **`customers.state_id`, `last_class_date`, `next_class_date` are never written** by the
  stale update: the backend owns state assignment, and the class dates are recomputed from
  the promoted reservations by Stage B.

## 5. Things that are assumed and cheap to check on the first run

- Athena writes NULL as an unquoted empty CSV field, so Postgres `COPY … CSV` reads it as
  NULL. A genuine empty string also lands as NULL. Confirm on a column that holds a real
  `''` before trusting it in anger.
- `information_schema` intersection will log a warning for every spec column the target
  lacks. `customer_notes.is_pinned` / `author_id` are already known-absent and marked
  `no_update`; anything else it reports is a spec bug worth fixing.
- The strict count validations inside the copied processors raise on mismatch. Two of them
  subtract overlapping sets (`missing_dependencies` and `already_exist`), so a staging row
  that is both would double-subtract and raise. That hazard exists in the onboarding
  pipeline too, but delta windows are dominated by already-existing rows, so it is far more
  likely to fire here. Per-account Map isolation means it fails one branch, not the run.
