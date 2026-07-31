# Step Functions

## `bulk_silver_sync.asl.json`

State machine for the bulk delta pipeline (see the README's "Bulk Delta Pipeline" section).

```
StageSilverDelta ──▶ VacuumTargets ──▶ PromoteAccounts (Map) ──▶ Cleanup ──▶ Succeeded
       │                   │                   │
       │ error             │ error             │ error
       ▼                   ▼                   ▼
ReleaseRunSlot ◀───────────┴───────────────────┘  ──▶ PipelineFailed (redrive from here)
```

- **StageSilverDelta** — `s3_to_stg_bulk:stage`. Returns `account_ids`, which becomes the
  Map's input. An empty list means the window held nothing promotable: the Map runs zero
  iterations and the execution still succeeds.
- **VacuumTargets** — `stg_to_main_bulk:vacuum`, once, before the fan-out. A failure here is
  caught and ignored: a missed vacuum costs query plan quality, not correctness.
- **PromoteAccounts** — Map over the accounts, `MaxConcurrency: 4`, invoking
  `stg_to_main_bulk:promote`. Every branch writes to the same production tables through the
  same RDS instance, so raise the concurrency only with the DB's headroom in mind.
- **Cleanup** — `s3_to_stg_bulk:cleanup`. Drops the staging tables and releases the run slot.
- **ReleaseRunSlot** — the failure path. Releases the slot (otherwise the next run is blocked
  until `STALE_RUN_HOURS` elapses) but **leaves the staging tables in place** so the window
  can be inspected and re-promoted without re-querying Athena.

### Creating it

The definition carries two placeholders — substitute them, it is not deployable as-is:

```bash
sed -e "s|\${STAGE_LAMBDA_ARN}|arn:aws:lambda:us-east-1:<acct>:function:revi-bulk-s3-to-stg|" \
    -e "s|\${PROMOTE_LAMBDA_ARN}|arn:aws:lambda:us-east-1:<acct>:function:revi-bulk-stg-to-main|" \
    step_functions/bulk_silver_sync.asl.json > /tmp/bulk_silver_sync.json

aws stepfunctions create-state-machine \
  --name revi-bulk-silver-sync \
  --definition file:///tmp/bulk_silver_sync.json \
  --role-arn arn:aws:iam::<acct>:role/<states-execution-role>
```

The state machine's role needs `lambda:InvokeFunction` on both functions.

### Running it

```bash
# default window (last BULK_DELTA_MINUTES)
aws stepfunctions start-execution --state-machine-arn <arn> --input '{}'

# explicit window
aws stepfunctions start-execution --state-machine-arn <arn> \
  --input '{"start_time":"2026-07-30T00:00:00Z","end_time":"2026-07-31T00:00:00Z"}'

# one account, taking over a stale run slot
aws stepfunctions start-execution --state-machine-arn <arn> \
  --input '{"delta_minutes":180,"account_ids":[1410],"force":true}'
```

The execution input is merged into the `stage` payload wholesale (via `States.JsonMerge`)
rather than field by field, because a JsonPath to an absent field would fail the state and
every window field is optional. Anything `handlers/s3_to_stg_bulk.py` accepts on the `stage`
action can therefore be passed straight through as execution input.

### Scheduling

An EventBridge rule invoking `StartExecution` with `{}` on a fixed cadence is the intended
production trigger. Keep the rate at or below `BULK_DELTA_MINUTES` so consecutive windows
abut rather than leaving gaps, and remember the run slot serialises overlapping executions —
a run that starts while another holds the slot fails fast rather than interleaving.
