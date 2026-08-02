"""
Settings for the bulk Silver → staging → main pipeline (s3_to_stg_bulk /
stg_to_main_bulk). Kept separate from core/config.py (Stage 1/2) and
core/stg_db_config.py (Stage 3) so the bulk Lambdas do not inherit their required
fields — neither bulk Lambda talks to the CRM API or Auth0.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str

    # ── Silver reads (Athena over the Iceberg tables) ─────────────────────────
    REGION: str = "us-east-1"
    SILVER_NAMESPACE: str = "silver"

    # The Silver tables are S3 Tables (managed Iceberg), NOT the default Glue catalog —
    # `silver` does not exist in AwsDataCatalog. The catalog name contains a '/', so every
    # SQL reference to it has to be double-quoted; athena.fqn does that. Matches
    # reviDataInsightsAPI/revi-cloud-campaign's ATHENA_CATALOG.
    ATHENA_CATALOG: str = "s3tablescatalog/revi-crm-data"

    # The data-lake workgroup, same as reviDataInsightsAPI/revi-dlk-gold. It sets
    # EnforceWorkGroupConfiguration=true with its own output location
    # (s3://revi-datalake-athena/athena-results/), so whatever we pass in
    # ResultConfiguration is overridden. That is fine — the enforced location is exactly
    # what the execution role grants, and athena.result_location reads the real location
    # back from Athena rather than assuming ours was used.
    ATHENA_WORKGROUP: str = "revi-dlk-gold"

    # Bucket name only, not a URI — matching revi-dlk-gold's ATHENA_OUTPUT_BUCKET. Required,
    # so a missing value fails at import rather than after the run slot has been claimed
    # and 13 staging tables created.
    ATHENA_OUTPUT_BUCKET: str

    # The execution role grants s3:GetObject/PutObject only under
    # revi-datalake-athena/{athena-results,temp,athena-ddl-results}/ — stay inside one.
    ATHENA_OUTPUT_PREFIX: str = "athena-results"

    ATHENA_TIMEOUT_SECONDS: float = 600.0  # a day-wide delta far exceeds any 60s default

    # ── Window ───────────────────────────────────────────────────────────────
    # Fallback window length. Precedence: event.delta_minutes > this > nothing.
    # Only consulted when the event supplies neither start_datetime/end_datetime nor its
    # own delta_minutes.
    BULK_DELTA_MINUTES: int = 10

    # ── Stale update ─────────────────────────────────────────────────────────
    # Whether the stale pass actually writes. False means a dry run: it counts what would
    # change, logs it, and writes nothing. Deliberately False so a misconfigured or
    # accidental invocation cannot rewrite production rows.
    # Precedence: the event's "update" field overrides this per invocation.
    BULK_UPDATE: bool = False

    # Also require staging.updated_at > target.updated_at before overwriting a column.
    # Be aware this suppresses most real updates: the target's updated_at is a backend
    # write clock (every insert sets now()), while staging's comes from MarianaTek via
    # Silver and is therefore systematically older. update_stale logs the suppressed
    # count beside the value-differs count so a dry run shows the gap.
    REQUIRE_NEWER_UPDATED_AT: bool = True

    # A run slot older than this is assumed dead and may be taken over.
    STALE_RUN_HOURS: int = 6

    # ── Promotion (stg_to_main_bulk) ─────────────────────────────────────────
    # Off by default for bulk: the downstream EZTexting sync is an onboarding concern and
    # would fire once per account per delta run.
    IS_POST_PROCESS: bool = False
    TOKEN_SERVICE_LAMBDA_NAME: str = "revi-backend-cronjobs-all"

    TEAMS_ENABLED: bool = False
    TEAMS_WEBHOOK_URL: str = ""
    TEAMS_MAX_RETRIES: int = 5
    TEAMS_TIMEOUT_SECONDS: int = 30

    ENVIRONMENT: str = "prod"

    @property
    def ATHENA_OUTPUT_LOCATION(self) -> str:
        """
        Where Athena is asked to write results. Composed rather than configured so the
        bucket and prefix cannot drift apart, and so the bucket stays a plain name that
        matches revi-dlk-gold's ATHENA_OUTPUT_BUCKET.

        Note the ATHENA_WORKGROUP above enforces its own location, so in practice this is
        what we request and the workgroup's value is what Athena uses.
        """
        return f"s3://{self.ATHENA_OUTPUT_BUCKET}/{self.ATHENA_OUTPUT_PREFIX.strip('/')}/"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()
