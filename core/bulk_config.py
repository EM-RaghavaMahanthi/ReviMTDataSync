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
    ATHENA_CATALOG: str = "awsdatacatalog"
    ATHENA_WORKGROUP: str = "primary"
    ATHENA_OUTPUT_LOCATION: str = ""       # s3://.../ — required by the stage action
    ATHENA_TIMEOUT_SECONDS: float = 600.0  # a day-wide delta far exceeds any 60s default

    # ── Window ───────────────────────────────────────────────────────────────
    # Used when the event supplies neither start_time/end_time nor delta_minutes.
    BULK_DELTA_MINUTES: int = 60

    # ── Stale update ─────────────────────────────────────────────────────────
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

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()
