
import os
import json
from typing import Optional, Dict
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    API_BATCH_SIZE: int = 1000

    NUM_THREADS: int = 8
    DATABASE_URL: str
    API_KEY: str = ""  # Made optional with default empty string
    API_BASE_URL: str = "https://revelmethod.marianatek.com/api"  # Added field
    PARQUET_BATCH_SIZE: int = 1000
    S3_BUCKET: str = ""  # optional - only Lambdas that actually write to S3 need this set
    CONCURRENCY_LIMIT: int = 16
    PAGE_SIZE: int = 200  # MarianaTek confirmed page_size up to 500 on all endpoints

    # Ids per filter[id] call for the id_batch resources. Bounded by query-string length,
    # not by page size: 200 seven-digit ids is ~1.6KB of query string. Kept as its own knob
    # rather than reusing PAGE_SIZE — one is "how many records per page", the other is "how
    # many ids fit in a URL", and they are limited by different things.
    MAX_IDS: int = 200

    # Per-tenant request rate cap (50% of MarianaTek's 200 req/min ceiling — headroom
    # for the in-process token bucket in utils/api_client.py). Read there via os.environ.
    CRM_MAX_REQUESTS_PER_MIN: int = 100
    # Pages per Step Functions shard (crm_sync/state.py splits [1..total_pages] by this).
    PAGES_PER_SHARD: int = 200
    # 100-user batches per user_batch shard (credit_transactions, membership_transactions).
    # pages/shard ≈ USER_BATCHES_PER_SHARD × txns_per_user; time ≈ pages / CRM_MAX_REQUESTS_PER_MIN.
    # 20 batches × ~10 txns/user ≈ 200 pages ≈ ~2 min/shard. Lower for txn-heavy tenants.
    USER_BATCHES_PER_SHARD: int = 20

    CHECK_STALE_DATA: bool = False  # New setting to enable/disable stale data checking

    TEAMS_ENABLED: bool = False
    TEAMS_WEBHOOK_URL: str = ""
    TEAMS_MAX_RETRIES: int = 5
    TEAMS_TIMEOUT_SECONDS: int = 30

    AWS_PROFILE_NAME: str = "revi"  
    
    # Individual S3 prefix fields (used if s3_prefixes is not provided)
    CUSTOMERS_S3_PREFIX: str = "customers/"
    ORDERS_S3_PREFIX: str = "orders/"
    ORDER_LINES_S3_PREFIX: str = "order_lines/"
    CLASS_SESSIONS_S3_PREFIX: str = "class_sessions/"
    RESERVATIONS_S3_PREFIX: str = "reservations/"
    CREDIT_TRANSACTIONS_S3_PREFIX: str = "credit-transactions-details"
    MEMBERSHIP_INSTANCES_S3_PREFIX: str = "membership-instances-details"
    MEMBERSHIP_TRANSACTIONS_S3_PREFIX: str = "membership-transactions-details"
    # Tags & notes (new)
    USER_NOTES_S3_PREFIX: str = "mariana-tek/user_notes-details"
    USER_TAGS_S3_PREFIX: str = "mariana-tek/user_tags-details"
    CUSTOMER_TAGS_S3_PREFIX: str = "mariana-tek/customer_tags-details"

    # Optional JSON string field for all prefixes at once
    s3_prefixes: Optional[str] = None
    
    @property
    def S3_PREFIXES(self) -> Dict[str, str]:
        # If s3_prefixes JSON string is provided, parse and use it
        if self.s3_prefixes:
            try:
                return json.loads(self.s3_prefixes)
            except json.JSONDecodeError:
                # Fall back to individual fields if JSON parsing fails
                pass
        
        # Otherwise, use individual prefix fields
        return {
            "customers": self.CUSTOMERS_S3_PREFIX,
            "orders": self.ORDERS_S3_PREFIX,
            "order_lines": self.ORDER_LINES_S3_PREFIX,
            "class_sessions": self.CLASS_SESSIONS_S3_PREFIX,
            "reservations": self.RESERVATIONS_S3_PREFIX,
            "credit_transactions": self.CREDIT_TRANSACTIONS_S3_PREFIX,
            "membership_instances": self.MEMBERSHIP_INSTANCES_S3_PREFIX,
            "membership_transactions": self.MEMBERSHIP_TRANSACTIONS_S3_PREFIX,
            "user_notes": self.USER_NOTES_S3_PREFIX,
            "user_tags": self.USER_TAGS_S3_PREFIX,
            "customer_tags": self.CUSTOMER_TAGS_S3_PREFIX,
        }

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # Allow extra fields in .env without raising errors

settings = Settings()
