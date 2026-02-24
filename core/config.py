
import os
import json
from typing import Optional, Dict
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    API_BATCH_SIZE: int = 1000

    NUM_THREADS: int = 8  
    DATABASE_URL: str
    PROD_DATABASE_URL: str
    API_KEY: str = ""  # Made optional with default empty string
    API_BASE_URL: str = "https://revelmethod.marianatek.com/api"  # Added field
    PARQUET_BATCH_SIZE: int = 1000
    S3_BUCKET: str
    CONCURRENCY_LIMIT: int = 20
    PAGE_SIZE: int = 500

    CHECK_STALE_DATA: bool = False  # New setting to enable/disable stale data checking

    TEAMS_ENABLED: bool = False
    TEAMS_WEBHOOK_URL: str = ""
    TEAMS_MAX_RETRIES: int = 5
    TEAMS_TIMEOUT_SECONDS: int = 30

    AWS_PROFILE_NAME: str = "raghava.revi"  
    
    # Individual S3 prefix fields (used if s3_prefixes is not provided)
    CUSTOMERS_S3_PREFIX: str = "customers/"
    ORDERS_S3_PREFIX: str = "orders/"
    ORDER_LINES_S3_PREFIX: str = "order_lines/"
    CLASS_SESSIONS_S3_PREFIX: str = "class_sessions/"
    RESERVATIONS_S3_PREFIX: str = "reservations/"
    CREDIT_TRANSACTIONS_S3_PREFIX: str = "credit-transactions-details"
    MEMBERSHIP_INSTANCES_S3_PREFIX: str = "membership-instances-details"
    MEMBERSHIP_TRANSACTIONS_S3_PREFIX: str = "membership-transactions-details"
    
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
        }

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # Allow extra fields in .env without raising errors

settings = Settings()
