
import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    API_BATCH_SIZE: int = 1000

    NUM_THREADS: int = 8  
    DATABASE_URL: str
    PROD_DATABASE_URL: str
    API_KEY: str
    PARQUET_BATCH_SIZE: int = 1000
    S3_BUCKET: str
    CONCURRENCY_LIMIT: int = 20
    PAGE_SIZE: int = 500

    AWS_PROFILE_NAME: str = "raghava.revi"  
    CUSTOMERS_S3_PREFIX: str = "customers/"
    ORDERS_S3_PREFIX: str = "orders/"
    ORDER_LINES_S3_PREFIX: str = "order_lines/"
    CLASS_SESSIONS_S3_PREFIX: str = "class_sessions/"
    RESERVATIONS_S3_PREFIX: str = "reservations/"
    CREDIT_TRANSACTIONS_S3_PREFIX: str = "credit-transactions-details"
    MEMBERSHIP_INSTANCES_S3_PREFIX: str = "membership-instances-details"
    MEMBERSHIP_TRANSACTIONS_S3_PREFIX: str = "membership-transactions-details"
    @property
    def S3_PREFIXES(self):
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

settings = Settings()
