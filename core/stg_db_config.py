
import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    NUM_THREADS: int = 8
    DATABASE_URL: str

    AUTH0_CLIENT_ID: str
    AUTH0_CLIENT_SECRET: str
    AUTH0_AUDIENCE: str
    AUTH0_TOKEN_URL: str
    
    # API Settings (Required)
    ENV_API_BASE_URL: str
    CONNECTION_TIMEOUT: int = 300  # 5 minutes
    READ_TIMEOUT: int = 120  # 2 minutes
    
    # Development Settings (Optional - only for dev environment)
    ENVIRONMENT: str = "prod"  
    DEV_AUTH_TOKEN: str = "" 

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
