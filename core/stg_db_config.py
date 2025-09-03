
import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    NUM_THREADS: int = 8
    DATABASE_URL: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
