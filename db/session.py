# db/session.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from core.config import settings

# Create SQLAlchemy engine
engine = create_engine(
    settings.DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,   
)

# Session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    expire_on_commit=False
)

def get_db():
    """
    Dependency-style generator that yields a database session.
    Useful if you use frameworks like FastAPI or context managers.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
