import logging
from typing import List, Optional, Dict, Any
from sqlalchemy import Column, Integer, String, create_engine, text, Engine, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager

from core.logger import get_logger
from core.config import settings

logger = get_logger(__name__)

# SQLAlchemy setup
Base = declarative_base()

class Account(Base):
    """Account model matching your database schema"""
    __tablename__ = 'accounts'
    
    id = Column(Integer, primary_key=True, index=True)
    status = Column(String)
    crm_config = Column(String)
    integration_id = Column(String)
    crm_api_end_point = Column(String)
    ez_texting_id = Column(Integer)

class States(Base):
    """States model matching your database schema"""
    __tablename__ = 'states'
    
    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(Integer)
    status = Column(String)
    deleted_at = Column(DateTime)
    priority = Column(String)
    state_name = Column(String) 

class DatabaseManager:
    """
    Class-based database manager for proper connection lifecycle control
    Each instance manages its own connection pool with explicit cleanup
    """
    
    def __init__(self, pool_size: int = None, max_overflow: int = 4):
        """
        Initialize database manager with custom pool settings
        
        Args:
            pool_size: Number of concurrent connections (defaults to MAX_CONCURRENT_ACCOUNTS)
            max_overflow: Additional connections allowed during bursts
        """
        self.pool_size = pool_size or getattr(settings, 'MAX_CONCURRENT_REQUESTS', 8)
        self.max_overflow = max_overflow
        self.engine = None
        self.SessionLocal = None
        self.is_initialized = False
        
        logger.info(f"DatabaseManager initialized - Pool size: {self.pool_size}, Max overflow: {self.max_overflow}")
    
    def initialize(self) -> Engine:
        """
        Initialize database engine and session factory
        Returns the engine for immediate use
        """
        if not self.is_initialized:
            try:
                logger.info(f"Initializing database engine with pool_size={self.pool_size}")
                
                # FIXED: Create engine with corrected configuration
                self.engine = create_engine(
                    settings.DATABASE_URL,
                    
                    # Aurora-optimized pool settings
                    poolclass=QueuePool,
                    pool_size=self.pool_size,
                    max_overflow=self.max_overflow,
                    pool_timeout=60,
                    pool_recycle=900,  # 15 minutes
                    pool_pre_ping=True,
                    
                    # FIXED: Aurora-specific connection settings (removed conflicting isolation)
                    echo=False,
                    connect_args={
                        "connect_timeout": 30,
                        "application_name": f"analytics_lambda_{getattr(settings, 'ENVIRONMENT', 'dev')}_{id(self)}",
                        "options": "-c statement_timeout=600000"  # 10-minute query timeout
                    }
                    
                    # REMOVED: Conflicting isolation level settings that caused the error
                    # isolation_level="READ_COMMITTED",
                    # execution_options={
                    #     "isolation_level": "AUTOCOMMIT"
                    # }
                )
                
                # Create session factory
                self.SessionLocal = sessionmaker(
                    autocommit=False,
                    autoflush=False,
                    bind=self.engine,
                    expire_on_commit=False
                )
                
                self.is_initialized = True
                logger.info(f"Database engine initialized successfully - Instance ID: {id(self)}")
                
            except Exception as e:
                logger.error(f"Failed to initialize database engine: {str(e)}")
                raise Exception(f"Database initialization failed: {str(e)}")
        
        return self.engine
    
    def get_engine(self) -> Engine:
        """
        Get the database engine, initializing if necessary
        """
        if not self.is_initialized:
            self.initialize()
        return self.engine
    
    @contextmanager
    def get_session(self):
        """
        FIXED: Context manager for database sessions with proper error handling
        """
        if not self.is_initialized:
            self.initialize()
        
        session = self.SessionLocal()
        try:
            yield session
            # Commit any pending changes
            session.commit()
        except Exception as e:
            # Rollback on error
            session.rollback()
            logger.error(f"Session error in DatabaseManager {id(self)}: {str(e)}")
            raise
        finally:
            # FIXED: Safer session cleanup
            try:
                session.close()
            except Exception as close_error:
                logger.warning(f"Error closing session in DatabaseManager {id(self)}: {str(close_error)}")
    
    def health_check(self) -> bool:
        """Instance-specific database health check with better error handling"""
        try:
            if not self.is_initialized:
                self.initialize()
            
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
            
            is_healthy = result == 1
            logger.debug(f"Database health check: {'PASS' if is_healthy else 'FAIL'} - Instance: {id(self)}")
            
            return is_healthy
            
        except Exception as e:
            logger.warning(f"Database health check failed for instance {id(self)}: {str(e)}")
            return False
    
    def get_pool_status(self) -> Dict[str, Any]:
        """FIXED: Get connection pool status with safer attribute access"""
        try:
            if not self.is_initialized:
                return {'status': 'not_initialized', 'instance_id': id(self)}
            
            pool = self.engine.pool
            
            # FIXED: Safe attribute access - some methods might not exist in all SQLAlchemy versions
            status = {
                'instance_id': id(self),
                'is_initialized': self.is_initialized
            }
            
            # Safe attribute checks
            try:
                status['pool_size'] = pool.size()
            except (AttributeError, Exception):
                status['pool_size'] = 'unknown'
            
            try:
                status['checked_in'] = pool.checkedin()
            except (AttributeError, Exception):
                status['checked_in'] = 'unknown'
            
            try:
                status['checked_out'] = pool.checkedout()
            except (AttributeError, Exception):
                status['checked_out'] = 'unknown'
            
            try:
                status['overflow'] = pool.overflow()
            except (AttributeError, Exception):
                status['overflow'] = 'unknown'
            
            # REMOVED: This attribute doesn't exist in QueuePool
            # try:
            #     status['invalidated'] = pool.invalidated()
            # except (AttributeError, Exception):
            #     status['invalidated'] = 'unknown'
            
            # Calculate utilization if we have the data
            if isinstance(status['pool_size'], int) and isinstance(status['checked_out'], int):
                max_connections = status['pool_size'] + (status['overflow'] if isinstance(status['overflow'], int) else 0)
                if max_connections > 0:
                    utilization = (status['checked_out'] / max_connections) * 100
                    status['utilization_percent'] = round(utilization, 1)
                    
                    if utilization > 80:
                        logger.warning(f"High pool utilization: {utilization:.1f}% - Instance {id(self)}")
            
            logger.debug(f"Pool status for instance {id(self)}: {status}")
            return status
            
        except Exception as e:
            logger.error(f"Error getting pool status for instance {id(self)}: {str(e)}")
            return {'error': str(e), 'instance_id': id(self)}
    
    def get_active_accounts(self) -> List[str]:
        """FIXED: Get all active accounts with better error handling"""
        try:
            with self.get_session() as session:
                logger.debug(f"Querying active accounts - Instance: {id(self)}")
                
                results = session.query(Account.id).filter(
                    Account.status == 'ACTIVE',
                    Account.crm_config.isnot(None),
                    Account.crm_api_end_point.isnot(None)
                ).all()
                
                accounts = [str(row.id) for row in results if row.id]
                
                logger.info(f"Found {len(accounts)} active accounts - Instance: {id(self)}")
                
                if len(accounts) == 0:
                    logger.warning("No active accounts found - pipeline will have no work to do")
                
                return accounts
                
        except SQLAlchemyError as e:
            logger.error(f"Database error while fetching active accounts - Instance {id(self)}: {str(e)}")
            raise Exception(f"Failed to fetch active accounts: {str(e)}")
        
        except Exception as e:
            logger.error(f"Unexpected error while fetching active accounts - Instance {id(self)}: {str(e)}")
            # FIXED: Don't re-raise the same empty exception
            raise Exception(f"Failed to fetch active accounts: {str(e)}")

    def get_active_accounts_with_eztexting(self) -> List[str]:
        """Get all active accounts that have ez_texting_id configured"""
        try:
            with self.get_session() as session:
                logger.debug(f"Querying active accounts with EZTexting - Instance: {id(self)}")
                
                results = session.query(Account.id).filter(
                    Account.status == 'ACTIVE',
                    Account.ez_texting_id.isnot(None)
                ).all()
                
                accounts = [str(row.id) for row in results if row.id]
                
                logger.info(f"Found {len(accounts)} active accounts with EZTexting - Instance: {id(self)}")
                
                if len(accounts) == 0:
                    logger.warning("No active accounts with EZTexting found")
                
                return accounts
                
        except SQLAlchemyError as e:
            logger.error(f"Database error while fetching active accounts with EZTexting - Instance {id(self)}: {str(e)}")
            raise Exception(f"Failed to fetch active accounts with EZTexting: {str(e)}")
        
        except Exception as e:
            logger.error(f"Unexpected error while fetching active accounts with EZTexting - Instance {id(self)}: {str(e)}")
            raise Exception(f"Failed to fetch active accounts with EZTexting: {str(e)}")

    def get_inactive_accounts(self) -> List[str]:
        """Get all inactive accounts"""
        try:
            with self.get_session() as session:
                logger.debug(f"Querying inactive accounts - Instance: {id(self)}")
                
                results = session.query(Account.id).filter(
                    Account.status == 'INACTIVE',
                    Account.crm_config.isnot(None),
                    Account.crm_api_end_point.isnot(None)
                ).all()
                
                accounts = [str(row.id) for row in results if row.id]
                
                logger.info(f"Found {len(accounts)} inactive accounts - Instance: {id(self)}")
                
                if len(accounts) == 0:
                    logger.warning("No inactive accounts found")
                
                return accounts
                
        except SQLAlchemyError as e:
            logger.error(f"Database error while fetching inactive accounts - Instance {id(self)}: {str(e)}")
            raise Exception(f"Failed to fetch inactive accounts: {str(e)}")
        
        except Exception as e:
            logger.error(f"Unexpected error while fetching inactive accounts - Instance {id(self)}: {str(e)}")
            raise Exception(f"Failed to fetch inactive accounts: {str(e)}")

    def get_account_active_states(self, account_id: str) -> List[int]:
        """Get all active state IDs for a specific account"""
        try:
            with self.get_session() as session:
                logger.debug(f"Querying active states for account {account_id} - Instance: {id(self)}")
                
                results = session.query(States.id).filter(
                    States.account_id == int(account_id),
                    States.status == 'ACTIVE',
                    States.deleted_at.is_(None),
                    States.priority != '-1'
                ).all()
                
                state_ids = [row.id for row in results if row.id]
                
                logger.info(f"Found {len(state_ids)} active states for account {account_id} - Instance: {id(self)}")
                
                if len(state_ids) == 0:
                    logger.warning(f"No active states found for account {account_id}")
                
                return state_ids
                
        except SQLAlchemyError as e:
            logger.error(f"Database error while fetching states for account {account_id} - Instance {id(self)}: {str(e)}")
            raise Exception(f"Failed to fetch states for account {account_id}: {str(e)}")
        
        except Exception as e:
            logger.error(f"Unexpected error while fetching states for account {account_id} - Instance {id(self)}: {str(e)}")
            raise Exception(f"Failed to fetch states for account {account_id}: {str(e)}")


    
    def close(self):
        """
        Explicitly close this database manager's connections
        Only affects THIS instance, not others
        """
        if self.engine and self.is_initialized:
            try:
                # Log pool status before closing
                pool_status = self.get_pool_status()
                logger.info(f"Closing database connections - Instance {id(self)} - Pool status: {pool_status}")
                
                # Dispose of engine and all its connections
                self.engine.dispose()
                
                logger.info(f"Database engine disposed successfully - Instance: {id(self)}")
                
            except Exception as e:
                logger.warning(f"Error disposing database engine - Instance {id(self)}: {str(e)}")
            finally:
                self.engine = None
                self.SessionLocal = None
                self.is_initialized = False
        else:
            logger.debug(f"Database manager not initialized or already closed - Instance: {id(self)}")
    
    def __enter__(self):
        """Context manager entry - initialize if needed"""
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - automatic cleanup"""
        self.close()
    
    def __del__(self):
        """Destructor - ensure cleanup on garbage collection"""
        try:
            self.close()
        except Exception as e:
            # Use print instead of logger to avoid potential issues during cleanup
            print(f"Warning: Error in DatabaseManager destructor - Instance {id(self)}: {str(e)}")


# Factory function for creating database managers
def create_database_manager(pool_size: int = None, max_overflow: int = 4) -> DatabaseManager:
    """
    Factory function to create a new DatabaseManager instance
    
    Args:
        pool_size: Number of concurrent connections
        max_overflow: Additional connections for bursts
    
    Returns:
        DatabaseManager: New instance with independent connection pool
    """
    return DatabaseManager(pool_size=pool_size, max_overflow=max_overflow)


# Convenience functions for backward compatibility (optional)
def get_default_database_manager() -> DatabaseManager:
    """Get a default database manager (for simple use cases)"""
    return create_database_manager()
