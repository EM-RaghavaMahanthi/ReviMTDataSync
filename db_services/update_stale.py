"""
Update stale data functions for each table.
These functions check and update stale records after staging data insertion.
"""
import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine
from core.logger import setup_logging

# Setup logging using centralized configuration
setup_logging()
logger = logging.getLogger(__name__)


def update_customers(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """
    Update stale customer records.
    
    Args:
        engine: SQLAlchemy engine instance
        account_id: Account ID to filter records
        update: If True, perform updates. If False, only count changes.
        
    Returns:
        dict: {'expected': int, 'updated': int, 'success': bool, 'error': str (optional)}
    """
    logger.info("Checking stale customers...")
    try:
        # TODO: Implement stale customer check logic
        num_changes = 0  # Replace with actual count query
        
        if update:
            # TODO: Implement actual update logic
            rows_updated = 0
            logger.info(f"✅ Updated {rows_updated} stale customers")
        else:
            rows_updated = 0
            logger.info(f"📊 Found {num_changes} stale customers (update=False, no changes made)")
        
        return {
            'expected': num_changes,
            'updated': rows_updated if update else 0,
            'success': True
        }
    except Exception as e:
        logger.error(f"❌ Failed to check/update stale customers: {e}")
        return {
            'expected': 0,
            'updated': 0,
            'success': False,
            'error': str(e)
        }


def update_class_sessions(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """
    Update stale class_sessions records.
    
    Args:
        engine: SQLAlchemy engine instance
        account_id: Account ID to filter records
        update: If True, perform updates. If False, only count changes.
        
    Returns:
        bool: True if check/update successful, False otherwise
    """
    logger.info("Checking stale class_sessions...")
    try:
        # Count query - check how many records need updating
        count_query = text("""
            SELECT COUNT(*) as num_changes
            FROM class_sessions cs
            JOIN mt_class_sessions_details_dlk exp
              ON cs.account_id = exp.account_id
             AND cs.class_session_id = exp.class_session_id
            WHERE cs.account_id = :account_id
              AND exp.updated_at > cs.updated_at
              AND (
                   -- start_datetime (timestamp)
                   (cs.start_datetime IS NULL) <> (exp.start_datetime IS NULL)
                OR (
                     cs.start_datetime IS NOT NULL
                 AND exp.start_datetime IS NOT NULL
                 AND abs(extract(epoch from (cs.start_datetime - exp.start_datetime))) >= 0.1
                   )

                   -- end_datetime (timestamp)
                OR (cs.end_datetime IS NULL) <> (exp.end_datetime IS NULL)
                OR (
                     cs.end_datetime IS NOT NULL
                 AND exp.end_datetime IS NOT NULL
                 AND abs(extract(epoch from (cs.end_datetime - exp.end_datetime))) >= 0.1
                   )

                   -- start_date (TEXT)
                OR cs.start_date IS DISTINCT FROM exp.start_date

                   -- cancellation_datetime (timestamp)
                OR (cs.cancellation_datetime IS NULL) <> (exp.cancellation_datetime IS NULL)
                OR (
                     cs.cancellation_datetime IS NOT NULL
                 AND exp.cancellation_datetime IS NOT NULL
                 AND abs(extract(epoch from (cs.cancellation_datetime - exp.cancellation_datetime))) >= 0.1
                   )
              )
        """)
        
        with engine.connect() as conn:
            result = conn.execute(count_query, {"account_id": account_id})
            num_changes = result.scalar()
        
        logger.info(f"Found {num_changes} stale class_sessions records")
        
        if update:
            if num_changes == 0:
                logger.info("✅ No stale class_sessions to update")
                return {
                    'expected': 0,
                    'updated': 0,
                    'success': True
                }
            
            # Perform the update
            update_query = text("""
                UPDATE class_sessions cs
                SET
                  start_datetime        = exp.start_datetime,
                  end_datetime          = exp.end_datetime,
                  start_date            = exp.start_date,
                  cancellation_datetime = exp.cancellation_datetime,
                  updated_by            = 1,
                  updated_at            = now()
                FROM mt_class_sessions_details_dlk exp
                WHERE cs.account_id = exp.account_id
                  AND cs.class_session_id = exp.class_session_id
                  AND cs.account_id = :account_id
                  AND exp.updated_at > cs.updated_at
                  AND (
                       -- start_datetime (timestamp)
                       (cs.start_datetime IS NULL) <> (exp.start_datetime IS NULL)
                    OR (
                         cs.start_datetime IS NOT NULL
                     AND exp.start_datetime IS NOT NULL
                     AND abs(extract(epoch from (cs.start_datetime - exp.start_datetime))) >= 0.1
                       )

                       -- end_datetime (timestamp)
                    OR (cs.end_datetime IS NULL) <> (exp.end_datetime IS NULL)
                    OR (
                         cs.end_datetime IS NOT NULL
                     AND exp.end_datetime IS NOT NULL
                     AND abs(extract(epoch from (cs.end_datetime - exp.end_datetime))) >= 0.1
                       )

                       -- start_date (TEXT)
                    OR cs.start_date IS DISTINCT FROM exp.start_date

                       -- cancellation_datetime (timestamp)
                    OR (cs.cancellation_datetime IS NULL) <> (exp.cancellation_datetime IS NULL)
                    OR (
                         cs.cancellation_datetime IS NOT NULL
                     AND exp.cancellation_datetime IS NOT NULL
                     AND abs(extract(epoch from (cs.cancellation_datetime - exp.cancellation_datetime))) >= 0.1
                       )
                  )
            """)
            
            with engine.begin() as conn:
                update_result = conn.execute(update_query, {"account_id": account_id})
                rows_updated = update_result.rowcount
            
            # Verify count matches
            if rows_updated == num_changes:
                logger.info(f"✅ Successfully updated {rows_updated} stale class_sessions (matches expected count)")
            else:
                logger.warning(f"⚠️  Updated {rows_updated} class_sessions, but expected {num_changes} (mismatch!)")
            
            logger.info(f"✅ Updated {rows_updated} stale class_sessions")
        else:
            rows_updated = 0
            logger.info(f"📊 Found {num_changes} stale class_sessions (update=False, no changes made)")
        
        return {
            'expected': num_changes,
            'updated': rows_updated if update else 0,
            'success': True
        }
    except Exception as e:
        logger.error(f"❌ Failed to check/update stale class_sessions: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'expected': 0,
            'updated': 0,
            'success': False,
            'error': str(e)
        }


def update_orders(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """
    Update stale orders records.
    
    Args:
        engine: SQLAlchemy engine instance
        account_id: Account ID to filter records
        update: If True, perform updates. If False, only count changes.
        
    Returns:
        bool: True if check/update successful, False otherwise
    """
    logger.info("Checking stale orders...")
    try:
        # Count query - check how many records need updating
        count_query = text("""
            SELECT COUNT(*) as num_changes
            FROM orders o
            JOIN public.mt_orders_details_dlk exp
              ON exp.account_id = o.account_id
             AND exp.order_id   = o.order_id
            WHERE o.account_id = :account_id
              AND exp.updated_at > o.updated_at
              AND (
                   -- date_placed (timestamp/date) with NULL-safe + 0.1s tolerance
                   (o.date_placed IS NULL) <> (exp.date_placed IS NULL)
                OR (
                     o.date_placed IS NOT NULL
                 AND exp.date_placed IS NOT NULL
                 AND abs(extract(epoch from (o.date_placed - exp.date_placed))) >= 0.1
                   )

                   -- status (text)
                OR o.status IS DISTINCT FROM exp.status
              )
        """)
        
        with engine.connect() as conn:
            result = conn.execute(count_query, {"account_id": account_id})
            num_changes = result.scalar()
        
        logger.info(f"Found {num_changes} stale orders records")
        
        if update:
            if num_changes == 0:
                logger.info("✅ No stale orders to update")
                return {
                    'expected': 0,
                    'updated': 0,
                    'success': True
                }
            
            # Perform the update
            update_query = text("""
                UPDATE orders o
                SET
                  date_placed = exp.date_placed,
                  status      = exp.status,
                  updated_by  = 1,
                  updated_at  = now()
                FROM public.mt_orders_details_dlk exp
                WHERE exp.account_id = o.account_id
                  AND exp.order_id   = o.order_id
                  AND o.account_id = :account_id
                  AND exp.updated_at > o.updated_at
                  AND (
                       (o.date_placed IS NULL) <> (exp.date_placed IS NULL)
                    OR (
                         o.date_placed IS NOT NULL
                     AND exp.date_placed IS NOT NULL
                     AND abs(extract(epoch from (o.date_placed - exp.date_placed))) >= 0.1
                       )
                    OR o.status IS DISTINCT FROM exp.status
                  )
            """)
            
            with engine.begin() as conn:
                update_result = conn.execute(update_query, {"account_id": account_id})
                rows_updated = update_result.rowcount
            
            # Verify count matches
            if rows_updated == num_changes:
                logger.info(f"✅ Successfully updated {rows_updated} stale orders (matches expected count)")
            else:
                logger.warning(f"⚠️  Updated {rows_updated} orders, but expected {num_changes} (mismatch!)")
            
            logger.info(f"✅ Updated {rows_updated} stale orders")
        else:
            rows_updated = 0
            logger.info(f"📊 Found {num_changes} stale orders (update=False, no changes made)")
        
        return {
            'expected': num_changes,
            'updated': rows_updated if update else 0,
            'success': True
        }
    except Exception as e:
        logger.error(f"❌ Failed to check/update stale orders: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'expected': 0,
            'updated': 0,
            'success': False,
            'error': str(e)
        }


def update_credit_transactions(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """
    Update stale credit_transactions records.
    
    Args:
        engine: SQLAlchemy engine instance
        account_id: Account ID to filter records
        update: If True, perform updates. If False, only count changes.
        
    Returns:
        bool: True if check/update successful, False otherwise
    """
    logger.info("Checking stale credit_transactions...")
    try:
        # Count query - check how many records need updating
        count_query = text("""
            SELECT COUNT(*) as num_changes
            FROM credit_transactions ct
            JOIN mt_credit_transactions_details_dlk exp
              ON exp.account_id = ct.account_id
             AND exp.credit_transactions_id = ct.credit_transactions_id
            WHERE ct.account_id = :account_id
              AND exp.updated_at > ct.updated_at
              AND (
                   ct.credit_name IS DISTINCT FROM exp.credit_name
                OR ct.is_expired IS DISTINCT FROM exp.is_expired
                OR ct.is_intro_offer IS DISTINCT FROM exp.is_intro_offer
                OR ct.parent_credit_transaction_id IS DISTINCT FROM exp.parent_credit_transaction_id
              )
        """)
        
        with engine.connect() as conn:
            result = conn.execute(count_query, {"account_id": account_id})
            num_changes = result.scalar()
        
        logger.info(f"Found {num_changes} stale credit_transactions records")
        
        if update:
            if num_changes == 0:
                logger.info("✅ No stale credit_transactions to update")
                return {
                    'expected': 0,
                    'updated': 0,
                    'success': True
                }
            
            # Perform the update
            update_query = text("""
                UPDATE credit_transactions ct
                SET
                  credit_name                  = exp.credit_name,
                  is_expired                   = exp.is_expired,
                  is_intro_offer               = exp.is_intro_offer,
                  parent_credit_transaction_id = exp.parent_credit_transaction_id,
                  updated_by                   = 1,
                  updated_at                   = now()
                FROM mt_credit_transactions_details_dlk exp
                WHERE exp.account_id = ct.account_id
                  AND exp.credit_transactions_id = ct.credit_transactions_id
                  AND ct.account_id = :account_id
                  AND exp.updated_at > ct.updated_at
                  AND (
                       ct.credit_name IS DISTINCT FROM exp.credit_name
                    OR ct.is_expired IS DISTINCT FROM exp.is_expired
                    OR ct.is_intro_offer IS DISTINCT FROM exp.is_intro_offer
                    OR ct.parent_credit_transaction_id IS DISTINCT FROM exp.parent_credit_transaction_id
                  )
            """)
            
            with engine.begin() as conn:
                update_result = conn.execute(update_query, {"account_id": account_id})
                rows_updated = update_result.rowcount
            
            # Verify count matches
            if rows_updated == num_changes:
                logger.info(f"✅ Successfully updated {rows_updated} stale credit_transactions (matches expected count)")
            else:
                logger.warning(f"⚠️  Updated {rows_updated} credit_transactions, but expected {num_changes} (mismatch!)")
            
            logger.info(f"✅ Updated {rows_updated} stale credit_transactions")
        else:
            rows_updated = 0
            logger.info(f"📊 Found {num_changes} stale credit_transactions (update=False, no changes made)")
        
        return {
            'expected': num_changes,
            'updated': rows_updated if update else 0,
            'success': True
        }
    except Exception as e:
        logger.error(f"❌ Failed to check/update stale credit_transactions: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'expected': 0,
            'updated': 0,
            'success': False,
            'error': str(e)
        }


def update_credit_transactions_orders(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """
    Update stale credit_transactions_orders records.
    
    Args:
        engine: SQLAlchemy engine instance
        account_id: Account ID to filter records
        update: If True, perform updates. If False, only count changes.
        
    Returns:
        bool: True if check/update successful, False otherwise
    """
    logger.info("Checking stale credit_transactions_orders...")
    try:
        # Count query - check how many records need updating
        count_query = text("""
            SELECT COUNT(*) as num_changes
            FROM credit_transactions_orders cto
            JOIN mt_credit_transactions_details_dlk exp
              ON exp.account_id = cto.account_id
             AND exp.credit_transactions_id = cto.credit_transactions_id
            WHERE cto.account_id = :account_id
              AND exp.updated_at > cto.updated_at
              AND (
                   cto.credit_name IS DISTINCT FROM exp.credit_name
                OR cto.is_expired IS DISTINCT FROM exp.is_expired
                OR cto.is_intro_offer IS DISTINCT FROM exp.is_intro_offer
                OR cto.parent_credit_transaction_id IS DISTINCT FROM exp.parent_credit_transaction_id
                OR cto.remaining_credits_cache IS DISTINCT FROM exp.remaining_credits_cache
              )
        """)
        
        with engine.connect() as conn:
            result = conn.execute(count_query, {"account_id": account_id})
            num_changes = result.scalar()
        
        logger.info(f"Found {num_changes} stale credit_transactions_orders records")
        
        if update:
            if num_changes == 0:
                logger.info("✅ No stale credit_transactions_orders to update")
                return {
                    'expected': 0,
                    'updated': 0,
                    'success': True
                }
            
            # Perform the update
            update_query = text("""
                UPDATE credit_transactions_orders cto
                SET
                  credit_name                  = exp.credit_name,
                  is_expired                   = exp.is_expired,
                  is_intro_offer               = exp.is_intro_offer,
                  parent_credit_transaction_id = exp.parent_credit_transaction_id,
                  remaining_credits_cache      = exp.remaining_credits_cache,
                  updated_by                   = 1,
                  updated_at                   = now()
                FROM mt_credit_transactions_details_dlk exp
                WHERE exp.account_id = cto.account_id
                  AND exp.credit_transactions_id = cto.credit_transactions_id
                  AND cto.account_id = :account_id
                  AND exp.updated_at > cto.updated_at
                  AND (
                       cto.credit_name IS DISTINCT FROM exp.credit_name
                    OR cto.is_expired IS DISTINCT FROM exp.is_expired
                    OR cto.is_intro_offer IS DISTINCT FROM exp.is_intro_offer
                    OR cto.parent_credit_transaction_id IS DISTINCT FROM exp.parent_credit_transaction_id
                    OR cto.remaining_credits_cache IS DISTINCT FROM exp.remaining_credits_cache
                  )
            """)
            
            with engine.begin() as conn:
                update_result = conn.execute(update_query, {"account_id": account_id})
                rows_updated = update_result.rowcount
            
            # Verify count matches
            if rows_updated == num_changes:
                logger.info(f"✅ Successfully updated {rows_updated} stale credit_transactions_orders (matches expected count)")
            else:
                logger.warning(f"⚠️  Updated {rows_updated} credit_transactions_orders, but expected {num_changes} (mismatch!)")
            
            logger.info(f"✅ Updated {rows_updated} stale credit_transactions_orders")
        else:
            rows_updated = 0
            logger.info(f"📊 Found {num_changes} stale credit_transactions_orders (update=False, no changes made)")
        
        return {
            'expected': num_changes,
            'updated': rows_updated if update else 0,
            'success': True
        }
    except Exception as e:
        logger.error(f"❌ Failed to check/update stale credit_transactions_orders: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'expected': 0,
            'updated': 0,
            'success': False,
            'error': str(e)
        }


def update_membership_transactions(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """
    Update stale membership_transactions records.
    
    Args:
        engine: SQLAlchemy engine instance
        account_id: Account ID to filter records
        update: If True, perform updates. If False, only count changes.
        
    Returns:
        bool: True if check/update successful, False otherwise
    """
    logger.info("Checking stale membership_transactions...")
    try:
        # Count query - check how many records need updating
        count_query = text("""
            SELECT COUNT(*) as num_changes
            FROM membership_transactions mt
            JOIN mt_membership_transactions_details_dlk exp
              ON exp.account_id = mt.account_id
             AND exp.membership_transactions_id = mt.membership_transactions_id
            LEFT JOIN membership_instances mi
              ON mi.account_id = mt.account_id
             AND mi.membership_instances_id = exp.membership_instances_id
            WHERE mt.account_id = :account_id
              AND exp.updated_at > mt.updated_at
              AND (
                   mt.parent_membership_transaction_id IS DISTINCT FROM exp.parent_membership_transaction_id
                OR mt.membership_instances_id IS DISTINCT FROM exp.membership_instances_id
                OR mt.membership_instances_ref_id IS DISTINCT FROM mi.id
              )
        """)
        
        with engine.connect() as conn:
            result = conn.execute(count_query, {"account_id": account_id})
            num_changes = result.scalar()
        
        logger.info(f"Found {num_changes} stale membership_transactions records")
        
        if update:
            if num_changes == 0:
                logger.info("✅ No stale membership_transactions to update")
                return {
                    'expected': 0,
                    'updated': 0,
                    'success': True
                }
            
            # Step 1: Update original columns (parent_membership_transaction_id, membership_instances_id)
            update_query_1 = text("""
                UPDATE membership_transactions mt
                SET
                  parent_membership_transaction_id = exp.parent_membership_transaction_id,
                  membership_instances_id          = exp.membership_instances_id,
                  updated_by                       = 1,
                  updated_at                       = now()
                FROM mt_membership_transactions_details_dlk exp
                WHERE exp.account_id = mt.account_id
                  AND exp.membership_transactions_id = mt.membership_transactions_id
                  AND mt.account_id = :account_id
                  AND exp.updated_at > mt.updated_at
                  AND (
                       mt.parent_membership_transaction_id IS DISTINCT FROM exp.parent_membership_transaction_id
                    OR mt.membership_instances_id IS DISTINCT FROM exp.membership_instances_id
                  )
            """)
            
            # Step 2: Update ref_id (membership_instances_ref_id)
            update_query_2 = text("""
                UPDATE membership_transactions mt
                SET
                  membership_instances_ref_id = mi.id,
                  updated_by                  = 1,
                  updated_at                  = now()
                FROM mt_membership_transactions_details_dlk exp
                JOIN membership_instances mi
                  ON mi.account_id = exp.account_id
                 AND mi.membership_instances_id = exp.membership_instances_id
                WHERE exp.account_id = mt.account_id
                  AND exp.membership_transactions_id = mt.membership_transactions_id
                  AND mt.account_id = :account_id
                  AND exp.updated_at > mt.updated_at
                  AND mt.membership_instances_ref_id IS DISTINCT FROM mi.id
            """)
            
            with engine.begin() as conn:
                # Execute step 1
                update_result_1 = conn.execute(update_query_1, {"account_id": account_id})
                rows_updated_1 = update_result_1.rowcount
                
                # Execute step 2
                update_result_2 = conn.execute(update_query_2, {"account_id": account_id})
                rows_updated_2 = update_result_2.rowcount
            
            total_rows_updated = rows_updated_1 + rows_updated_2
            
            # Verify count matches (approximately - some rows might be updated in both steps)
            logger.info(f"✅ Updated {rows_updated_1} membership_transactions (original columns)")
            logger.info(f"✅ Updated {rows_updated_2} membership_transactions (ref_id)")
            logger.info(f"✅ Total row updates: {total_rows_updated} across {num_changes} membership_transactions")
        else:
            total_rows_updated = 0
            logger.info(f"📊 Found {num_changes} stale membership_transactions (update=False, no changes made)")
        
        return {
            'expected': num_changes,
            'updated': num_changes if update else 0,  # Report unique transactions, not total updates
            'total_row_updates': total_rows_updated if update else 0,  # Additional field for actual update count
            'success': True
        }
    except Exception as e:
        logger.error(f"❌ Failed to check/update stale membership_transactions: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'expected': 0,
            'updated': 0,
            'success': False,
            'error': str(e)
        }


def update_membership_transactions_orders(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """
    Update stale membership_transactions_orders records.
    
    Args:
        engine: SQLAlchemy engine instance
        account_id: Account ID to filter records
        update: If True, perform updates. If False, only count changes.
        
    Returns:
        bool: True if check/update successful, False otherwise
    """
    logger.info("Checking stale membership_transactions_orders...")
    try:
        # Count query - check how many records need updating
        count_query = text("""
            SELECT COUNT(*) as num_changes
            FROM membership_transactions_orders mto
            JOIN mt_membership_transactions_details_dlk exp
              ON exp.account_id = mto.account_id
             AND exp.membership_transactions_id = mto.membership_transactions_id
            LEFT JOIN membership_instances mi
              ON mi.account_id = mto.account_id
             AND mi.membership_instances_id = exp.membership_instances_id
            WHERE mto.account_id = :account_id
              AND exp.updated_at > mto.updated_at
              AND (
                   mto.parent_membership_transaction_id IS DISTINCT FROM exp.parent_membership_transaction_id
                OR mto.membership_instances_id IS DISTINCT FROM exp.membership_instances_id
                OR mto.membership_instances_ref_id IS DISTINCT FROM mi.id
                OR mto.payment_interval_end_date IS DISTINCT FROM exp.payment_interval_end_date
              )
        """)
        
        with engine.connect() as conn:
            result = conn.execute(count_query, {"account_id": account_id})
            num_changes = result.scalar()
        
        logger.info(f"Found {num_changes} stale membership_transactions_orders records")
        
        if update:
            if num_changes == 0:
                logger.info("✅ No stale membership_transactions_orders to update")
                return {
                    'expected': 0,
                    'updated': 0,
                    'success': True
                }
            
            # Step 1: Update original columns (parent_membership_transaction_id, membership_instances_id, payment_interval_end_date)
            update_query_1 = text("""
                UPDATE membership_transactions_orders mto
                SET
                  parent_membership_transaction_id = exp.parent_membership_transaction_id,
                  membership_instances_id          = exp.membership_instances_id,
                  payment_interval_end_date        = exp.payment_interval_end_date,
                  updated_by                       = 1,
                  updated_at                       = now()
                FROM mt_membership_transactions_details_dlk exp
                WHERE exp.account_id = mto.account_id
                  AND exp.membership_transactions_id = mto.membership_transactions_id
                  AND mto.account_id = :account_id
                  AND exp.updated_at > mto.updated_at
                  AND (
                       mto.parent_membership_transaction_id IS DISTINCT FROM exp.parent_membership_transaction_id
                    OR mto.membership_instances_id IS DISTINCT FROM exp.membership_instances_id
                    OR mto.payment_interval_end_date IS DISTINCT FROM exp.payment_interval_end_date
                  )
            """)
            
            # Step 2: Update ref_id (membership_instances_ref_id)
            update_query_2 = text("""
                UPDATE membership_transactions_orders mto
                SET
                  membership_instances_ref_id = mi.id,
                  updated_by                  = 1,
                  updated_at                  = now()
                FROM mt_membership_transactions_details_dlk exp
                JOIN membership_instances mi
                  ON mi.account_id = exp.account_id
                 AND mi.membership_instances_id = exp.membership_instances_id
                WHERE exp.account_id = mto.account_id
                  AND exp.membership_transactions_id = mto.membership_transactions_id
                  AND mto.account_id = :account_id
                  AND exp.updated_at > mto.updated_at
                  AND mto.membership_instances_ref_id IS DISTINCT FROM mi.id
            """)
            
            with engine.begin() as conn:
                # Execute step 1
                update_result_1 = conn.execute(update_query_1, {"account_id": account_id})
                rows_updated_1 = update_result_1.rowcount
                
                # Execute step 2
                update_result_2 = conn.execute(update_query_2, {"account_id": account_id})
                rows_updated_2 = update_result_2.rowcount
            
            total_rows_updated = rows_updated_1 + rows_updated_2
            
            # Verify count matches (approximately - some rows might be updated in both steps)
            logger.info(f"✅ Updated {rows_updated_1} membership_transactions_orders (original columns)")
            logger.info(f"✅ Updated {rows_updated_2} membership_transactions_orders (ref_id)")
            logger.info(f"✅ Total row updates: {total_rows_updated} across {num_changes} membership_transactions_orders")
        else:
            total_rows_updated = 0
            logger.info(f"📊 Found {num_changes} stale membership_transactions_orders (update=False, no changes made)")
        
        return {
            'expected': num_changes,
            'updated': num_changes if update else 0,  # Report unique transactions, not total updates
            'total_row_updates': total_rows_updated if update else 0,  # Additional field for actual update count
            'success': True
        }
    except Exception as e:
        logger.error(f"❌ Failed to check/update stale membership_transactions_orders: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'expected': 0,
            'updated': 0,
            'success': False,
            'error': str(e)
        }


def update_membership_instances(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """
    Update stale membership_instances records.
    
    Args:
        engine: SQLAlchemy engine instance
        account_id: Account ID to filter records
        update: If True, perform updates. If False, only count changes.
        
    Returns:
        bool: True if check/update successful, False otherwise
    """
    logger.info("Checking stale membership_instances...")
    try:
        # Count query - check how many records need updating
        count_query = text("""
            SELECT COUNT(*) as num_changes
            FROM membership_instances mi
            JOIN public.mt_membership_instances_details_dlk exp
              ON exp.account_id = mi.account_id
             AND exp.membership_instances_id = mi.membership_instances_id
            WHERE mi.account_id = :account_id
              AND exp.updated_at > mi.updated_at
              AND (
                   -- next_charge_date (timestamp/date) with NULL-safe + 0.1s tolerance
                   (mi.next_charge_date IS NULL) <> (exp.next_charge_date IS NULL)
                OR (
                     mi.next_charge_date IS NOT NULL
                 AND exp.next_charge_date IS NOT NULL
                 AND abs(extract(epoch from (mi.next_charge_date - exp.next_charge_date))) >= 0.1
                   )

                   -- purchase_date (timestamp/date) with NULL-safe + 0.1s tolerance
                OR (mi.purchase_date IS NULL) <> (exp.purchase_date IS NULL)
                OR (
                     mi.purchase_date IS NOT NULL
                 AND exp.purchase_date IS NOT NULL
                 AND abs(extract(epoch from (mi.purchase_date - exp.purchase_date))) >= 0.1
                   )

                   -- membership_name (text)
                OR mi.membership_name IS DISTINCT FROM exp.membership_name

                   -- status (text)
                OR mi.status IS DISTINCT FROM exp.status
              )
        """)
        
        with engine.connect() as conn:
            result = conn.execute(count_query, {"account_id": account_id})
            num_changes = result.scalar()
        
        logger.info(f"Found {num_changes} stale membership_instances records")
        
        if update:
            if num_changes == 0:
                logger.info("✅ No stale membership_instances to update")
                return {
                    'expected': 0,
                    'updated': 0,
                    'success': True
                }
            
            # Perform the update
            update_query = text("""
                UPDATE membership_instances mi
                SET
                  next_charge_date = exp.next_charge_date,
                  purchase_date    = exp.purchase_date,
                  membership_name  = exp.membership_name,
                  status           = exp.status,
                  updated_by       = 1,
                  updated_at       = now()
                FROM public.mt_membership_instances_details_dlk exp
                WHERE exp.account_id = mi.account_id
                  AND exp.membership_instances_id = mi.membership_instances_id
                  AND mi.account_id = :account_id
                  AND exp.updated_at > mi.updated_at
                  AND (
                       (mi.next_charge_date IS NULL) <> (exp.next_charge_date IS NULL)
                    OR (
                         mi.next_charge_date IS NOT NULL
                     AND exp.next_charge_date IS NOT NULL
                     AND abs(extract(epoch from (mi.next_charge_date - exp.next_charge_date))) >= 0.1
                       )

                    OR (mi.purchase_date IS NULL) <> (exp.purchase_date IS NULL)
                    OR (
                         mi.purchase_date IS NOT NULL
                     AND exp.purchase_date IS NOT NULL
                     AND abs(extract(epoch from (mi.purchase_date - exp.purchase_date))) >= 0.1
                       )

                    OR mi.membership_name IS DISTINCT FROM exp.membership_name
                    OR mi.status IS DISTINCT FROM exp.status
                  )
            """)
            
            with engine.begin() as conn:
                update_result = conn.execute(update_query, {"account_id": account_id})
                rows_updated = update_result.rowcount
            
            # Verify count matches
            if rows_updated == num_changes:
                logger.info(f"✅ Successfully updated {rows_updated} stale membership_instances (matches expected count)")
            else:
                logger.warning(f"⚠️  Updated {rows_updated} membership_instances, but expected {num_changes} (mismatch!)")
            
            logger.info(f"✅ Updated {rows_updated} stale membership_instances")
        else:
            rows_updated = 0
            logger.info(f"📊 Found {num_changes} stale membership_instances (update=False, no changes made)")
        
        return {
            'expected': num_changes,
            'updated': rows_updated if update else 0,
            'success': True
        }
    except Exception as e:
        logger.error(f"❌ Failed to check/update stale membership_instances: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'expected': 0,
            'updated': 0,
            'success': False,
            'error': str(e)
        }


def update_order_lines(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """
    Update stale order_lines records.
    
    Args:
        engine: SQLAlchemy engine instance
        account_id: Account ID to filter records
        update: If True, perform updates. If False, only count changes.
        
    Returns:
        bool: True if check/update successful, False otherwise
    """
    logger.info("Checking stale order_lines...")
    try:
        # Count query - check how many records need updating
        count_query = text("""
            SELECT COUNT(*) as num_changes
            FROM order_lines ol
            JOIN public.mt_order_lines_details_dlk exp
              ON exp.account_id = ol.account_id
             AND exp.order_line_id = ol.order_line_id
            WHERE ol.account_id = :account_id
              AND exp.updated_at > ol.updated_at
              AND (
                   ol.title IS DISTINCT FROM exp.title
                OR ol.transaction_type IS DISTINCT FROM exp.transaction_type
              )
        """)
        
        with engine.connect() as conn:
            result = conn.execute(count_query, {"account_id": account_id})
            num_changes = result.scalar()
        
        logger.info(f"Found {num_changes} stale order_lines records")
        
        if update:
            if num_changes == 0:
                logger.info("✅ No stale order_lines to update")
                return {
                    'expected': 0,
                    'updated': 0,
                    'success': True
                }
            
            # Perform the update
            update_query = text("""
                UPDATE order_lines ol
                SET
                  title            = exp.title,
                  transaction_type = exp.transaction_type,
                  updated_by       = 1,
                  updated_at       = now()
                FROM public.mt_order_lines_details_dlk exp
                WHERE exp.account_id = ol.account_id
                  AND exp.order_line_id = ol.order_line_id
                  AND ol.account_id = :account_id
                  AND exp.updated_at > ol.updated_at
                  AND (
                       ol.title IS DISTINCT FROM exp.title
                    OR ol.transaction_type IS DISTINCT FROM exp.transaction_type
                  )
            """)
            
            with engine.begin() as conn:
                update_result = conn.execute(update_query, {"account_id": account_id})
                rows_updated = update_result.rowcount
            
            # Verify count matches
            if rows_updated == num_changes:
                logger.info(f"✅ Successfully updated {rows_updated} stale order_lines (matches expected count)")
            else:
                logger.warning(f"⚠️  Updated {rows_updated} order_lines, but expected {num_changes} (mismatch!)")
            
            logger.info(f"✅ Updated {rows_updated} stale order_lines")
        else:
            rows_updated = 0
            logger.info(f"📊 Found {num_changes} stale order_lines (update=False, no changes made)")
        
        return {
            'expected': num_changes,
            'updated': rows_updated if update else 0,
            'success': True
        }
    except Exception as e:
        logger.error(f"❌ Failed to check/update stale order_lines: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'expected': 0,
            'updated': 0,
            'success': False,
            'error': str(e)
        }


def delete_invalid_order_lines(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """
    Delete invalid order_lines from main table where is_valid = FALSE in staging.
    
    This removes order_lines that have child_orders (deferred payment placeholders)
    where the child orders exist in the orders table.
    
    Args:
        engine: SQLAlchemy engine instance
        account_id: Account ID to filter records
        update: If True, perform deletion. If False, only count records to delete.
        
    Returns:
        dict: {'expected': int, 'deleted': int, 'success': bool, 'error': str (optional)}
    """
    logger.info("Checking invalid order_lines to delete...")
    try:
        # Count query - check how many invalid order_lines exist in main table
        count_query = text("""
            SELECT COUNT(*) as num_invalid
            FROM order_lines ol
            JOIN mt_order_lines_details_dlk stg
              ON stg.account_id = ol.account_id
             AND stg.order_line_id = ol.order_line_id
            WHERE ol.account_id = :account_id
              AND stg.is_valid = FALSE
        """)
        
        with engine.connect() as conn:
            result = conn.execute(count_query, {"account_id": account_id})
            num_invalid = result.scalar()
        
        logger.info(f"Found {num_invalid} invalid order_lines records to delete")
        
        if update:
            if num_invalid == 0:
                logger.info("✅ No invalid order_lines to delete")
                return {
                    'expected': 0,
                    'deleted': 0,
                    'success': True
                }
            
            # Perform the deletion
            delete_query = text("""
                DELETE FROM order_lines ol
                USING mt_order_lines_details_dlk stg
                WHERE stg.account_id = ol.account_id
                  AND stg.order_line_id = ol.order_line_id
                  AND ol.account_id = :account_id
                  AND stg.is_valid = FALSE
            """)
            
            with engine.begin() as conn:
                delete_result = conn.execute(delete_query, {"account_id": account_id})
                rows_deleted = delete_result.rowcount
            
            # Verify count matches
            if rows_deleted == num_invalid:
                logger.info(f"✅ Successfully deleted {rows_deleted} invalid order_lines (matches expected count)")
            else:
                logger.warning(f"⚠️  Deleted {rows_deleted} order_lines, but expected {num_invalid} (mismatch!)")
            
            logger.info(f"✅ Deleted {rows_deleted} invalid order_lines")
        else:
            rows_deleted = 0
            logger.info(f"📊 Found {num_invalid} invalid order_lines (update=False, no deletion performed)")
        
        return {
            'expected': num_invalid,
            'deleted': rows_deleted if update else 0,
            'success': True
        }
    except Exception as e:
        logger.error(f"❌ Failed to check/delete invalid order_lines: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'expected': 0,
            'deleted': 0,
            'success': False,
            'error': str(e)
        }


def update_reservations(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """
    Update stale reservations records.
    
    Args:
        engine: SQLAlchemy engine instance
        account_id: Account ID to filter records
        update: If True, perform updates. If False, only count changes.
        
    Returns:
        bool: True if check/update successful, False otherwise
    """
    logger.info("Checking stale reservations...")
    try:
        # Count query - check how many records need updating
        count_query = text("""
            SELECT COUNT(*) AS num_changes
            FROM public.reservations r
            JOIN public.mt_reservations_details_dlk exp
            ON exp.account_id = r.account_id
            AND exp.reservations_id = r.reservations_id
            WHERE r.account_id = :account_id
            AND exp.updated_at > r.updated_at
            AND (
                -- main field diffs
                r.cancel_date                   IS DISTINCT FROM exp.cancel_date
                OR r.check_in_date                 IS DISTINCT FROM exp.check_in_date
                OR r.creation_date                 IS DISTINCT FROM exp.creation_date
                OR r.status                        IS DISTINCT FROM exp.status
                OR r.credit_transactions_type      IS DISTINCT FROM exp.credit_transactions_type
                OR r.credit_transactions_id        IS DISTINCT FROM exp.credit_transactions_id
                OR r.membership_transactions_type  IS DISTINCT FROM exp.membership_transactions_type
                OR r.membership_transactions_id    IS DISTINCT FROM exp.membership_transactions_id

                -- credit ref_id mismatch (only if exp has a credit_transactions_id)
                OR (
                    exp.credit_transactions_id IS NOT NULL
                    AND EXISTS (
                    SELECT 1
                    FROM public.credit_transactions ct
                    WHERE ct.account_id = r.account_id
                        AND ct.credit_transactions_id = exp.credit_transactions_id
                        AND r.credit_transactions_ref_id IS DISTINCT FROM ct.id
                    )
                )

                -- membership ref_id mismatch (only if exp has a membership_transactions_id)
                OR (
                    exp.membership_transactions_id IS NOT NULL
                    AND EXISTS (
                    SELECT 1
                    FROM public.membership_transactions mt
                    WHERE mt.account_id = r.account_id
                        AND mt.membership_transactions_id = exp.membership_transactions_id
                        AND r.membership_transactions_ref_id IS DISTINCT FROM mt.id
                    )
                )
            )
        """)

        
        with engine.connect() as conn:
            result = conn.execute(count_query, {"account_id": account_id})
            num_changes = result.scalar()
        
        logger.info(f"Found {num_changes} stale reservations records")
        
        if update:
            if num_changes == 0:
                logger.info("✅ No stale reservations to update")
                return {
                    'expected': 0,
                    'updated': 0,
                    'success': True
                }
            
            # Step 1: Update main fields (cancel_date, check_in_date, creation_date, status, transaction types/ids)
            update_query_1 = text("""
                UPDATE reservations r
                SET
                  cancel_date                  = exp.cancel_date,
                  check_in_date                = exp.check_in_date,
                  creation_date                = exp.creation_date,
                  status                       = exp.status,
                  credit_transactions_type     = exp.credit_transactions_type,
                  credit_transactions_id       = exp.credit_transactions_id,
                  membership_transactions_type = exp.membership_transactions_type,
                  membership_transactions_id   = exp.membership_transactions_id,
                  updated_by                   = 1,
                  updated_at                   = now()
                FROM public.mt_reservations_details_dlk exp
                WHERE exp.account_id = r.account_id
                  AND exp.reservations_id = r.reservations_id
                  AND r.account_id = :account_id
                  AND exp.updated_at > r.updated_at
                  AND (
                       r.cancel_date                   IS DISTINCT FROM exp.cancel_date
                    OR r.check_in_date                 IS DISTINCT FROM exp.check_in_date
                    OR r.creation_date                 IS DISTINCT FROM exp.creation_date
                    OR r.status                        IS DISTINCT FROM exp.status
                    OR r.credit_transactions_type      IS DISTINCT FROM exp.credit_transactions_type
                    OR r.credit_transactions_id        IS DISTINCT FROM exp.credit_transactions_id
                    OR r.membership_transactions_type  IS DISTINCT FROM exp.membership_transactions_type
                    OR r.membership_transactions_id    IS DISTINCT FROM exp.membership_transactions_id
                  )
            """)
            
            # Step 2: Update credit_transactions_ref_id
            update_query_2 = text("""
                UPDATE reservations r
                SET
                  credit_transactions_ref_id = ct.id,
                  updated_by                 = 1,
                  updated_at                 = now()
                FROM credit_transactions ct
                WHERE r.account_id = :account_id
                  AND ct.account_id = r.account_id
                  AND ct.credit_transactions_id = r.credit_transactions_id
                  AND r.credit_transactions_ref_id IS DISTINCT FROM ct.id
            """)
            
            # Step 3: Update membership_transactions_ref_id
            update_query_3 = text("""
                UPDATE reservations r
                SET
                  membership_transactions_ref_id = mt.id,
                  updated_by                     = 1,
                  updated_at                     = now()
                FROM membership_transactions mt
                WHERE r.account_id = :account_id
                  AND mt.account_id = r.account_id
                  AND mt.membership_transactions_id = r.membership_transactions_id
                  AND r.membership_transactions_ref_id IS DISTINCT FROM mt.id
            """)
            
            with engine.begin() as conn:
                # Execute step 1
                update_result_1 = conn.execute(update_query_1, {"account_id": account_id})
                rows_updated_1 = update_result_1.rowcount
                
                # Execute step 2
                update_result_2 = conn.execute(update_query_2, {"account_id": account_id})
                rows_updated_2 = update_result_2.rowcount
                
                # Execute step 3
                update_result_3 = conn.execute(update_query_3, {"account_id": account_id})
                rows_updated_3 = update_result_3.rowcount
            
            total_rows_updated = rows_updated_1 + rows_updated_2 + rows_updated_3
            
            # Log results for each step
            logger.info(f"✅ Updated {rows_updated_1} reservations (main fields)")
            logger.info(f"✅ Updated {rows_updated_2} reservations (credit_transactions_ref_id)")
            logger.info(f"✅ Updated {rows_updated_3} reservations (membership_transactions_ref_id)")
            logger.info(f"✅ Total row updates: {total_rows_updated} across {num_changes} reservations")
        else:
            total_rows_updated = 0
            logger.info(f"📊 Found {num_changes} stale reservations (update=False, no changes made)")
        
        return {
            'expected': num_changes,
            'updated': num_changes if update else 0,  # Report unique reservations, not total updates
            'total_row_updates': total_rows_updated if update else 0,  # Additional field for actual update count
            'success': True
        }
    except Exception as e:
        logger.error(f"❌ Failed to check/update stale reservations: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'expected': 0,
            'updated': 0,
            'success': False,
            'error': str(e)
        }


def update_stale_data(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """
    Orchestrate stale data checks/updates for all tables in the correct order.
    Order matches TABLE_COLUMNS_MAP in main_bulk_insert_service.py:
    1. customers
    2. class_sessions
    3. orders
    4. credit_transactions
    5. membership_transactions
    6. membership_instances
    7. order_lines
    8. reservations
    
    STRICT MODE: Stops on first failure and returns immediately.
    
    Args:
        engine: SQLAlchemy engine instance
        account_id: Account ID to filter records
        update: If True, perform updates. If False, only count changes to be made.
        
    Returns:
        dict: Dictionary with results for each table:
            {
                'success': bool,
                'successful_tables': list,
                'failed_table': str (optional),
                'remaining_tables': list,
                'elapsed_time_seconds': float,
                'tables': {
                    'table_name': {
                        'expected': int,
                        'updated': int,
                        'success': bool,
                        'error': str (optional),
                        'elapsed_time_seconds': float
                    }
                }
            }
    """
    import time
    
    overall_start_time = time.time()
    
    logger.info("=" * 80)
    logger.info(f"🔍 Starting stale data {'update' if update else 'check'} process...")
    logger.info("⚠️  STRICT MODE: Will stop on first failure")
    if not update:
        logger.info("⚠️  update=False - Will only count changes, no updates will be made")
    logger.info("=" * 80)
    
    update_functions = [
        ("customers", update_customers),
        ("class_sessions", update_class_sessions),
        ("membership_instances", update_membership_instances),
        ("orders", update_orders),
        ("credit_transactions", update_credit_transactions),
        ("credit_transactions_orders", update_credit_transactions_orders),
        ("membership_transactions", update_membership_transactions),
        ("membership_transactions_orders", update_membership_transactions_orders),
        ("delete_invalid_order_lines", delete_invalid_order_lines),  # Delete invalid order_lines FIRST
        ("order_lines", update_order_lines),  # Then update stale order_lines
        ("reservations", update_reservations),
    ]
    
    results = {
        'success': True,
        'successful_tables': [],
        'failed_table': None,
        'remaining_tables': [],
        'elapsed_time_seconds': 0.0,
        'tables': {}
    }
    
    for idx, (table_name, update_func) in enumerate(update_functions):
        try:
            logger.info(f"Processing {table_name}...")
            table_start_time = time.time()
            
            table_result = update_func(engine, account_id, update)
            
            table_end_time = time.time()
            table_elapsed = round(table_end_time - table_start_time, 2)
            table_result['elapsed_time_seconds'] = table_elapsed
            
            results['tables'][table_name] = table_result
            
            logger.info(f"⏱️  {table_name} completed in {table_elapsed}s")
            
            if not table_result.get('success', False):
                # STRICT MODE: Stop on first failure
                results['success'] = False
                results['failed_table'] = table_name
                results['remaining_tables'] = [t[0] for t in update_functions[idx + 1:]]
                
                logger.error(f"❌ STRICT MODE: Stopping due to failure in {table_name}")
                logger.error(f"   Successful tables: {', '.join(results['successful_tables']) if results['successful_tables'] else 'None'}")
                logger.error(f"   Failed table: {table_name}")
                logger.error(f"   Remaining tables (not processed): {', '.join(results['remaining_tables']) if results['remaining_tables'] else 'None'}")
                break  # Stop immediately on failure
            else:
                results['successful_tables'].append(table_name)
                
        except Exception as e:
            # STRICT MODE: Stop on exception
            table_end_time = time.time()
            table_elapsed = round(table_end_time - table_start_time, 2)
            
            logger.error(f"❌ Exception during {table_name} stale check/update: {e}")
            import traceback
            logger.error(traceback.format_exc())
            results['tables'][table_name] = {
                'expected': 0,
                'updated': 0,
                'success': False,
                'error': str(e),
                'elapsed_time_seconds': table_elapsed
            }
            results['success'] = False
            results['failed_table'] = table_name
            results['remaining_tables'] = [t[0] for t in update_functions[idx + 1:]]
            
            logger.error(f"❌ STRICT MODE: Stopping due to exception in {table_name}")
            logger.error(f"   Successful tables: {', '.join(results['successful_tables']) if results['successful_tables'] else 'None'}")
            logger.error(f"   Failed table: {table_name}")
            logger.error(f"   Remaining tables (not processed): {', '.join(results['remaining_tables']) if results['remaining_tables'] else 'None'}")
            break  # Stop immediately on exception
    
    # Calculate overall elapsed time
    overall_end_time = time.time()
    overall_elapsed = round(overall_end_time - overall_start_time, 2)
    results['elapsed_time_seconds'] = overall_elapsed
    
    # Summary
    logger.info("=" * 80)
    logger.info(f"📊 STALE DATA {'UPDATE' if update else 'CHECK'} SUMMARY (STRICT MODE)")
    logger.info(f"⏱️  Overall Time Elapsed: {overall_elapsed}s")
    logger.info("=" * 80)
    
    total_tables = len(update_functions)
    successful_count = len(results['successful_tables'])
    failed_count = 1 if results['failed_table'] else 0
    remaining_count = len(results['remaining_tables'])
    
    logger.info(f"✅ Successful: {successful_count}/{total_tables} tables")
    logger.info(f"❌ Failed: {failed_count}/{total_tables} tables")
    logger.info(f"⏭️  Remaining (not processed): {remaining_count}/{total_tables} tables")
    
    # Log successful tables
    if results['successful_tables']:
        logger.info(f"\n✅ Successful tables: {', '.join(results['successful_tables'])}")
    
    # Log failed table
    if results['failed_table']:
        logger.error(f"\n❌ Failed table: {results['failed_table']}")
        failed_result = results['tables'].get(results['failed_table'], {})
        error = failed_result.get('error', 'Unknown error')
        expected = failed_result.get('expected', 0)
        updated = failed_result.get('updated', 0)
        elapsed = failed_result.get('elapsed_time_seconds', 0)
        logger.error(f"   Error: {error}")
        logger.error(f"   Expected updates: {expected}, Completed: {updated}")
        logger.error(f"   Time before failure: {elapsed}s")
    
    # Log remaining tables (not processed due to strict mode)
    if results['remaining_tables']:
        logger.warning(f"\n⏭️  Remaining tables (not processed): {', '.join(results['remaining_tables'])}")
    
    # Log details for each processed table
    logger.info("\n📋 Detailed Results:")
    for table_name, table_result in results['tables'].items():
        expected = table_result.get('expected', 0)
        updated = table_result.get('updated', 0)
        success = table_result.get('success', False)
        elapsed = table_result.get('elapsed_time_seconds', 0)
        
        if success:
            logger.info(f"  ✅ {table_name}: expected={expected}, updated={updated}, time={elapsed}s")
        else:
            error = table_result.get('error', 'Unknown error')
            logger.error(f"  ❌ {table_name}: expected={expected}, updated={updated}, time={elapsed}s, error={error}")
    
    logger.info("=" * 80)
    logger.info(f"⏱️  Total Time Elapsed: {overall_elapsed}s")
    logger.info("=" * 80)
    
    return results

