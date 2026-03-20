"""
Stale data updater — SQL embedded as Python constants, no file I/O.
"""
import time
import logging
import traceback
from sqlalchemy import text
from sqlalchemy.engine import Engine
from core.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count(sql: str, engine: Engine, account_id: str) -> int:
    with engine.connect() as conn:
        return conn.execute(text(sql), {"account_id": account_id}).scalar()


def _execute(sql: str, engine: Engine, account_id: str, label: str) -> int:
    with engine.begin() as conn:
        rows = conn.execute(text(sql), {"account_id": account_id}).rowcount
    logger.info(f"  {label}: {rows} rows affected")
    return rows


def _result(expected: int, updated: int, success: bool, error: str = None) -> dict:
    r = {"expected": expected, "updated": updated, "success": success}
    if error:
        r["error"] = error
    return r


# ---------------------------------------------------------------------------
# Per-table functions
# ---------------------------------------------------------------------------

def update_class_sessions(engine: Engine, account_id: str, update: bool) -> dict:
    logger.info("[class_sessions] checking stale records...")
    try:
        num = _count("""
            SELECT COUNT(*) FROM class_sessions cs
            JOIN mt_class_sessions_details_dlk exp
              ON cs.account_id = exp.account_id
             AND cs.class_session_id = exp.class_session_id
            WHERE cs.account_id = :account_id
              AND exp.updated_at > cs.updated_at
              AND (
                   (cs.start_datetime IS NULL) <> (exp.start_datetime IS NULL)
                OR (cs.start_datetime IS NOT NULL AND exp.start_datetime IS NOT NULL
                    AND abs(extract(epoch from (cs.start_datetime - exp.start_datetime))) >= 0.1)
                OR (cs.end_datetime IS NULL) <> (exp.end_datetime IS NULL)
                OR (cs.end_datetime IS NOT NULL AND exp.end_datetime IS NOT NULL
                    AND abs(extract(epoch from (cs.end_datetime - exp.end_datetime))) >= 0.1)
                OR cs.start_date IS DISTINCT FROM exp.start_date
                OR (cs.cancellation_datetime IS NULL) <> (exp.cancellation_datetime IS NULL)
                OR (cs.cancellation_datetime IS NOT NULL AND exp.cancellation_datetime IS NOT NULL
                    AND abs(extract(epoch from (cs.cancellation_datetime - exp.cancellation_datetime))) >= 0.1)
              )
        """, engine, account_id)
        logger.info(f"[class_sessions] {num} stale records found")
        if not update or num == 0:
            return _result(num, 0, True)
        _execute("""
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
                   (cs.start_datetime IS NULL) <> (exp.start_datetime IS NULL)
                OR (cs.start_datetime IS NOT NULL AND exp.start_datetime IS NOT NULL
                    AND abs(extract(epoch from (cs.start_datetime - exp.start_datetime))) >= 0.1)
                OR (cs.end_datetime IS NULL) <> (exp.end_datetime IS NULL)
                OR (cs.end_datetime IS NOT NULL AND exp.end_datetime IS NOT NULL
                    AND abs(extract(epoch from (cs.end_datetime - exp.end_datetime))) >= 0.1)
                OR cs.start_date IS DISTINCT FROM exp.start_date
                OR (cs.cancellation_datetime IS NULL) <> (exp.cancellation_datetime IS NULL)
                OR (cs.cancellation_datetime IS NOT NULL AND exp.cancellation_datetime IS NOT NULL
                    AND abs(extract(epoch from (cs.cancellation_datetime - exp.cancellation_datetime))) >= 0.1)
              )
        """, engine, account_id, "update datetime fields")
        return _result(num, num, True)
    except Exception as e:
        logger.error(f"[class_sessions] failed: {e}\n{traceback.format_exc()}")
        return _result(0, 0, False, str(e))


def update_orders(engine: Engine, account_id: str, update: bool) -> dict:
    logger.info("[orders] checking stale records...")
    try:
        num = _count("""
            SELECT COUNT(*) FROM orders o
            JOIN public.mt_orders_details_dlk exp
              ON exp.account_id = o.account_id
             AND exp.order_id   = o.order_id
            WHERE o.account_id = :account_id
              AND exp.updated_at > o.updated_at
              AND (
                   (o.date_placed IS NULL) <> (exp.date_placed IS NULL)
                OR (o.date_placed IS NOT NULL AND exp.date_placed IS NOT NULL
                    AND abs(extract(epoch from (o.date_placed - exp.date_placed))) >= 0.1)
                OR o.status IS DISTINCT FROM exp.status
              )
        """, engine, account_id)
        logger.info(f"[orders] {num} stale records found")
        if not update or num == 0:
            return _result(num, 0, True)
        _execute("""
            UPDATE orders o
            SET
              date_placed = exp.date_placed,
              status      = exp.status,
              updated_by  = 1,
              updated_at  = now()
            FROM public.mt_orders_details_dlk exp
            WHERE exp.account_id = o.account_id
              AND exp.order_id   = o.order_id
              AND o.account_id   = :account_id
              AND exp.updated_at > o.updated_at
              AND (
                   (o.date_placed IS NULL) <> (exp.date_placed IS NULL)
                OR (o.date_placed IS NOT NULL AND exp.date_placed IS NOT NULL
                    AND abs(extract(epoch from (o.date_placed - exp.date_placed))) >= 0.1)
                OR o.status IS DISTINCT FROM exp.status
              )
        """, engine, account_id, "update date_placed + status")
        return _result(num, num, True)
    except Exception as e:
        logger.error(f"[orders] failed: {e}\n{traceback.format_exc()}")
        return _result(0, 0, False, str(e))


def update_credit_transactions(engine: Engine, account_id: str, update: bool) -> dict:
    logger.info("[credit_transactions] checking stale records...")
    try:
        num = _count("""
            SELECT COUNT(*) FROM credit_transactions ct
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
        """, engine, account_id)
        logger.info(f"[credit_transactions] {num} stale records found")
        if not update or num == 0:
            return _result(num, 0, True)
        _execute("""
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
        """, engine, account_id, "update credit fields")
        return _result(num, num, True)
    except Exception as e:
        logger.error(f"[credit_transactions] failed: {e}\n{traceback.format_exc()}")
        return _result(0, 0, False, str(e))


def update_credit_transactions_orders(engine: Engine, account_id: str, update: bool) -> dict:
    logger.info("[credit_transactions_orders] checking stale records...")
    try:
        num = _count("""
            SELECT COUNT(*) FROM credit_transactions_orders cto
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
        """, engine, account_id)
        logger.info(f"[credit_transactions_orders] {num} stale records found")
        if not update or num == 0:
            return _result(num, 0, True)
        _execute("""
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
        """, engine, account_id, "update credit fields + remaining_credits_cache")
        return _result(num, num, True)
    except Exception as e:
        logger.error(f"[credit_transactions_orders] failed: {e}\n{traceback.format_exc()}")
        return _result(0, 0, False, str(e))


def update_membership_instances(engine: Engine, account_id: str, update: bool) -> dict:
    logger.info("[membership_instances] checking stale records...")
    try:
        num = _count("""
            SELECT COUNT(*) FROM membership_instances mi
            JOIN public.mt_membership_instances_details_dlk exp
              ON exp.account_id = mi.account_id
             AND exp.membership_instances_id = mi.membership_instances_id
            WHERE mi.account_id = :account_id
              AND exp.updated_at > mi.updated_at
              AND (
                   (mi.next_charge_date IS NULL) <> (exp.next_charge_date IS NULL)
                OR (mi.next_charge_date IS NOT NULL AND exp.next_charge_date IS NOT NULL
                    AND abs(extract(epoch from (mi.next_charge_date - exp.next_charge_date))) >= 0.1)
                OR (mi.purchase_date IS NULL) <> (exp.purchase_date IS NULL)
                OR (mi.purchase_date IS NOT NULL AND exp.purchase_date IS NOT NULL
                    AND abs(extract(epoch from (mi.purchase_date - exp.purchase_date))) >= 0.1)
                OR mi.membership_name IS DISTINCT FROM exp.membership_name
                OR mi.status IS DISTINCT FROM exp.status
              )
        """, engine, account_id)
        logger.info(f"[membership_instances] {num} stale records found")
        if not update or num == 0:
            return _result(num, 0, True)
        _execute("""
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
                OR (mi.next_charge_date IS NOT NULL AND exp.next_charge_date IS NOT NULL
                    AND abs(extract(epoch from (mi.next_charge_date - exp.next_charge_date))) >= 0.1)
                OR (mi.purchase_date IS NULL) <> (exp.purchase_date IS NULL)
                OR (mi.purchase_date IS NOT NULL AND exp.purchase_date IS NOT NULL
                    AND abs(extract(epoch from (mi.purchase_date - exp.purchase_date))) >= 0.1)
                OR mi.membership_name IS DISTINCT FROM exp.membership_name
                OR mi.status IS DISTINCT FROM exp.status
              )
        """, engine, account_id, "update membership fields + dates")
        return _result(num, num, True)
    except Exception as e:
        logger.error(f"[membership_instances] failed: {e}\n{traceback.format_exc()}")
        return _result(0, 0, False, str(e))


def update_membership_transactions(engine: Engine, account_id: str, update: bool) -> dict:
    logger.info("[membership_transactions] checking stale records...")
    try:
        num = _count("""
            SELECT COUNT(*) FROM membership_transactions mt
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
        """, engine, account_id)
        logger.info(f"[membership_transactions] {num} stale records found")
        if not update or num == 0:
            return _result(num, 0, True)
        with engine.begin() as conn:
            r1 = conn.execute(text("""
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
            """), {"account_id": account_id}).rowcount
            logger.info(f"  update_1 (parent_id + instances_id): {r1} rows")
            r2 = conn.execute(text("""
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
            """), {"account_id": account_id}).rowcount
            logger.info(f"  update_2 (instances_ref_id): {r2} rows")
        return _result(num, num, True)
    except Exception as e:
        logger.error(f"[membership_transactions] failed: {e}\n{traceback.format_exc()}")
        return _result(0, 0, False, str(e))


def update_membership_transactions_orders(engine: Engine, account_id: str, update: bool) -> dict:
    logger.info("[membership_transactions_orders] checking stale records...")
    try:
        num = _count("""
            SELECT COUNT(*) FROM membership_transactions_orders mto
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
        """, engine, account_id)
        logger.info(f"[membership_transactions_orders] {num} stale records found")
        if not update or num == 0:
            return _result(num, 0, True)
        with engine.begin() as conn:
            r1 = conn.execute(text("""
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
            """), {"account_id": account_id}).rowcount
            logger.info(f"  update_1 (parent_id + instances_id + payment_end_date): {r1} rows")
            r2 = conn.execute(text("""
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
            """), {"account_id": account_id}).rowcount
            logger.info(f"  update_2 (instances_ref_id): {r2} rows")
        return _result(num, num, True)
    except Exception as e:
        logger.error(f"[membership_transactions_orders] failed: {e}\n{traceback.format_exc()}")
        return _result(0, 0, False, str(e))


def update_order_lines(engine: Engine, account_id: str, update: bool) -> dict:
    logger.info("[order_lines] checking stale records...")
    try:
        num = _count("""
            SELECT COUNT(*) FROM order_lines ol
            JOIN public.mt_order_lines_details_dlk exp
              ON exp.account_id = ol.account_id
             AND exp.order_line_id = ol.order_line_id
            WHERE ol.account_id = :account_id
              AND exp.updated_at > ol.updated_at
              AND (
                   ol.title IS DISTINCT FROM exp.title
                OR ol.transaction_type IS DISTINCT FROM exp.transaction_type
              )
        """, engine, account_id)
        logger.info(f"[order_lines] {num} stale records found")
        if not update or num == 0:
            return _result(num, 0, True)
        _execute("""
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
        """, engine, account_id, "update title + transaction_type")
        return _result(num, num, True)
    except Exception as e:
        logger.error(f"[order_lines] failed: {e}\n{traceback.format_exc()}")
        return _result(0, 0, False, str(e))


def delete_invalid_order_lines(engine: Engine, account_id: str, update: bool) -> dict:
    logger.info("[delete_invalid_order_lines] checking invalid order_lines...")
    try:
        num = _count("""
            SELECT COUNT(*) FROM order_lines ol
            JOIN mt_order_lines_details_dlk stg
              ON stg.account_id = ol.account_id
             AND stg.order_line_id = ol.order_line_id
            WHERE ol.account_id = :account_id
              AND stg.is_valid = FALSE
        """, engine, account_id)
        logger.info(f"[delete_invalid_order_lines] {num} invalid order_lines found")
        if not update or num == 0:
            return _result(num, 0, True)
        deleted = _execute("""
            DELETE FROM order_lines ol
            USING mt_order_lines_details_dlk stg
            WHERE stg.account_id = ol.account_id
              AND stg.order_line_id = ol.order_line_id
              AND ol.account_id = :account_id
              AND stg.is_valid = FALSE
        """, engine, account_id, "delete is_valid=FALSE rows")
        return _result(num, deleted, True)
    except Exception as e:
        logger.error(f"[delete_invalid_order_lines] failed: {e}\n{traceback.format_exc()}")
        return _result(0, 0, False, str(e))


def update_reservations(engine: Engine, account_id: str, update: bool) -> dict:
    logger.info("[reservations] checking stale records...")
    try:
        num = _count("""
            SELECT COUNT(*) FROM public.reservations r
            JOIN public.mt_reservations_details_dlk exp
              ON exp.account_id = r.account_id
             AND exp.reservations_id = r.reservations_id
            WHERE r.account_id = :account_id
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
                OR (exp.credit_transactions_id IS NOT NULL AND EXISTS (
                      SELECT 1 FROM public.credit_transactions ct
                      WHERE ct.account_id = r.account_id
                        AND ct.credit_transactions_id = exp.credit_transactions_id
                        AND r.credit_transactions_ref_id IS DISTINCT FROM ct.id
                    ))
                OR (exp.membership_transactions_id IS NOT NULL AND EXISTS (
                      SELECT 1 FROM public.membership_transactions mt
                      WHERE mt.account_id = r.account_id
                        AND mt.membership_transactions_id = exp.membership_transactions_id
                        AND r.membership_transactions_ref_id IS DISTINCT FROM mt.id
                    ))
              )
        """, engine, account_id)
        logger.info(f"[reservations] {num} stale records found")
        if not update or num == 0:
            return _result(num, 0, True)
        with engine.begin() as conn:
            r1 = conn.execute(text("""
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
            """), {"account_id": account_id}).rowcount
            logger.info(f"  update_1 (main fields): {r1} rows")
            r2 = conn.execute(text("""
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
            """), {"account_id": account_id}).rowcount
            logger.info(f"  update_2 (credit_transactions_ref_id): {r2} rows")
            r3 = conn.execute(text("""
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
            """), {"account_id": account_id}).rowcount
            logger.info(f"  update_3 (membership_transactions_ref_id): {r3} rows")
        return _result(num, num, True)
    except Exception as e:
        logger.error(f"[reservations] failed: {e}\n{traceback.format_exc()}")
        return _result(0, 0, False, str(e))


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def update_stale_data(engine: Engine, account_id: str = None, update: bool = False) -> dict:
    """Run stale updates for all tables in order. Stops on first failure."""
    steps = [
        ("class_sessions",                 update_class_sessions),
        ("membership_instances",           update_membership_instances),
        ("orders",                         update_orders),
        ("credit_transactions",            update_credit_transactions),
        ("credit_transactions_orders",     update_credit_transactions_orders),
        ("membership_transactions",        update_membership_transactions),
        ("membership_transactions_orders", update_membership_transactions_orders),
        ("delete_invalid_order_lines",     delete_invalid_order_lines),
        ("order_lines",                    update_order_lines),
        ("reservations",                   update_reservations),
    ]

    results = {
        "success": True, "successful_tables": [], "failed_table": None,
        "remaining_tables": [], "elapsed_time_seconds": 0.0, "tables": {}
    }
    t0 = time.time()
    logger.info(f"Stale {'update' if update else 'check'} starting for account={account_id}")

    for i, (name, fn) in enumerate(steps):
        step_start = time.time()
        result = fn(engine, account_id, update)
        result["elapsed_time_seconds"] = round(time.time() - step_start, 2)
        results["tables"][name] = result
        logger.info(f"[{name}] completed in {result['elapsed_time_seconds']}s — "
                    f"expected={result['expected']}, updated={result['updated']}, success={result['success']}")

        if not result["success"]:
            results["success"] = False
            results["failed_table"] = name
            results["remaining_tables"] = [s[0] for s in steps[i + 1:]]
            logger.error(f"Stopping at [{name}] (strict mode). Remaining: {results['remaining_tables']}")
            break

        results["successful_tables"].append(name)

    results["elapsed_time_seconds"] = round(time.time() - t0, 2)
    logger.info(f"Stale update done: {len(results['successful_tables'])}/{len(steps)} tables "
                f"in {results['elapsed_time_seconds']}s")
    return results
