from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class Customer(BaseModel):
    id: Optional[int] = None
    customer_id: Optional[str] = None
    location_id: Optional[int] = None
    account_id: Optional[int] = None
    first_name: str
    last_name: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    birth_date: Optional[datetime] = None
    birth_day: Optional[int] = None
    birth_month: Optional[int] = None
    phone_number: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    address_line3: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    state_province: Optional[str] = None
    customer_state: Optional[str] = None
    postal_code: Optional[str] = None
    gender: Optional[str] = None
    date_joined: Optional[datetime] = None
    is_opted_in_to_sms: bool = False
    completed_class_count: Optional[int] = 0
    state_id: Optional[int] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True


class Order(BaseModel):
    id: Optional[int] = None
    order_id: Optional[str] = None
    date_placed: Optional[datetime] = None
    location: Optional[str] = None
    location_id: Optional[int] = None
    payment_sources_labels: Optional[str] = None
    status: Optional[str] = None
    order_lines_id: Optional[str] = None
    customer_ref_id: Optional[int] = None
    account_id: Optional[int] = None
    customer_id: Optional[str] = None
    parent_order: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True


class OrderLines(BaseModel):
    id: Optional[int] = None
    order_line_id: Optional[str] = None
    order_id: Optional[str] = None
    order_ref_id: Optional[int] = None
    account_id: Optional[int] = None
    transaction_type: Optional[str] = None
    location: Optional[str] = None
    credit_transactions_id: Optional[int] = None
    credit_transactions_ref_id: Optional[int] = None
    membership_transactions_id: Optional[int] = None
    membership_transactions_ref_id: Optional[int] = None
    title: Optional[str] = None
    line_total: Optional[float] = None
    processed_by: bool = False
    child_orders: List[str] = Field(default_factory=list)
    is_valid: Optional[bool] = True
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True


class ClassSessions(BaseModel):
    id: Optional[int] = None
    class_session_id: Optional[str] = None
    start_datetime: Optional[datetime] = None
    start_date: Optional[str] = None
    location: Optional[str] = None
    end_datetime: Optional[datetime] = None
    cancellation_datetime: Optional[datetime] = None
    class_name: Optional[str] = None
    class_type_name: Optional[str] = None
    capacity: Optional[int] = None
    account_id: Optional[int] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True


class Reservation(BaseModel):
    id: Optional[int] = None
    reservations_id: Optional[int] = None
    cancel_date: Optional[datetime] = None
    check_in_date: Optional[datetime] = None
    creation_date: Optional[datetime] = None
    status: Optional[str] = None
    credit_transactions_type: Optional[str] = None
    credit_transactions_id: Optional[int] = None
    credit_transactions_ref_id: Optional[int] = None
    membership_transactions_type: Optional[str] = None
    membership_transactions_id: Optional[int] = None
    membership_transactions_ref_id: Optional[int] = None
    guest: bool = False
    customer_id: Optional[str] = None
    customer_ref_id: Optional[int] = None
    class_session_id: Optional[str] = None
    class_session_ref_id: Optional[int] = None
    account_id: Optional[int] = None
    first_timer: bool = False
    reservation_type: Optional[str] = None
    transaction_type: Optional[str] = None
    location: Optional[int] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True


class CreditTransactionOrder(BaseModel):
    id: Optional[int] = None
    credit_transactions_id: Optional[int] = None
    transaction_date: Optional[datetime] = None
    credit_name: Optional[str] = None
    is_expired: Optional[bool] = None
    remaining_credits_cache: Optional[int] = None
    is_intro_offer: Optional[bool] = None
    parent_credit_transaction_type: Optional[str] = None
    parent_credit_transaction_id: Optional[int] = None
    customer_id: Optional[str] = None
    customer_ref_id: Optional[int] = None
    location: Optional[int] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    account_id: Optional[int] = None

    class Config:
        from_attributes = True


class MembershipInstance(BaseModel):
    id: Optional[int] = None
    membership_instances_id: Optional[int] = None
    purchase_date: Optional[datetime] = None
    membership_name: Optional[str] = None
    renewal_rate_incl_tax: Optional[str] = None
    status: Optional[str] = None
    location: Optional[int] = None
    renewal_count: Optional[int] = None
    next_charge_date: Optional[datetime] = None
    account_id: Optional[int] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True


class MembershipTransaction(BaseModel):
    id: Optional[int] = None
    membership_transactions_id: Optional[int] = None
    transaction_date: Optional[datetime] = None
    membership_name: Optional[str] = None
    parent_membership_transaction_id: Optional[int] = None
    membership_instances_id: Optional[int] = None
    membership_instances_ref_id: Optional[int] = None
    customer_ref_id: Optional[int] = None
    customer_id: Optional[str] = None
    account_id: Optional[int] = None
    location: Optional[int] = None
    payment_interval_end_date: Optional[datetime] = None
    next_charge_date: Optional[datetime] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True
