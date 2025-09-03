from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Float, ForeignKey,
    UniqueConstraint, Index, JSON, ARRAY, Enum as SAEnum
)

from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from models.enums import Entity, Status, Personality, StateStatus, StateHistoryType
from models.base import Base  # your declarative_base()

# ✅ Customers Model
class Customers(Base):
    __tablename__ = "customers_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(255))
    location_id = Column(Integer, nullable=False)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)
    first_name = Column(String(64), nullable=False)
    last_name = Column(String(64), nullable=False)
    email = Column(String(255))
    full_name = Column(String(255))
    birth_date = Column(DateTime)
    phone_number = Column(String(64))
    address_line1 = Column(String)
    address_line2 = Column(String)
    address_line3 = Column(String)
    city = Column(String(255))
    country = Column(String(255))
    state_province = Column(String(255))
    customer_state = Column(String(64))
    postal_code = Column(String(64))
    gender = Column(String(64))
    date_joined = Column(DateTime, server_default=func.now())
    is_opted_in_to_sms = Column(Boolean, server_default="false")
    completed_class_count = Column(Integer, server_default="0")
    state_id = Column(Integer, ForeignKey("states_rmtest.id"))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)
    emailUnsubscribeHash = Column(String)
    isSubscribedToEmail = Column(Boolean, server_default="true")
    is_prospect = Column(Boolean)
    is_company = Column(Boolean)
    first_appointment_date = Column(DateTime)
    first_class_date = Column(DateTime)

    # ---- Relationships ----
    account = relationship("Accounts", back_populates="customers", foreign_keys=[account_id])
    state = relationship("States", back_populates="customers", foreign_keys=[state_id])

    credit_transactions = relationship(
        "CreditTransactions",
        back_populates="customer",
        foreign_keys="CreditTransactions.customer_ref_id"
    )
    credit_transactions_orders = relationship(
        "CreditTransactionsOrders",
        back_populates="customer",
        foreign_keys="CreditTransactionsOrders.customer_ref_id"
    )
    membership_transactions = relationship(
        "MembershipTransactions",
        back_populates="customer",
        foreign_keys="MembershipTransactions.customer_ref_id"
    )
    orders = relationship(
        "Orders",
        back_populates="customer",
        foreign_keys="Orders.customer_ref_id"
    )
    reservations = relationship(
        "Reservations",
        back_populates="customer",
        foreign_keys="Reservations.customer_ref_id"
    )
    messages = relationship(
        "EztextingMessages",
        back_populates="customer",
        foreign_keys="EztextingMessages.customer_ref_id"
    )
    state_history = relationship(
        "StateHistory",
        back_populates="customer",
        foreign_keys="StateHistory.customer_ref_id"
    )

    __table_args__ = (
        Index("ix_customers_account_id", "account_id"),
        Index("ix_customers_city", "city"),
        Index("ix_customers_state_province", "state_province"),
        Index("ix_customers_email", "email"),
        Index("ix_customers_phone_number", "phone_number"),
    )


class Orders(Base):
    __tablename__ = "orders_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String)  # nullable since in Prisma it's String?
    date_placed = Column(DateTime)
    location = Column(String)
    payment_sources_labels = Column(String(255))
    status = Column(String(255))
    order_lines_id = Column(String(64))
    customer_ref_id = Column(Integer, ForeignKey("customers_rmtest.id", ondelete="SET NULL"))
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="SET NULL"))
    customer_id = Column(String(255))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # --- Relationships ---
    orderLines = relationship("OrderLines", back_populates="order")
    orderCustomer = relationship("Customers", back_populates="orders", foreign_keys=[customer_ref_id])
    account = relationship("Accounts", back_populates="orders")
    customer = relationship(
        "Customers",
        back_populates="orders",
        foreign_keys=[customer_ref_id]
    )

    __table_args__ = (
        Index("ix_orders_order_id", "order_id"),
        Index("ix_orders_location", "location"),
        Index("ix_orders_status", "status"),
    )


# ✅ Accounts Model
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, JSON, Enum as SAEnum,
    ForeignKey, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base
from models.enums import Entity, Status, Personality  # Assuming these enums are defined


class Accounts(Base):
    __tablename__ = "accounts_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_name = Column(String(100), nullable=False)
    city = Column(String(60))
    address = Column(String(255))
    entity = Column(SAEnum(Entity), nullable=True)
    crm_integration_id = Column(Integer, ForeignKey("crm_integrations_rmtest.id"), nullable=False, server_default="1")
    status = Column(SAEnum(Status), nullable=False, server_default="ONBOARDING")
    on_boarding_step = Column(Integer, nullable=False, server_default="1")
    site_url = Column(String(255))
    integration_id = Column(String(60))
    integration_name = Column(String(60))
    crm_config = Column(JSON)
    ez_texting_id = Column(Integer, ForeignKey("eztexting_account_rmtest.id", ondelete="CASCADE"), nullable=True)
    is_syncing = Column(Boolean, server_default="false")
    last_sync_time = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    callback_url = Column(String(255))
    callback_secret = Column(String(255))
    personality = Column(SAEnum(Personality))
    crm_api_end_point = Column(String)
    password = Column(JSON)
    username = Column(String)

    # -------------------
    # Relationships
    # -------------------
    eztexting_account = relationship("EztextingAccount", back_populates="accounts", foreign_keys=[ez_texting_id], uselist=False)
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="accounts_created", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="accounts_updated", foreign_keys=[updated_by])
    deleted_by_user = relationship("models.orm_alchemy_models.Users", back_populates="accounts_deleted", foreign_keys=[deleted_by])
    integration = relationship("CrmIntegrations", back_populates="accounts")

    accounts_users_mapping = relationship("AccountsUsersMapping", back_populates="account")
    contact_groups = relationship("ContactGroup", back_populates="account")
    contacts = relationship("Contact", back_populates="account")
    states = relationship("States", back_populates="account")
    batch_processing_state = relationship("BatchProcessingState", back_populates="account")
    eztexting_messages = relationship("EztextingMessages", back_populates="account")
    customers = relationship("Customers", back_populates="account")
    membership_transactions_orders = relationship(
        "MembershipTransactionsOrders",
        back_populates="account"
    )

    atmosphere_options_accounts_mapping = relationship("AtmosphereOptionsAccountsMapping", back_populates="account")
    key_selling_points_accounts_mapping = relationship("KeySellingPointsAccountsMapping", back_populates="account")
    self_realizations_accounts_mapping = relationship("SelfRealizationsAccountsMapping", back_populates="account")
    target_membership_demographic_mappings = relationship("TargetMembershipDemographicMapping", back_populates="account")
    previous_campaigns = relationship("PreviousCampaigns", back_populates="account")
    areas_to_improve = relationship("AreasToImprove", back_populates="account")
    unique_phrases = relationship("UniquePhrases", back_populates="account")
    tone_of_communication_mapping = relationship("ToneOfCommunicationMapping", back_populates="account")
    introductory_offer = relationship("IntroductoryOffer", back_populates="account")
    incentive_discount = relationship("IncentiveDiscount", back_populates="account")
    offer_and_service = relationship("OfferAndService", back_populates="account")
    state_messages = relationship("StateMessages", back_populates="account")

    credit_transactions = relationship("CreditTransactions", back_populates="account")
    credit_transactions_orders = relationship(
        "CreditTransactionsOrders",
        back_populates="account"
    )

    membership_instances = relationship(
        "MembershipInstances",
        back_populates="account"
    )

    account_memberships = relationship("MembershipAccounts", back_populates="account")
    account_class_packs = relationship("ClassPackAccounts", back_populates="account")
    broadcasts = relationship("Broadcasts", back_populates="account")
    eztexting_message_verification = relationship("EztextingMessageVerification", back_populates="account")
    account_faqs = relationship("AccountFaqs", back_populates="account")
    notifications = relationship("Notifications", back_populates="account")
    email_configurations = relationship("EmailConfigurations", back_populates="account", uselist=False)
    orders = relationship("Orders", back_populates="account")

    __table_args__ = (
        Index("ix_accounts_created_by", "created_by"),
        Index("ix_accounts_updated_by", "updated_by"),
        Index("ix_accounts_deleted_by", "deleted_by"),
        Index("ix_accounts_ez_texting_id", "ez_texting_id"),
    )



from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Enum as SAEnum,
    ForeignKey, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from models.base import Base
from models.enums import StateStatus  # assumes Python Enum matches Prisma enum


class States(Base):
    __tablename__ = "states_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    crm_id = Column(Integer, nullable=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=True)
    contact_group_id = Column(Integer, ForeignKey("contact_group_rmtest.id", ondelete="NO ACTION"), nullable=True)
    priority = Column(Integer, nullable=False)
    state_name = Column(String(100), nullable=False)
    description = Column(String(255), server_default="", nullable=False)
    is_predefined = Column(Boolean, server_default="false", nullable=False)
    enabled_email = Column(Boolean, server_default="false", nullable=False)
    enabled_sms = Column(Boolean, server_default="true", nullable=False)
    status = Column(SAEnum(StateStatus), server_default="WAITING_FOR_APPROVAL", nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    deleted_at = Column(DateTime, nullable=True)
    deleted_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="states")
    contact_group = relationship("ContactGroup", back_populates="states")

    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="states_created", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="states_updated", foreign_keys=[updated_by])
    deleted_by_user = relationship("models.orm_alchemy_models.Users", back_populates="states_deleted", foreign_keys=[deleted_by])

    state_transitions_from = relationship(
        "StateTransitions",
        back_populates="state_from",
        foreign_keys="StateTransitions.state_id"
    )
    state_transitions_to = relationship(
        "StateTransitions",
        back_populates="state_to",
        foreign_keys="StateTransitions.next_state_id"
    )

    customers = relationship("Customers", back_populates="state")
    state_messages = relationship("StateMessages", back_populates="state")
    state_history = relationship("StateHistory", back_populates="state")
    broadcast_recipients = relationship("BroadcastRecipients", back_populates="state")

    __table_args__ = (
        Index("ix_states_created_by", "created_by"),
        Index("ix_states_updated_by", "updated_by"),
        Index("ix_states_deleted_by", "deleted_by"),
    )



from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class StateTransitions(Base):
    __tablename__ = "state_transitions_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_id = Column(Integer, ForeignKey("states_rmtest.id", ondelete="CASCADE"), nullable=False)
    next_state_id = Column(Integer, ForeignKey("states_rmtest.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Relationships back to States
    state_from = relationship(
        "States",
        back_populates="state_transitions_from",
        foreign_keys=[state_id]
    )
    state_to = relationship(
        "States",
        back_populates="state_transitions_to",
        foreign_keys=[next_state_id]
    )


    __table_args__ = (
        Index("ix_state_transitions_state_id_next_state_id", "state_id", "next_state_id"),
    )


class CreditTransactions(Base):
    __tablename__ = "credit_transactions_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    credit_transactions_id = Column(Integer)
    transaction_date = Column(DateTime)
    credit_name = Column(String(128))
    is_expired = Column(Boolean)
    is_intro_offer = Column(Boolean)
    parent_credit_transaction_type = Column(String(64))
    parent_credit_transaction_id = Column(Integer)
    customer_ref_id = Column(Integer, ForeignKey("customers_rmtest.id", ondelete="SET NULL"))
    customer_id = Column(String(255))
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="SET NULL"))
    location = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # Relationships
    # To Reservations (one-to-one per Prisma relation "CreditTransaction")
    creditTransactionReservations = relationship(
        "Reservations",
        back_populates="creditTransaction",
        uselist=False,
        foreign_keys="Reservations.credit_transactions_ref_id"
    )
    # To Customers
    creditTransactionsCustomer = relationship(
        "Customers",
        back_populates="credit_transactions",
        foreign_keys=[customer_ref_id]
    )

    credit_transactions_orders = relationship(
            "CreditTransactionsOrders",
            back_populates="credit_transaction",
            cascade="all, delete-orphan"
        )


    customer = relationship(
    "Customers",
    back_populates="credit_transactions",
    foreign_keys=[customer_ref_id]
)
    # To Accounts
    account = relationship("Accounts", back_populates="credit_transactions")

    __table_args__ = (
        Index("ix_credit_transactions_credit_transactions_id", "credit_transactions_id"),
        Index("ix_credit_transactions_location", "location"),
    )


from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, ForeignKey, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class CreditTransactionsOrders(Base):
    __tablename__ = "credit_transactions_orders_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    credit_transactions_id = Column(Integer, ForeignKey("credit_transactions_rmtest.id", ondelete="CASCADE"), nullable=True)  # FK to CreditTransactions
    transaction_date = Column(DateTime)
    credit_name = Column(String(128))
    is_expired = Column(Boolean)
    is_intro_offer = Column(Boolean)
    parent_credit_transaction_type = Column(String(64))
    parent_credit_transaction_id = Column(Integer)
    remaining_credits_cache = Column(Integer)
    customer_ref_id = Column(Integer, ForeignKey("customers_rmtest.id", ondelete="SET NULL"))
    customer_id = Column(String(255))
    location = Column(Integer)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="SET NULL"))
    order_line_id = Column(Integer, ForeignKey("order_lines_rmtest.id", ondelete="CASCADE"), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # --- Relationships ---
    account = relationship(
        "Accounts",
        back_populates="credit_transactions_orders"
    )

    customer = relationship(
        "Customers",
        back_populates="credit_transactions_orders",
        foreign_keys=[customer_ref_id]
    )

    credit_transaction = relationship(
        "CreditTransactions",
        back_populates="credit_transactions_orders",
        foreign_keys=[credit_transactions_id]
    )

    order_line = relationship(
        "OrderLines",
        back_populates="credit_transaction_order",
        foreign_keys=[order_line_id],
        uselist=False
    )

    __table_args__ = (
        Index("ix_credit_transactions_orders_credit_transactions_id", "credit_transactions_id"),
        Index("ix_credit_transactions_orders_location", "location"),
    )




from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class MembershipTransactionsOrders(Base):
    __tablename__ = "membership_transactions_orders_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    membership_transactions_id = Column(Integer)
    transaction_date = Column(DateTime)
    membership_name = Column(String(128))
    parent_membership_transaction_id = Column(Integer)
    membership_instances_id = Column(Integer)
    membership_instances_ref_id = Column(
        Integer,
        ForeignKey("membership_instances_rmtest.id", ondelete="SET NULL")
    )
    payment_interval_end_date = Column(DateTime)
    customer_id = Column(String(255))
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="SET NULL"))
    location = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    orderLineId = Column(Integer, ForeignKey("order_lines_rmtest.id", ondelete="CASCADE"), nullable=True)

    # -------------------- Relationships --------------------
    membershipInstance = relationship(
        "MembershipInstances",
        back_populates="membershipTransactionsOrders",
        foreign_keys=[membership_instances_ref_id]
    )
    membershipTransactionOrders = relationship(
        "OrderLines",
        back_populates="membershipTransactionOrders",
        uselist=False,
        foreign_keys=[orderLineId]
    )
    account = relationship(
        "Accounts",
        back_populates="membership_transactions_orders"
    )

    __table_args__ = (
        Index("ix_membership_transactions_orders_membership_transactions_id", "membership_transactions_id"),
        Index("ix_membership_transactions_orders_location", "location"),
    )


class MembershipInstances(Base):
    __tablename__ = "membership_instances_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    membership_instances_id = Column(Integer)
    purchase_date = Column(DateTime)
    membership_name = Column(String(128))
    renewal_rate_incl_tax = Column(String(16))
    status = Column(String(128))
    location = Column(Integer)
    renewal_count = Column(Integer)
    next_charge_date = Column(DateTime)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="SET NULL"))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)
    customer_ref_id = Column(Integer, ForeignKey("customers_rmtest.id", ondelete="SET NULL"))

    # -------------------- Relationships --------------------
    membershipTransactions = relationship(
        "MembershipTransactions",
        back_populates="membershipInstance",
        foreign_keys="MembershipTransactions.membership_instances_ref_id"
    )
    membershipTransactionsOrders = relationship(
        "MembershipTransactionsOrders",
        back_populates="membershipInstance",
        foreign_keys="MembershipTransactionsOrders.membership_instances_ref_id"
    )
    account = relationship(
        "Accounts",
        back_populates="membership_instances"
    )

    customer = relationship(
        "Customers",
        back_populates="membership_transactions",
        foreign_keys=[customer_ref_id]
    )

    __table_args__ = (
        Index("ix_membership_instances_membership_instances_id", "membership_instances_id"),
        Index("ix_membership_instances_location", "location"),
    )


class MembershipTransactions(Base):
    __tablename__ = "membership_transactions_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    membership_transactions_id = Column(Integer)
    transaction_date = Column(DateTime)
    membership_name = Column(String(128))
    parent_membership_transaction_id = Column(Integer)
    membership_instances_id = Column(Integer)
    membership_instances_ref_id = Column(
        Integer,
        ForeignKey("membership_instances_rmtest.id", ondelete="SET NULL")
    )
    customer_ref_id = Column(
        Integer,
        ForeignKey("customers_rmtest.id", ondelete="SET NULL")
    )
    customer_id = Column(String(255))
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="SET NULL"))
    location = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # -------------------- Relationships --------------------
    creditTransactionsCustomer = relationship(
        "Customers",
        back_populates="membership_transactions",
        foreign_keys=[customer_ref_id]
    )
    membershipTransactionReservations = relationship(
        "Reservations",
        back_populates="membershipTransaction",
        uselist=False,
        foreign_keys="Reservations.membership_transactions_ref_id"
    )
    membershipInstance = relationship(
        "MembershipInstances",
        back_populates="membershipTransactions",
        foreign_keys=[membership_instances_ref_id]
    )
    account = relationship(
        "Accounts",
        back_populates="membership_transactions"
    )

    customer = relationship(
        "Customers",
        back_populates="membership_transactions",
        foreign_keys=[customer_ref_id]
    )

    __table_args__ = (
        Index("ix_membership_transactions_membership_transactions_id", "membership_transactions_id"),
        Index("ix_membership_transactions_location", "location"),
    )


from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base





class OrderLines(Base):
    __tablename__ = "order_lines_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_line_id = Column(String(64))
    order_id = Column(String)
    order_ref_id = Column(Integer, ForeignKey("orders_rmtest.id", ondelete="SET NULL"))
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="SET NULL"))
    transaction_type = Column(String(255))
    location = Column(String(255))
    credit_transactions_id = Column(Integer)
    credit_transactions_ref_id = Column(Integer, ForeignKey("credit_transactions_rmtest.id", ondelete="SET NULL"), unique=True)
    membership_transactions_id = Column(Integer)
    membership_transactions_ref_id = Column(Integer, ForeignKey("membership_transactions_rmtest.id", ondelete="SET NULL"), unique=True)
    title = Column(String(255))
    processed_by = Column(Boolean, server_default="false")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    credit_transactions_orders_id = Column(
        Integer,
        ForeignKey("credit_transactions_orders_rmtest.id", ondelete="CASCADE"),
        nullable=True
    )

    # --- Relationships ---
    order = relationship("Orders", back_populates="orderLines", foreign_keys=[order_ref_id])
    credit_transaction_order = relationship("CreditTransactionsOrders", back_populates="order_line",foreign_keys=[credit_transactions_orders_id])
    membershipTransactionOrders = relationship("MembershipTransactionsOrders", back_populates="membershipTransactionOrders", foreign_keys=[membership_transactions_ref_id])

    __table_args__ = (
        Index("ix_order_lines_order_id", "order_id"),
        Index("ix_order_lines_location", "location"),
    )

   


class Reservations(Base):
    __tablename__ = "reservations_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    reservations_id = Column(Integer)
    cancel_date = Column(DateTime)
    check_in_date = Column(DateTime)
    creation_date = Column(DateTime)
    status = Column(String(16))
    credit_transactions_type = Column(String(64))
    credit_transactions_id = Column(Integer)
    credit_transactions_ref_id = Column(Integer, ForeignKey("credit_transactions_rmtest.id", ondelete="SET NULL"), unique=True)
    membership_transactions_type = Column(String(64))
    membership_transactions_id = Column(Integer)
    membership_transactions_ref_id = Column(Integer, ForeignKey("membership_transactions_rmtest.id", ondelete="SET NULL"), unique=True)
    guest = Column(Boolean, server_default="false")
    customer_id = Column(String(255))
    customer_ref_id = Column(Integer, ForeignKey("customers_rmtest.id", ondelete="SET NULL"))
    class_session_id = Column(String(255))
    class_session_ref_id = Column(Integer, ForeignKey("class_sessions_rmtest.id", ondelete="SET NULL"))
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="SET NULL"))
    first_timer = Column(Boolean, server_default="false")
    reservation_type = Column(String(64))
    location = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # --- Relationships ---
    creditTransaction = relationship("CreditTransactions", back_populates="creditTransactionReservations", foreign_keys=[credit_transactions_ref_id])
    membershipTransaction = relationship("MembershipTransactions", back_populates="membershipTransactionReservations", foreign_keys=[membership_transactions_ref_id])
    reservationCustomer = relationship("Customers", back_populates="reservations", foreign_keys=[customer_ref_id])
    classSession = relationship("ClassSessions", back_populates="reservations", foreign_keys=[class_session_ref_id])
    account = relationship("Accounts", back_populates="reservations")
    customer = relationship(
        "Customers",
        back_populates="reservations",
        foreign_keys=[customer_ref_id]
    )
    __table_args__ = (
        Index("ix_reservations_reservations_id", "reservations_id"),
        Index("ix_reservations_location", "location"),
    )


from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Index, JSON
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class EztextingMessages(Base):
    __tablename__ = "eztexting_messages_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(String(255), nullable=False, unique=True)
    user_number = Column(String(15), nullable=False)
    contact_number = Column(String(15), nullable=False)
    inbound = Column(Boolean, nullable=False)
    message = Column(String(1600), nullable=False)
    sent_at = Column(DateTime, nullable=False)
    opt_in = Column(Boolean, nullable=False)
    opt_out = Column(Boolean, nullable=False)
    status = Column(String(10), nullable=False)
    type = Column(String(3), nullable=False)
    is_group_message = Column(Boolean, nullable=False)

    # Escalation fields
    is_escalated = Column(Boolean, server_default="false")
    is_answered_by_ai = Column(Boolean, server_default="false")
    escalated_answered_for = Column(ARRAY(Integer), server_default="{}", nullable=False)  # Int[] equivalent in SQLAlchemy
    escalated_answered_at = Column(DateTime)

    message_group_id = Column(String(255))
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)
    customer_ref_id = Column(Integer, ForeignKey("customers_rmtest.id", ondelete="SET NULL"))
    customer_id = Column(String(255))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="eztexting_messages")
    customer = relationship("Customers", back_populates="messages", foreign_keys=[customer_ref_id])

    __table_args__ = (
        Index("ix_ezm_account_id", "account_id"),
        Index("ix_ezm_customer_id", "customer_id"),
        Index("ix_ezm_contact_number", "contact_number"),
        Index("ix_ezm_status", "status"),
        Index("ix_ezm_sent_at", "sent_at"),
    )


class StateHistory(Base):
    __tablename__ = "state_history_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_id = Column(Integer, ForeignKey("states_rmtest.id", ondelete="CASCADE"))
    contact_group_id = Column(Integer)
    customer_id = Column(String(255))
    customer_ref_id = Column(Integer, ForeignKey("customers_rmtest.id", ondelete="SET NULL"))
    history_type = Column(
        SAEnum(StateHistoryType, name="statehistorytype"),  # Postgres native enum
        nullable=False
    )
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    # --- Relationships ---
    state = relationship("States", back_populates="state_history")
    customer = relationship("Customers", back_populates="stateHistory", foreign_keys=[customer_ref_id])



class EztextingAccount(Base):
    __tablename__ = "eztexting_account_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), unique=True, nullable=False)
    password = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # One-to-Many relationship: an eztexting_account can link to multiple Accounts
    accounts = relationship("Accounts", back_populates="eztexting_account")

    # Indexes for quick lookups if needed
    __table_args__ = (
        Index("ix_eztexting_account_username", "username"),
    )


from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class CrmIntegrations(Base):
    __tablename__ = "crm_integrations_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    status = Column(Boolean, server_default="true", nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users_rmtest.id"))

    accounts = relationship(
        "Accounts",
        back_populates="integration"
    )

    createdBy = relationship("models.orm_alchemy_models.Users", foreign_keys=[created_by])
    updatedBy = relationship("models.orm_alchemy_models.Users", foreign_keys=[updated_by])
    deletedBy = relationship("models.orm_alchemy_models.Users", foreign_keys=[deleted_by])

    __table_args__ = (
        Index("ix_crm_integrations_created_by", "created_by"),
        Index("ix_crm_integrations_updated_by", "updated_by"),
        Index("ix_crm_integrations_deleted_by", "deleted_by"),
    )

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Index, JSON
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base
from models.enums import Role  # Assuming Role enum matches Prisma's Role

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, JSON, Enum as SAEnum, ForeignKey,
    UniqueConstraint, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from models.base import Base
from models.enums import Role  # Assuming your enums.Role matches Prisma Role

class Users(Base):
    __tablename__ = "users_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False)
    name = Column(String(100))
    role = Column(SAEnum(Role), nullable=False)
    email_verified = Column(Boolean, server_default="false", nullable=False)
    auth0_id = Column(String(60), unique=True, nullable=False)
    user_metadata = Column(JSON)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    deleted_at = Column(DateTime, nullable=True)
    deleted_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # -------------------
    # Self-referencing relations
    # -------------------
    created_by_user = relationship(
        "models.orm_alchemy_models.Users",
        remote_side=[id],
        back_populates="created_users",
        foreign_keys=[created_by]
    )
    created_users = relationship(
        "models.orm_alchemy_models.Users",
        back_populates="created_by_user",
        foreign_keys=[created_by]
    )

    updated_by_user = relationship(
        "models.orm_alchemy_models.Users",
        remote_side=[id],
        back_populates="updated_users",
        foreign_keys=[updated_by]
    )
    updated_users = relationship(
        "models.orm_alchemy_models.Users",
        back_populates="updated_by_user",
        foreign_keys=[updated_by]
    )

    deleted_by_user = relationship(
        "models.orm_alchemy_models.Users",
        remote_side=[id],
        back_populates="deleted_users",
        foreign_keys=[deleted_by]
    )
    deleted_users = relationship(
        "models.orm_alchemy_models.Users",
        back_populates="deleted_by_user",
        foreign_keys=[deleted_by]
    )

    # -------------------
    # Accounts (Created/Updated/Deleted)
    # -------------------
    accounts_created = relationship("Accounts", back_populates="created_by_user", foreign_keys="Accounts.created_by")
    accounts_updated = relationship("Accounts", back_populates="updated_by_user", foreign_keys="Accounts.updated_by")
    accounts_deleted = relationship("Accounts", back_populates="deleted_by_user", foreign_keys="Accounts.deleted_by")

    # -------------------
    # AccountsUsersMapping
    # -------------------
    accounts_users_mapping = relationship("AccountsUsersMapping", back_populates="user", foreign_keys="AccountsUsersMapping.user_id")
    created_accounts_users_map = relationship("AccountsUsersMapping", back_populates="createdBy", foreign_keys="AccountsUsersMapping.created_by")

    # -------------------
    # States
    # -------------------
    states_created = relationship("States", back_populates="created_by_user", foreign_keys="States.created_by")
    states_updated = relationship("States", back_populates="updated_by_user", foreign_keys="States.updated_by")
    states_deleted = relationship("States", back_populates="deleted_by_user", foreign_keys="States.deleted_by")

    # -------------------
    # Payment setups
    # -------------------
    payment_setups = relationship("PaymentSetup", back_populates="user", foreign_keys="PaymentSetup.user_id")

    # -------------------
    # Subscription plans
    # -------------------
    subscription_plans_created = relationship("SubscriptionPlan", back_populates="created_by_user", foreign_keys="SubscriptionPlan.created_by")
    subscription_plans_updated = relationship("SubscriptionPlan", back_populates="updated_by_user", foreign_keys="SubscriptionPlan.updated_by")
    subscription_plans_deleted = relationship("SubscriptionPlan", back_populates="deleted_by_user", foreign_keys="SubscriptionPlan.deleted_by")

    # -------------------
    # Self realizations
    # -------------------
    created_self_realizations = relationship("SelfRealizations", back_populates="created_by_user", foreign_keys="SelfRealizations.created_by")
    updated_self_realizations = relationship("SelfRealizations", back_populates="updated_by_user", foreign_keys="SelfRealizations.updated_by")

    # -------------------
    # Self realizations accounts mapping
    # -------------------
    created_self_realization_mappings = relationship("SelfRealizationsAccountsMapping", back_populates="created_by_user", foreign_keys="SelfRealizationsAccountsMapping.created_by")
    updated_self_realization_mappings = relationship("SelfRealizationsAccountsMapping", back_populates="updated_by_user", foreign_keys="SelfRealizationsAccountsMapping.updated_by")

    # -------------------
    # Target membership demographic
    # -------------------
    created_target_membership_demographics = relationship("TargetMembershipDemographic", back_populates="created_by_user", foreign_keys="TargetMembershipDemographic.created_by")
    updated_target_membership_demographics = relationship("TargetMembershipDemographic", back_populates="updated_by_user", foreign_keys="TargetMembershipDemographic.updated_by")

    # -------------------
    # Target membership demographic mapping
    # -------------------
    created_target_membership_demographic_mappings = relationship("TargetMembershipDemographicMapping", back_populates="created_by_user", foreign_keys="TargetMembershipDemographicMapping.created_by")
    updated_target_membership_demographic_mappings = relationship("TargetMembershipDemographicMapping", back_populates="updated_by_user", foreign_keys="TargetMembershipDemographicMapping.updated_by")

    # -------------------
    # Previous campaigns
    # -------------------
    created_previous_campaigns = relationship("PreviousCampaigns", back_populates="created_by_user", foreign_keys="PreviousCampaigns.created_by")
    updated_previous_campaigns = relationship("PreviousCampaigns", back_populates="updated_by_user", foreign_keys="PreviousCampaigns.updated_by")

    # -------------------
    # Areas to improve
    # -------------------
    created_areas_to_improve = relationship("AreasToImprove", back_populates="created_by_user", foreign_keys="AreasToImprove.created_by")
    updated_areas_to_improve = relationship("AreasToImprove", back_populates="updated_by_user", foreign_keys="AreasToImprove.updated_by")

    # -------------------
    # Unique phrases
    # -------------------
    created_unique_phrases = relationship("UniquePhrases", back_populates="created_by_user", foreign_keys="UniquePhrases.created_by")
    updated_unique_phrases = relationship("UniquePhrases", back_populates="updated_by_user", foreign_keys="UniquePhrases.updated_by")

    # -------------------
    # Tone of communications
    # -------------------
    created_tone_of_communications = relationship("ToneOfCommunication", back_populates="created_by_user", foreign_keys="ToneOfCommunication.created_by")
    updated_tone_of_communications = relationship("ToneOfCommunication", back_populates="updated_by_user", foreign_keys="ToneOfCommunication.updated_by")

    # -------------------
    # Account membership
    # -------------------
    created_account_membership = relationship("MembershipAccounts", back_populates="created_by_user", foreign_keys="MembershipAccounts.created_by")
    updated_account_membership = relationship("MembershipAccounts", back_populates="updated_by_user", foreign_keys="MembershipAccounts.updated_by")
    deleted_account_membership = relationship("MembershipAccounts", back_populates="deleted_by_user", foreign_keys="MembershipAccounts.deleted_by")

    # -------------------
    # Introductory offers
    # -------------------
    created_introductory_offers = relationship("IntroductoryOffer", back_populates="created_by_user", foreign_keys="IntroductoryOffer.created_by")
    updated_introductory_offers = relationship("IntroductoryOffer", back_populates="updated_by_user", foreign_keys="IntroductoryOffer.updated_by")

    # -------------------
    # Incentive discounts
    # -------------------
    created_incentive_discounts = relationship("IncentiveDiscount", back_populates="created_by_user", foreign_keys="IncentiveDiscount.created_by")
    updated_incentive_discounts = relationship("IncentiveDiscount", back_populates="updated_by_user", foreign_keys="IncentiveDiscount.updated_by")

    # -------------------
    # Offers and services
    # -------------------
    created_offers_services = relationship("OfferAndService", back_populates="created_by_user", foreign_keys="OfferAndService.created_by")
    updated_offers_services = relationship("OfferAndService", back_populates="updated_by_user", foreign_keys="OfferAndService.updated_by")

    # -------------------
    # State messages
    # -------------------
    created_state_messages = relationship("StateMessages", back_populates="created_by_user", foreign_keys="StateMessages.created_by")
    updated_state_messages = relationship("StateMessages", back_populates="updated_by_user", foreign_keys="StateMessages.updated_by")

    # -------------------
    # Account class pack
    # -------------------
    created_account_class_pack = relationship("ClassPackAccounts", back_populates="created_by_user", foreign_keys="ClassPackAccounts.created_by")
    updated_account_class_pack = relationship("ClassPackAccounts", back_populates="updated_by_user", foreign_keys="ClassPackAccounts.updated_by")
    deleted_account_class_pack = relationship("ClassPackAccounts", back_populates="deleted_by_user", foreign_keys="ClassPackAccounts.deleted_by")

    # -------------------
    # User subscriptions
    # -------------------
    user_subscriptions = relationship("UserSubscription", back_populates="user", foreign_keys="UserSubscription.user_id")

    # -------------------
    # Broadcasts
    # -------------------
    created_broadcast = relationship("Broadcasts", back_populates="created_by_user", foreign_keys="Broadcasts.created_by")
    updated_broadcast = relationship("Broadcasts", back_populates="updated_by_user", foreign_keys="Broadcasts.updated_by")
    deleted_broadcast = relationship("Broadcasts", back_populates="deleted_by_user", foreign_keys="Broadcasts.deleted_by")

    # -------------------
    # Broadcast recipients
    # -------------------
    created_broadcast_recipients = relationship("BroadcastRecipients", back_populates="created_by_user", foreign_keys="BroadcastRecipients.created_by")
    updated_broadcast_recipients = relationship("BroadcastRecipients", back_populates="updated_by_user", foreign_keys="BroadcastRecipients.updated_by")

    # -------------------
    # Popular topics
    # -------------------
    created_popular_topics = relationship("PopularTopics", back_populates="created_by_user", foreign_keys="PopularTopics.created_by")
    updated_popular_topics = relationship("PopularTopics", back_populates="updated_by_user", foreign_keys="PopularTopics.updated_by")

    # -------------------
    # FAQs
    # -------------------
    created_faqs = relationship("Faqs", back_populates="created_by_user", foreign_keys="Faqs.created_by")
    updated_faqs = relationship("Faqs", back_populates="updated_by_user", foreign_keys="Faqs.updated_by")

    # -------------------
    # Chat threads
    # -------------------
    chat_threads = relationship("ChatThreads", back_populates="user", foreign_keys="ChatThreads.user_id")
    created_chat_threads = relationship("ChatThreads", back_populates="created_by_user", foreign_keys="ChatThreads.created_by")
    updated_chat_threads = relationship("ChatThreads", back_populates="updated_by_user", foreign_keys="ChatThreads.updated_by")

    # -------------------
    # Chat thread participants
    # -------------------
    chat_thread_participants = relationship("ChatThreadParticipants", back_populates="user", foreign_keys="ChatThreadParticipants.user_id")
    created_chat_thread_participants = relationship("ChatThreadParticipants", back_populates="created_by_user", foreign_keys="ChatThreadParticipants.created_by")
    updated_chat_thread_participants = relationship("ChatThreadParticipants", back_populates="updated_by_user", foreign_keys="ChatThreadParticipants.updated_by")

    # -------------------
    # Chat messages
    # -------------------
    sent_messages = relationship("ChatMessages", back_populates="sender", foreign_keys="ChatMessages.sender_id")
    created_chat_messages = relationship("ChatMessages", back_populates="created_by_user", foreign_keys="ChatMessages.created_by")
    deleted_chat_messages = relationship("ChatMessages", back_populates="deleted_by_user", foreign_keys="ChatMessages.deleted_by")
    updated_chat_messages = relationship("ChatMessages", back_populates="updated_by_user", foreign_keys="ChatMessages.updated_by")

    # -------------------
    # Message tags
    # -------------------
    tagged_in_messages = relationship("MessageTags", back_populates="tagged_user", foreign_keys="MessageTags.user_id")

    __table_args__ = (
        UniqueConstraint("email", name="uq_users_email"),
        UniqueConstraint("auth0_id", name="uq_users_auth0"),
    )


from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Enum as SAEnum, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base
from models.enums import ContactStatus  # Assuming you have a Python enum for ACTIVE, INACTIVE, PENDING

class Contact(Base):
    __tablename__ = "contact_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # FKs
    account_id = Column(
        Integer,
        ForeignKey("accounts_rmtest.id", ondelete="CASCADE"),
        nullable=False
    )
    customer_id = Column(
        Integer,
        ForeignKey("customers_rmtest.id"),
        nullable=True
    )

    # Fields
    phone_number = Column(String(16), nullable=False)
    first_name = Column(String(128), nullable=False)
    last_name = Column(String(128), nullable=False)
    email = Column(String(255), nullable=False)
    status = Column(SAEnum(ContactStatus, name="contact_status_enum"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    account = relationship(
        "Accounts",
        back_populates="contacts"
    )
    customer = relationship(
        "Customers",
        back_populates="contacts"
    )

    # Many-to-many with contact_group_rmtest via contact_group_membership_rmtest
    memberships = relationship(
        "ContactGroupMembership",
        back_populates="contact"
    )

    __table_args__ = (
        UniqueConstraint("account_id", "phone_number", name="uq_account_phone"),
    )


from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey,
    Index, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class ContactGroup(Base):
    __tablename__ = "contact_group_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign keys
    account_id = Column(
        Integer,
        ForeignKey("accounts_rmtest.id", ondelete="CASCADE"),
        nullable=False
    )

    eztexting_group_id = Column(String(128), nullable=True)
    name = Column(String(128), nullable=False)
    note = Column(String(255), nullable=True)
    strict_validation = Column(Boolean, server_default="false", nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # --------------------
    # Relationships
    # --------------------
    account = relationship(
        "Accounts",
        back_populates="contact_groups"   # Accounts model must have contact_groups = relationship(...)
    )

    memberships = relationship(
        "ContactGroupMembership",
        back_populates="contact_group",
        cascade="all, delete-orphan"
    )

    states = relationship(
        "States",
        back_populates="contact_group"    # States model must have contact_group = relationship(...)
    )

    # --------------------
    # Table Constraints
    # --------------------
    __table_args__ = (
        Index("ix_contact_group_account_id", "account_id"),
        UniqueConstraint("eztexting_group_id", "account_id", name="uq_eztexting_group_per_account"),
    )


from sqlalchemy import (
    Column, Integer, DateTime, ForeignKey, Enum as SAEnum, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base
from models.enums import MembershipStatus  # ACTIVE, REMOVED — from your enums module


class ContactGroupMembership(Base):
    __tablename__ = "contact_group_membership_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # FKs
    contact_id = Column(
        Integer,
        ForeignKey("contact_rmtest.id", ondelete="CASCADE"),
        nullable=False
    )
    contact_group_id = Column(
        Integer,
        ForeignKey("contact_group_rmtest.id", ondelete="CASCADE"),
        nullable=False
    )

    # Fields
    added_at = Column(DateTime, server_default=func.now(), nullable=False)
    removed_at = Column(DateTime, nullable=True)
    status = Column(SAEnum(MembershipStatus, name="membership_status_enum"), nullable=False)

    # Relationships
    contact_group = relationship(
        "ContactGroup",
        back_populates="memberships"
    )
    contact = relationship(
        "Contact",
        back_populates="memberships"
    )

    __table_args__ = (
        Index("ix_contact_group_membership_contact_id_group_id", "contact_id", "contact_group_id"),
    )


from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from models.base import Base

class ClassSessions(Base):
    __tablename__ = "class_sessions_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    class_session_id = Column(String, nullable=True)
    start_datetime = Column(DateTime, nullable=True)
    start_date = Column(String, nullable=True)
    location = Column(String, nullable=True)
    end_datetime = Column(DateTime, nullable=True)
    cancellation_datetime = Column(DateTime, nullable=True)

    # Foreign key to accounts_rmtest table; nullable because your model has Int?
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    deleted_at = Column(DateTime, nullable=True)
    deleted_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # Relationships
    account = relationship("Accounts", back_populates="class_sessions")
    createdBy = relationship("models.orm_alchemy_models.Users", foreign_keys=[created_by])
    updatedBy = relationship("models.orm_alchemy_models.Users", foreign_keys=[updated_by])
    deletedBy = relationship("models.orm_alchemy_models.Users", foreign_keys=[deleted_by])

    # One-to-many backref from Reservations (assuming Reservations model exists)
    reservations = relationship("Reservations", back_populates="class_session")

    __table_args__ = (
        Index("ix_class_sessions_class_session_id", "class_session_id"),
        Index("ix_class_sessions_location", "location"),
    )

from sqlalchemy import (
    Column, Integer, DateTime, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from models.base import Base


from sqlalchemy import (
    Column, Integer, DateTime, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class AccountsUsersMapping(Base):
    __tablename__ = "accounts_users_mapping_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("users_rmtest.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="accounts_users_mapping")
    user = relationship("models.orm_alchemy_models.Users", back_populates="accounts_users_mapping", foreign_keys=[user_id])
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_accounts_users_map", foreign_keys=[created_by])

    __table_args__ = (
        UniqueConstraint("account_id", "user_id", name="uq_account_user"),
        Index("ix_accounts_users_mapping_account_id", "account_id"),
        Index("ix_accounts_users_mapping_user_id", "user_id"),
    )



from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class PaymentSetup(Base):
    __tablename__ = "payment_setup_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stripe_customer_id = Column(String(255), unique=True, nullable=False)
    stripe_setup_intent_id = Column(String(255), nullable=True)
    stripe_payment_method_id = Column(String(255), nullable=True)

    user_id = Column(Integer, ForeignKey("users_rmtest.id"), unique=True, nullable=False)
    credits_remaining = Column(Integer, server_default="0")
    has_used_trial = Column(Boolean, server_default="false", nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # --- Relationships ---
    user = relationship("models.orm_alchemy_models.Users", back_populates="payment_setups")

    __table_args__ = (
        UniqueConstraint("stripe_customer_id", name="uq_payment_setup_stripe_customer"),
        UniqueConstraint("user_id", name="uq_payment_setup_user"),
    )


from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Float, JSON, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class SubscriptionPlan(Base):
    __tablename__ = "subscription_plan_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_name = Column(String(255), nullable=False)
    stripe_product_id = Column(String(255), nullable=True)
    stripe_price_id = Column(String(255), nullable=True)
    credits = Column(Integer, server_default="0", nullable=False)
    price = Column(Float, server_default="0", nullable=False)
    billing_interval = Column(String(10), server_default="month", nullable=False)
    trial_days = Column(Integer, nullable=True)
    is_active = Column(Boolean, server_default="true", nullable=False)
    deleted_at = Column(DateTime, nullable=True)
    deleted_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    features = Column(JSON, nullable=True)

    # --- Relationships ---
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="subscription_plans_created", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="subscription_plans_updated", foreign_keys=[updated_by])
    deleted_by_user = relationship("models.orm_alchemy_models.Users", back_populates="subscription_plans_deleted", foreign_keys=[deleted_by])

    # current and previous plan relation to UserSubscription
    user_subscriptions = relationship(
        "UserSubscription", back_populates="current_plan", foreign_keys="UserSubscription.current_plan_id"
    )
    user_subscriptions_prev = relationship(
        "UserSubscription", back_populates="previous_plan", foreign_keys="UserSubscription.previous_plan_id"
    )


from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class SelfRealizations(Base):
    __tablename__ = "self_realizations_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    is_custom = Column(Boolean, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    self_realizations_accounts_mapping = relationship("SelfRealizationsAccountsMapping", back_populates="self_realization")

    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_self_realizations", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_self_realizations", foreign_keys=[updated_by])


from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class PreviousCampaigns(Base):
    __tablename__ = "previous_campaigns_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="previous_campaigns")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_previous_campaigns", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_previous_campaigns", foreign_keys=[updated_by])


class AreasToImprove(Base):
    __tablename__ = "areas_to_improve_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="areas_to_improve")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_areas_to_improve", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_areas_to_improve", foreign_keys=[updated_by])


class UniquePhrases(Base):
    __tablename__ = "unique_phrases_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)
    phrase = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="unique_phrases")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_unique_phrases", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_unique_phrases", foreign_keys=[updated_by])


from sqlalchemy import (
    Column, Integer, DateTime, ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base

class ToneOfCommunicationMapping(Base):
    __tablename__ = "tone_of_communication_mapping_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    tone_of_communication_id = Column(Integer, ForeignKey("tone_of_communication_rmtest.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="tone_of_communication_mapping")
    tone_of_communication = relationship("ToneOfCommunication", back_populates="tone_of_communication_mapping")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_tone_of_communication_mapping", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_tone_of_communication_mapping", foreign_keys=[updated_by])

    __table_args__ = (
        UniqueConstraint("account_id", "tone_of_communication_id", name="uq_account_tone"),
        Index("ix_tone_mapping_account_id", "account_id"),
    )

from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class ToneOfCommunication(Base):
    __tablename__ = "tone_of_communication_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(200), nullable=False)
    description = Column(String(200), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    tone_of_communication_mapping = relationship(
        "ToneOfCommunicationMapping",
        back_populates="tone_of_communication"
    )

    created_by_user = relationship(
        "models.orm_alchemy_models.Users",
        back_populates="created_tone_of_communications",
        foreign_keys=[created_by]
    )
    updated_by_user = relationship(
        "models.orm_alchemy_models.Users",
        back_populates="updated_tone_of_communications",
        foreign_keys=[updated_by]
    )


class StateMessages(Base):
    __tablename__ = "state_messages_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_id = Column(Integer, ForeignKey("states_rmtest.id", ondelete="CASCADE"), nullable=False)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)
    message_frequency = Column(String(20), nullable=False)
    scheduled_at = Column(String(20), nullable=True)
    message_body = Column(String, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="state_messages")
    state = relationship("States", back_populates="state_messages")

    created_by_user = relationship(
        "models.orm_alchemy_models.Users",
        back_populates="created_state_messages",
        foreign_keys=[created_by]
    )
    updated_by_user = relationship(
        "models.orm_alchemy_models.Users",
        back_populates="updated_state_messages",
        foreign_keys=[updated_by]
    )


from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base

class IntroductoryOffer(Base):
    __tablename__ = "introductory_offer_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(255), nullable=False)
    is_active = Column(Boolean, server_default="true", nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="introductory_offer")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_introductory_offers", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_introductory_offers", foreign_keys=[updated_by])



from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base

class IncentiveDiscount(Base):
    __tablename__ = "incentive_discount_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    description = Column(String(255), nullable=False)
    percentage = Column(Integer, nullable=False)
    is_active = Column(Boolean, server_default="true", nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="incentive_discount")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_incentive_discounts", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_incentive_discounts", foreign_keys=[updated_by])


from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base

class OfferAndService(Base):
    __tablename__ = "offer_and_service_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(String(255), nullable=True)
    is_active = Column(Boolean, server_default="true", nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="offer_and_service")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_offers_services", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_offers_services", foreign_keys=[updated_by])


class MembershipAccounts(Base):
    __tablename__ = "membership_accounts_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    membership_name = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    deleted_at = Column(DateTime, nullable=True)
    deleted_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="account_memberships")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_account_membership", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_account_membership", foreign_keys=[updated_by])
    deleted_by_user = relationship("models.orm_alchemy_models.Users", back_populates="deleted_account_membership", foreign_keys=[deleted_by])


from sqlalchemy import (
    Column, Integer, String, Float, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class ClassPackAccounts(Base):
    __tablename__ = "class_pack_accounts_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    plan_name = Column(String(255), nullable=False)
    pricing = Column(Float, nullable=False)
    no_credits = Column(String, nullable=False)
    credit_expiration = Column(Integer, nullable=False)
    description = Column(String, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    deleted_at = Column(DateTime, nullable=True)
    deleted_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="account_class_packs")

    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_account_class_pack", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_account_class_pack", foreign_keys=[updated_by])
    deleted_by_user = relationship("models.orm_alchemy_models.Users", back_populates="deleted_account_class_pack", foreign_keys=[deleted_by])



from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Enum as SAEnum
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base
from models.enums import SubscriptionStatusDb  # Assuming this matches Prisma enum


class UserSubscription(Base):
    __tablename__ = "user_subscription_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users_rmtest.id", ondelete="CASCADE"), nullable=False)
    plan_id = Column(Integer, ForeignKey("subscription_plan_rmtest.id"), nullable=False)
    stripe_subscription_id = Column(String(255), nullable=True)
    status = Column(SAEnum(SubscriptionStatusDb), server_default="ACTIVE", nullable=False)
    started_at = Column(DateTime, server_default=func.now(), nullable=False)
    ended_at = Column(DateTime, nullable=True)
    reason = Column(String(100), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    previous_plan_id = Column(Integer, ForeignKey("subscription_plan_rmtest.id"), nullable=True)

    # --- Relationships ---
    # Link back to the owning user
    user = relationship("models.orm_alchemy_models.Users", back_populates="user_subscriptions", foreign_keys=[user_id])

    # Current plan relation
    plan = relationship(
        "SubscriptionPlan",
        back_populates="user_subscriptions",
        foreign_keys=[plan_id]
    )

    # Previous plan relation
    previous_plan = relationship(
        "SubscriptionPlan",
        back_populates="user_subscriptions_prev",
        foreign_keys=[previous_plan_id]
    )



from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Enum as SAEnum
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base
from models.enums import ScheduleType  # Assuming your enums.ScheduleType matches Prisma enum


class Broadcasts(Base):
    __tablename__ = "broadcasts_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    schedule_type = Column(SAEnum(ScheduleType), nullable=False)
    sent_at = Column(DateTime, nullable=True)
    message = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    deleted_at = Column(DateTime, nullable=True)
    deleted_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="broadcasts")

    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_broadcast", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_broadcast", foreign_keys=[updated_by])
    deleted_by_user = relationship("models.orm_alchemy_models.Users", back_populates="deleted_broadcast", foreign_keys=[deleted_by])

    broadcast_recipients = relationship("BroadcastRecipients", back_populates="broadcast")



from sqlalchemy import (
    Column, Integer, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base

class BroadcastRecipients(Base):
    __tablename__ = "broadcast_recipients_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    broadcast_id = Column(Integer, ForeignKey("broadcasts_rmtest.id", ondelete="CASCADE"), nullable=False)
    state_id = Column(Integer, ForeignKey("states_rmtest.id", ondelete="CASCADE"), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    broadcast = relationship("Broadcasts", back_populates="broadcast_recipients")
    state = relationship("States", back_populates="broadcast_recipients")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_broadcast_recipients", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_broadcast_recipients", foreign_keys=[updated_by])


from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class PopularTopics(Base):
    __tablename__ = "popular_topics_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_popular_topics", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_popular_topics", foreign_keys=[updated_by])

    # One PopularTopic has many FAQs
    faqs = relationship("Faqs", back_populates="topic")



from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class Faqs(Base):
    __tablename__ = "faqs_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    question = Column(String, nullable=False, unique=True)
    answer = Column(String, nullable=False)
    topic_id = Column(Integer, ForeignKey("popular_topics_rmtest.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    topic = relationship("PopularTopics", back_populates="faqs")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_faqs", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_faqs", foreign_keys=[updated_by])



from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class ChatThreads(Base):
    __tablename__ = "chat_threads_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users_rmtest.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    last_seen_message_id = Column(Integer, ForeignKey("chat_messages_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="chat_threads")
    user = relationship("models.orm_alchemy_models.Users", back_populates="chat_threads", foreign_keys=[user_id])
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_chat_threads", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_chat_threads", foreign_keys=[updated_by])

    last_seen_message = relationship("ChatMessages", foreign_keys=[last_seen_message_id])

    participants = relationship("ChatThreadParticipants", back_populates="thread")
    messages = relationship("ChatMessages", back_populates="thread")


class ChatThreadParticipants(Base):
    __tablename__ = "chat_thread_participants_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    thread_id = Column(Integer, ForeignKey("chat_threads_rmtest.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("users_rmtest.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    last_seen_message_id = Column(Integer, ForeignKey("chat_messages_rmtest.id"), nullable=True)

    # --- Relationships ---
    thread = relationship("ChatThreads", back_populates="participants")
    user = relationship("models.orm_alchemy_models.Users", back_populates="chat_thread_participants", foreign_keys=[user_id])
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_chat_thread_participants", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_chat_thread_participants", foreign_keys=[updated_by])

    last_seen_message = relationship("ChatMessages", foreign_keys=[last_seen_message_id])


class ChatMessages(Base):
    __tablename__ = "chat_messages_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    thread_id = Column(Integer, ForeignKey("chat_threads_rmtest.id", ondelete="CASCADE"), nullable=False)
    sender_id = Column(Integer, ForeignKey("users_rmtest.id"), nullable=False)
    message = Column(String, nullable=True)
    is_edited = Column(Boolean, server_default="false", nullable=False)
    attachment_url = Column(String, nullable=True)
    deleted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    deleted_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    thread = relationship("ChatThreads", back_populates="messages")
    sender = relationship("models.orm_alchemy_models.Users", back_populates="sent_messages", foreign_keys=[sender_id])
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_chat_messages", foreign_keys=[created_by])
    deleted_by_user = relationship("models.orm_alchemy_models.Users", back_populates="deleted_chat_messages", foreign_keys=[deleted_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_chat_messages", foreign_keys=[updated_by])

    message_tags = relationship("MessageTags", back_populates="message")



from sqlalchemy import (
    Column, Integer, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class MessageTags(Base):
    __tablename__ = "message_tags_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(Integer, ForeignKey("chat_messages_rmtest.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("users_rmtest.id"), nullable=False)  # tagged user

    tagged_at = Column(DateTime, server_default=func.now(), nullable=False)

    # --- Relationships ---
    message = relationship("ChatMessages", back_populates="message_tags")
    tagged_user = relationship("models.orm_alchemy_models.Users", back_populates="tagged_in_messages", foreign_keys=[user_id])


from sqlalchemy import (
    Column, Integer, DateTime, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class SelfRealizationsAccountsMapping(Base):
    __tablename__ = "self_realizations_accounts_mapping_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    self_realizations_id = Column(Integer, ForeignKey("self_realizations_rmtest.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="self_realizations_accounts_mapping")
    self_realization = relationship("SelfRealizations", back_populates="self_realizations_accounts_mapping")

    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_self_realization_mappings", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_self_realization_mappings", foreign_keys=[updated_by])

    __table_args__ = (
        UniqueConstraint("account_id", "self_realizations_id", name="uq_account_self_realization"),
        Index("ix_self_realizations_accounts_mapping_account_id", "account_id"),
    )


from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class TargetMembershipDemographic(Base):
    __tablename__ = "target_membership_demographic_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    category = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    target_membership_demographic_mappings = relationship(
        "TargetMembershipDemographicMapping",
        back_populates="target_membership_demographic"
    )

    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_target_membership_demographics", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_target_membership_demographics", foreign_keys=[updated_by])

    __table_args__ = (
        UniqueConstraint("name", "category", name="uq_target_membership_name_category"),
    )


from sqlalchemy import (
    Column, Integer, DateTime, ForeignKey, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class TargetMembershipDemographicMapping(Base):
    __tablename__ = "target_membership_demographic_mapping_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    target_membership_demographic_id = Column(Integer, ForeignKey("target_membership_demographic_rmtest.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="target_membership_demographic_mappings")
    target_membership_demographic = relationship(
        "TargetMembershipDemographic",
        back_populates="target_membership_demographic_mappings"
    )

    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="created_target_membership_demographic_mappings", foreign_keys=[created_by])
    updated_by_user = relationship("models.orm_alchemy_models.Users", back_populates="updated_target_membership_demographic_mappings", foreign_keys=[updated_by])

    __table_args__ = (
        Index("ix_target_membership_mapping_account_id", "account_id"),
    )


from sqlalchemy import (
    Column, Integer, Boolean, DateTime, ForeignKey, JSON, Enum as SAEnum
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base
from models.enums import Chats  # Matches Prisma enum


class Notifications(Base):
    __tablename__ = "notifications_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    notification_type = Column(SAEnum(Chats), nullable=False)
    notification_obj = Column(JSON, nullable=False)
    is_read = Column(Boolean, server_default="false", nullable=False)
    read_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="notifications")
    user = relationship("models.orm_alchemy_models.Users", back_populates="notifications", foreign_keys=[user_id])


from sqlalchemy import (
    Column, Integer, String, Float, DateTime, ForeignKey, Enum as SAEnum
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base
from models.enums import CreditStatus  # Matches Prisma enum


class EztextingTransactionHistory(Base):
    __tablename__ = "eztexting_transaction_history_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)

    stripe_customer_id = Column(String, nullable=True)
    payment_intent_id = Column(String, nullable=True)
    last_error_message = Column(String, nullable=True)
    credit_status = Column(SAEnum(CreditStatus), nullable=False)
    invoice_id = Column(String, nullable=True)
    credit_amount = Column(Float, nullable=True)
    credit_value = Column(Integer, nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="eztexting_transaction_history")


class MindbodyLocations(Base):
    __tablename__ = "mindbody_locations_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)

    address = Column(String, nullable=True)
    city = Column(String, nullable=True)
    state_prov_code = Column(String, nullable=True)
    postal_code = Column(String, nullable=True)
    location_id = Column(Integer, nullable=False)
    location_name = Column(String, nullable=True)
    site_id = Column(Integer, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="mindbody_locations")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="mindbody_locations", foreign_keys=[created_by])


class MindbodySites(Base):
    __tablename__ = "mindbody_sites_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)

    time_zone = Column(String, nullable=True)
    site_id = Column(Integer, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="mindbody_sites")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="mindbody_sites", foreign_keys=[created_by])


from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime, ForeignKey, ARRAY
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class MindbodySaleServices(Base):
    __tablename__ = "mindbody_sale_services_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)

    product_id = Column(Integer, nullable=False)
    service_id = Column(String, nullable=True)
    name = Column(String, nullable=True)
    count = Column(Integer, nullable=True)
    service_type = Column(String, nullable=True)
    membership_id = Column(Integer, nullable=True)
    is_intro_offer = Column(Boolean, nullable=True)
    intro_offer_type = Column(String, nullable=True)
    program = Column(String, nullable=True)
    price = Column(Float, nullable=True)
    program_id = Column(Integer, nullable=True)
    revenue_category = Column(String, nullable=True)
    sell_at_location_ids = Column(ARRAY(Integer), server_default="{}", nullable=False)
    use_at_location_ids = Column(ARRAY(Integer), server_default="{}", nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="mindbody_sale_services")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="mindbody_sale_services", foreign_keys=[created_by])

class MindbodySitesPrograms(Base):
    __tablename__ = "mindbody_sites_programs_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)

    program_id = Column(Integer, nullable=False)
    program_name = Column(String, nullable=True)
    schedule_type = Column(String, nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="mindbody_sites_programs")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="mindbody_sites_programs", foreign_keys=[created_by])


class MindbodySitesSessionTypes(Base):
    __tablename__ = "mindbody_sites_session_types_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id"), nullable=False)

    session_type_id = Column(Integer, nullable=False)
    session_type_name = Column(String, nullable=True)
    program_id = Column(Integer, nullable=True)
    category = Column(String, nullable=True)
    sub_category = Column(String, nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="mindbody_sites_session_types")
    created_by_user = relationship("models.orm_alchemy_models.Users", back_populates="mindbody_sites_session_types", foreign_keys=[created_by])



from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class BatchProcessingState(Base):
    __tablename__ = "batch_processing_state_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_type = Column(String(255), nullable=False)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    last_page = Column(Integer, nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="batch_processing_state")

    __table_args__ = (
        UniqueConstraint("batch_type", "account_id", name="uq_batch_type_account"),
    )
from sqlalchemy import (
    Column, Integer, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class AtmosphereOptionsAccountsMapping(Base):
    __tablename__ = "atmosphere_options_accounts_mapping_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    atmosphere_option_id = Column(Integer, ForeignKey("atmosphere_options_rmtest.id", ondelete="CASCADE"), nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    deleted_at = Column(DateTime, nullable=True)
    deleted_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="atmosphere_options_accounts_mapping")
    atmosphere_option = relationship("AtmosphereOptions", back_populates="accounts_mapping")

    created_by_user = relationship("Users", back_populates="created_atmosphere_options_accounts", foreign_keys=[created_by])
    updated_by_user = relationship("Users", back_populates="updated_atmosphere_options_accounts", foreign_keys=[updated_by])
    deleted_by_user = relationship("Users", back_populates="deleted_atmosphere_options_accounts", foreign_keys=[deleted_by])


from sqlalchemy import (
    Column, Integer, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class KeySellingPointsAccountsMapping(Base):
    __tablename__ = "key_selling_points_accounts_mapping_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    key_selling_points_id = Column(Integer, ForeignKey("key_selling_points_rmtest.id", ondelete="CASCADE"), nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    deleted_at = Column(DateTime, nullable=True)
    deleted_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="key_selling_points_accounts_mapping")
    key_selling_point_details = relationship("KeySellingPoints", back_populates="accounts_mapping")

    created_by_user = relationship("Users", back_populates="created_key_selling_points_accounts", foreign_keys=[created_by])
    updated_by_user = relationship("Users", back_populates="updated_key_selling_points_accounts", foreign_keys=[updated_by])
    deleted_by_user = relationship("Users", back_populates="deleted_key_selling_points_accounts", foreign_keys=[deleted_by])


from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, JSON, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class EztextingMessageVerification(Base):
    __tablename__ = "eztexting_message_verification_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    message_id = Column(String(255), nullable=False)
    scheduled_at = Column(DateTime, nullable=False)
    verified_at = Column(DateTime, nullable=True)
    status = Column(String(50), nullable=True)

    # Counts
    bounced = Column(Integer, server_default="0", nullable=True)
    queued = Column(Integer, server_default="0", nullable=True)
    total_delivered = Column(Integer, server_default="0", nullable=True)
    total_not_sent = Column(Integer, server_default="0", nullable=True)

    group_ids = Column(JSON, nullable=True)
    failed_contacts = Column(JSON, nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="eztexting_message_verification")

    __table_args__ = (
        Index("ix_eztexting_message_verification_account_id", "account_id"),
    )


from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class AccountFaqs(Base):
    __tablename__ = "account_faqs_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False)
    description = Column(String, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    deleted_at = Column(DateTime, nullable=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="account_faqs")


from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, JSON, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class EmailConfigurations(Base):
    __tablename__ = "email_configurations_rmtest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    custom_message = Column(String(255), nullable=True)
    sendgrid_config = Column(JSON, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users_rmtest.id"), nullable=True)
    account_id = Column(Integer, ForeignKey("accounts_rmtest.id", ondelete="CASCADE"), nullable=False, unique=True)

    # --- Relationships ---
    account = relationship("Accounts", back_populates="email_configurations")
    created_by_user = relationship("Users", back_populates="created_email_configurations", foreign_keys=[created_by])
    updated_by_user = relationship("Users", back_populates="updated_email_configurations", foreign_keys=[updated_by])

    __table_args__ = (
        UniqueConstraint("account_id", name="uq_email_configurations_account_id"),
    )
