# models/schema_batch1.py
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Float, ForeignKey, UniqueConstraint, Index, JSON
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import ENUM
from models.base import Base
from models.enums import (
    Entity, Status, Role, StateStatus,
    SubscriptionStatusDb, CreditStatus,
    Personality, ScheduleType
)

# crm_integrations
class CrmIntegrations(Base):
    __tablename__ = "crm_integrations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    status = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    accounts = relationship("Accounts", back_populates="integration", lazy="select")  # relation from IntegrationAccounts


# atmosphere_options
class AtmosphereOptions(Base):
    __tablename__ = "atmosphere_options"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(String(255), unique=True, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)


# key_selling_points
class KeySellingPoints(Base):
    __tablename__ = "key_selling_points"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    is_custom = Column(Boolean, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    key_selling_points_accounts_mapping = relationship(
        "KeySellingPointsAccountsMapping",
        back_populates="keySellingPointDetails",
        lazy="select"
    )


# key_selling_points_accounts_mapping
class KeySellingPointsAccountsMapping(Base):
    __tablename__ = "key_selling_points_accounts_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    key_selling_points_id = Column(Integer, ForeignKey("key_selling_points.id"), nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    account = relationship("Accounts", back_populates="keySellingPointsMappings")
    keySellingPointDetails = relationship("KeySellingPoints", back_populates="key_selling_points_accounts_mapping")


# atmosphere_options_accounts_mapping
class AtmosphereOptionsAccountsMapping(Base):
    __tablename__ = "atmosphere_options_accounts_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    atmosphere_option_id = Column(Integer, nullable=False)  # Relation to AtmosphereOptions not explicitly in this batch
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    account = relationship("Accounts", back_populates="atmosphereOptionsMappings")


# membership_accounts
class MembershipAccounts(Base):
    __tablename__ = "membership_accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    plan_name = Column(String(255), nullable=False)
    pricing = Column(Float, nullable=False)
    registration_fees = Column(Float, nullable=False)
    credit_allocated = Column(String, nullable=False)
    credit_roll_over = Column(Boolean, nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="accountMemberships")
    createdBy = relationship("Users", foreign_keys=[created_by])
    updatedBy = relationship("Users", foreign_keys=[updated_by])
    deletedBy = relationship("Users", foreign_keys=[deleted_by])


# class_pack_accounts
class ClassPackAccounts(Base):
    __tablename__ = "class_pack_accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    plan_name = Column(String(255), nullable=False)
    pricing = Column(Float, nullable=False)
    no_credits = Column(String, nullable=False)
    credit_expiration = Column(Integer, nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="accountClassPacks")
    createdBy = relationship("Users", foreign_keys=[created_by])
    updatedBy = relationship("Users", foreign_keys=[updated_by])
    deletedBy = relationship("Users", foreign_keys=[deleted_by])




class Accounts(Base):
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_name = Column(String(100), nullable=False)
    city = Column(String(60))
    address = Column(String(255))
    entity = Column(ENUM(Entity, name='entity_enum'), nullable=True)
    crm_integration_id = Column(Integer, ForeignKey("crm_integrations.id"), nullable=False, default=1)
    status = Column(ENUM(Status, name='status_enum'), nullable=False, default=Status.ONBOARDING.value)
    on_boarding_step = Column(Integer, nullable=False, default=1)
    site_url = Column(String(255))
    integration_id = Column(String(60))
    integration_name = Column(String(60))
    crm_config = Column(JSON, nullable=True)
    ez_texting_id = Column(Integer, ForeignKey("eztexting_account.id", ondelete="CASCADE"))
    is_syncing = Column(Boolean, default=False)
    last_sync_time = Column(DateTime)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))
    callback_url = Column(String(255))
    callback_secret = Column(String(255))
    personality = Column(ENUM(Personality, name='personality_enum'), nullable=True)
    crm_api_end_point = Column(String)
    password = Column(JSON, nullable=True)
    username = Column(String)

    # Relationships
    eztexting_account = relationship("EztextingAccount", back_populates="accounts")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="accountsCreated")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="accountsUpdated")
    deletedBy = relationship("Users", foreign_keys=[deleted_by], back_populates="accountsDeleted")
    integration = relationship("CrmIntegrations", back_populates="accounts")
    accounts_users_mapping = relationship("AccountsUsersMapping", back_populates="account")
    contact_groups = relationship("ContactGroup", back_populates="account")
    contacts = relationship("Contact", back_populates="account")
    states = relationship("States", back_populates="account")
    batch_processing_state = relationship("BatchProcessingState", back_populates="account")
    eztexting_messages = relationship("EztextingMessages", back_populates="account")
    customers = relationship("Customers", back_populates="account")
    atmosphereOptions = relationship("AtmosphereOptionsAccountsMapping", back_populates="account")
    keySellingPoints = relationship("KeySellingPointsAccountsMapping", back_populates="account")
    selfRealizations = relationship("SelfRealizationsAccountsMapping", back_populates="account")
    targetMembershipDemographicMappings = relationship("TargetMembershipDemographicMapping", back_populates="account")
    previous_campaigns = relationship("PreviousCampaigns", back_populates="account")
    areas_to_improve = relationship("AreasToImprove", back_populates="account")
    unique_phrases = relationship("UniquePhrases", back_populates="account")
    tone_of_communication_mapping = relationship("ToneOfCommunicationMapping", back_populates="account")
    introductory_offer = relationship("IntroductoryOffer", back_populates="account")
    incentive_discount = relationship("IncentiveDiscount", back_populates="account")
    offer_and_service = relationship("OfferAndService", back_populates="account")
    state_messages = relationship("StateMessages", back_populates="account")

    accountMemberships = relationship("MembershipAccounts", back_populates="account")
    accountClassPacks = relationship("ClassPackAccounts", back_populates="account")
    broadcast = relationship("Broadcasts", back_populates="account")
    eztexting_message_verification = relationship("EztextingMessageVerification", back_populates="account")
    account_faqs = relationship("AccountFaqs", back_populates="account")
    notifications = relationship("Notifications", back_populates="account")
    email_configurations = relationship("EmailConfigurations", back_populates="account", uselist=False)

    # Indexes
    __table_args__ = (
        Index('ix_accounts_created_by', 'created_by'),
        Index('ix_accounts_updated_by', 'updated_by'),
        Index('ix_accounts_deleted_by', 'deleted_by'),
        Index('ix_accounts_ez_texting_id', 'ez_texting_id'),
    )


class EmailConfigurations(Base):
    __tablename__ = "email_configurations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    custom_message = Column(String(255))
    sendgrid_config = Column(JSON)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    account_id = Column(Integer, ForeignKey("accounts.id"), unique=True, nullable=False)
    account = relationship("Accounts", back_populates="email_configurations")


class Users(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False)
    name = Column(String(100))
    role = Column(ENUM(Role, name='role_enum'), nullable=False)
    email_verified = Column(Boolean, default=False)
    auth0_id = Column(String(60), unique=True, nullable=False)
    user_metadata = Column(JSON)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    # Self-referencing relations
    createdBy = relationship("Users", remote_side=[id], foreign_keys=[created_by], back_populates="createdUsers")
    createdUsers = relationship("Users", back_populates="createdBy", foreign_keys=[created_by])
    updatedBy = relationship("Users", remote_side=[id], foreign_keys=[updated_by], back_populates="updatedUsers")
    updatedUsers = relationship("Users", back_populates="updatedBy", foreign_keys=[updated_by])
    deletedBy = relationship("Users", remote_side=[id], foreign_keys=[deleted_by], back_populates="deletedUsers")
    deletedUsers = relationship("Users", back_populates="deletedBy", foreign_keys=[deleted_by])

    # Reverse/bidirectional relations for accounts
    accountsCreated = relationship("Accounts", back_populates="createdBy", foreign_keys="[Accounts.created_by]")
    accountsUpdated = relationship("Accounts", back_populates="updatedBy", foreign_keys="[Accounts.updated_by]")
    accountsDeleted = relationship("Accounts", back_populates="deletedBy", foreign_keys="[Accounts.deleted_by]")

    # Reverse relations for accounts_users_mapping
    accounts_users_mapping = relationship("AccountsUsersMapping", back_populates="creator")
    createdAccounts = relationship("AccountsUsersMapping", back_populates="createdBy")

    # Reverse relations for states
    statesCreated = relationship("States", back_populates="createdBy")
    statesUpdated = relationship("States", back_populates="updatedBy")
    statesDeleted = relationship("States", back_populates="deletedBy")

    # Payment setups
    paymentSetups = relationship("PaymentSetup", back_populates="user")

    # Subscription plans
    subscriptionPlansCreated = relationship("SubscriptionPlan", back_populates="createdBy")
    subscriptionPlansUpdated = relationship("SubscriptionPlan", back_populates="updatedBy")
    subscriptionPlansDeleted = relationship("SubscriptionPlan", back_populates="deletedBy")

    # Self realizations
    createdSelfRealizations = relationship("SelfRealizations", back_populates="createdBy")
    updatedSelfRealizations = relationship("SelfRealizations", back_populates="updatedBy")

    # Self realizations accounts mapping
    createdSelfRealizationMappings = relationship("SelfRealizationsAccountsMapping", back_populates="createdBy")
    updatedSelfRealizationMappings = relationship("SelfRealizationsAccountsMapping", back_populates="updatedBy")

    # Target membership demographic
    createdTargetMembershipDemographics = relationship("TargetMembershipDemographic", back_populates="createdBy")
    updatedTargetMembershipDemographics = relationship("TargetMembershipDemographic", back_populates="updatedBy")

    # Target membership demographic mapping
    createdTargetMembershipDemographicMappings = relationship("TargetMembershipDemographicMapping", back_populates="createdBy")
    updatedTargetMembershipDemographicMappings = relationship("TargetMembershipDemographicMapping", back_populates="updatedBy")

    createdPreviousCampaigns = relationship("PreviousCampaigns", back_populates="createdBy")
    updatedPreviousCampaigns = relationship("PreviousCampaigns", back_populates="updatedBy")

    createdAreasToImprove = relationship("AreasToImprove", back_populates="createdBy")
    updatedAreasToImprove = relationship("AreasToImprove", back_populates="updatedBy")

    createdUniquePhrases = relationship("UniquePhrases", back_populates="createdBy")
    updatedUniquePhrases = relationship("UniquePhrases", back_populates="updatedBy")

    createdToneOfCommunications = relationship("ToneOfCommunication", back_populates="createdBy")
    updatedToneOfCommunications = relationship("ToneOfCommunication", back_populates="updatedBy")

    # Account membership
    createdAccountMembership = relationship("MembershipAccounts", back_populates="createdBy")
    updatedAccountMembership = relationship("MembershipAccounts", back_populates="updatedBy")
    deletedAccountMembership = relationship("MembershipAccounts", back_populates="deletedBy")

    # Introductory offers
    createdIntroductoryOffers = relationship("IntroductoryOffer", back_populates="createdBy")
    updatedIntroductoryOffers = relationship("IntroductoryOffer", back_populates="updatedBy")

    # Incentive discounts
    createdIncentiveDiscounts = relationship("IncentiveDiscount", back_populates="createdBy")
    updatedIncentiveDiscounts = relationship("IncentiveDiscount", back_populates="updatedBy")

    # Offers and services
    createdOffersServices = relationship("OfferAndService", back_populates="createdBy")
    updatedOffersServices = relationship("OfferAndService", back_populates="updatedBy")

    # State messages
    createdStateMessages = relationship("StateMessages", back_populates="createdBy")
    updatedStateMessages = relationship("StateMessages", back_populates="updatedBy")

    # Account class pack
    createdAccountClassPack = relationship("ClassPackAccounts", back_populates="createdBy")
    updatedAccountClassPack = relationship("ClassPackAccounts", back_populates="updatedBy")
    deletedAccountClassPack = relationship("ClassPackAccounts", back_populates="deletedBy")

    # User subscription
    userSubscriptions = relationship("UserSubscription", back_populates="user")

    # Broadcasts
    createdBroadcast = relationship("Broadcasts", back_populates="createdBy")
    updatedBroadcast = relationship("Broadcasts", back_populates="updatedBy")
    deletedBroadcast = relationship("Broadcasts", back_populates="deletedBy")

    # Broadcast recipients
    createdBroadcastRecipients = relationship("BroadcastRecipients", back_populates="createdBy")
    updatedBroadcastRecipients = relationship("BroadcastRecipients", back_populates="updatedBy")

    # Popular topics
    createdPopularTopics = relationship("PopularTopics", back_populates="createdBy")
    updatedPopularTopics = relationship("PopularTopics", back_populates="updatedBy")

    # Faqs
    createdFaqs = relationship("Faqs", back_populates="createdBy")
    updatedFaqs = relationship("Faqs", back_populates="updatedBy")

    # Chat threads
    chatThreads = relationship("ChatThreads", back_populates="user")
    createdChatThreads = relationship("ChatThreads", back_populates="createdBy")
    updatedChatThreads = relationship("ChatThreads", back_populates="updatedBy")

    # Chat thread participants
    chatThreadParticipants = relationship("ChatThreadParticipants", back_populates="user")
    createdChatThreadParticipants = relationship("ChatThreadParticipants", back_populates="createdBy")
    updatedChatThreadParticipants = relationship("ChatThreadParticipants", back_populates="updatedBy")

    # Chat messages
    sentMessages = relationship("ChatMessages", back_populates="sender")
    createdChatMessages = relationship("ChatMessages", back_populates="createdBy")
    deletedChatMessages = relationship("ChatMessages", back_populates="deletedBy")
    updatedChatMessages = relationship("ChatMessages", back_populates="updatedBy")

    # Message tags
    taggedInMessages = relationship("MessageTags", back_populates="taggedUser")

    __table_args__ = (
        UniqueConstraint('email', name='uq_users_email'),
        UniqueConstraint('auth0_id', name='uq_users_auth0_id'),
    )






# ==========================================================
# accounts_users_mapping
# ==========================================================
class AccountsUsersMapping(Base):
    __tablename__ = "accounts_users_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)

    # Relationships
    account = relationship("Accounts", back_populates="accounts_users_mapping")
    users = relationship("Users", back_populates="accounts_users_mapping", foreign_keys=[user_id])
    createdBy = relationship("Users", back_populates="createdAccounts", foreign_keys=[created_by])

    __table_args__ = (
        UniqueConstraint('account_id', 'user_id', name='uq_account_user'),
        Index('ix_accounts_users_mapping_account_id', 'account_id'),
        Index('ix_accounts_users_mapping_user_id', 'user_id'),
    )


# ==========================================================
# customers
# ==========================================================
class Customers(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(255))
    location_id = Column(Integer, nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
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
    is_opted_in_to_sms = Column(Boolean, nullable=False, server_default="false")
    completed_class_count = Column(Integer, server_default="0")
    state_id = Column(Integer, ForeignKey("states.id"))
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)
    emailUnsubscribeHash = Column(String)
    isSubscribedToEmail = Column(Boolean, server_default="true")
    is_prospect = Column(Boolean)
    is_company = Column(Boolean)
    first_appointment_date = Column(DateTime)
    first_class_date = Column(DateTime)

    # Relationships
    credit_transactions = relationship("CreditTransactions", back_populates="customer")
    credit_transactions_orders = relationship("CreditTransactionsOrders", back_populates="customerOrder")
    membership_transactions = relationship("MembershipTransactions", back_populates="customer")
    orders = relationship("Orders", back_populates="orderCustomer")
    reservations = relationship("Reservations", back_populates="customer")
    messages = relationship("EztextingMessages", back_populates="customer")
    stateHistory = relationship("StateHistory", back_populates="customer")
    account = relationship("Accounts", back_populates="customers")
    states = relationship("States", back_populates="customers")

    __table_args__ = (
        Index('ix_customers_account_id', 'account_id'),
        Index('ix_customers_city', 'city'),
        Index('ix_customers_state_province', 'state_province'),
        Index('ix_customers_email', 'email'),
        Index('ix_customers_phone_number', 'phone_number'),
    )


# ==========================================================
# orders
# ==========================================================
class Orders(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String)
    date_placed = Column(DateTime)
    location = Column(String)
    payment_sources_labels = Column(String(255))
    status = Column(String(255))
    order_lines_id = Column(String(64))
    customer_ref_id = Column(Integer, ForeignKey("customers.id"))
    account_id = Column(Integer)
    customer_id = Column(String(255))
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # Relationships
    orderLines = relationship("OrderLines", back_populates="order")
    orderCustomer = relationship("Customers", back_populates="orders", foreign_keys=[customer_ref_id])

    __table_args__ = (
        Index('ix_orders_order_id', 'order_id'),
        Index('ix_orders_location', 'location'),
        Index('ix_orders_status', 'status'),
    )


from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey,
    UniqueConstraint, Index
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from models.base import Base


# ==========================================================
# order_lines
# ==========================================================
class OrderLines(Base):
    __tablename__ = "order_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_line_id = Column(String(64))
    order_id = Column(String)
    order_ref_id = Column(Integer, ForeignKey("orders.id"))
    account_id = Column(Integer)
    transaction_type = Column(String(255))
    location = Column(String(255))
    credit_transactions_id = Column(Integer)
    credit_transactions_ref_id = Column(Integer, unique=True)
    membership_transactions_id = Column(Integer)
    membership_transactions_ref_id = Column(Integer, unique=True)
    title = Column(String(255))
    processed_by = Column(Boolean, server_default="false", nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # Relationships
    order = relationship("Orders", back_populates="orderLines", foreign_keys=[order_ref_id])
    creditTransactionOrders = relationship("CreditTransactionsOrders", back_populates="creditTransactionOrders")
    membershipTransactionOrders = relationship("MembershipTransactionsOrders", back_populates="membershipTransactionOrders")

    __table_args__ = (
        Index('ix_order_lines_order_id', 'order_id'),
        Index('ix_order_lines_location', 'location'),
    )


# ==========================================================
# reservations
# ==========================================================
class Reservations(Base):
    __tablename__ = "reservations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    reservations_id = Column(Integer)
    cancel_date = Column(DateTime)
    check_in_date = Column(DateTime)
    creation_date = Column(DateTime)
    status = Column(String(16))
    credit_transactions_type = Column(String(64))
    credit_transactions_id = Column(Integer)
    credit_transactions_ref_id = Column(Integer, unique=True)
    membership_transactions_type = Column(String(64))
    membership_transactions_id = Column(Integer)
    membership_transactions_ref_id = Column(Integer, unique=True)
    guest = Column(Boolean, server_default="false", nullable=False)
    customer_id = Column(String(255))
    customer_ref_id = Column(Integer, ForeignKey("customers.id"))
    class_session_id = Column(String(255))
    class_session_ref_id = Column(Integer, ForeignKey("class_sessions.id"))
    account_id = Column(Integer)
    first_timer = Column(Boolean, server_default="false", nullable=False)
    reservation_type = Column(String(64))
    location = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # Relationships
    creditTransaction = relationship("CreditTransactions", back_populates="creditTransactionReservations")
    membershipTransaction = relationship("MembershipTransactions", back_populates="membershipTransactionReservations")
    reservationCustomer = relationship("Customers", back_populates="reservations", foreign_keys=[customer_ref_id])
    classSession = relationship("ClassSessions", back_populates="reservations", foreign_keys=[class_session_ref_id])

    __table_args__ = (
        Index('ix_reservations_reservations_id', 'reservations_id'),
        Index('ix_reservations_location', 'location'),
    )


# ==========================================================
# class_sessions
# ==========================================================
class ClassSessions(Base):
    __tablename__ = "class_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    class_session_id = Column(String)
    start_datetime = Column(DateTime)
    start_date = Column(String)
    location = Column(String)
    end_datetime = Column(DateTime)
    cancellation_datetime = Column(DateTime)
    account_id = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # Relationships
    reservations = relationship("Reservations", back_populates="classSession")

    __table_args__ = (
        Index('ix_class_sessions_class_session_id', 'class_session_id'),
        Index('ix_class_sessions_location', 'location'),
    )


# ==========================================================
# credit_transactions
# ==========================================================
class CreditTransactions(Base):
    __tablename__ = "credit_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    credit_transactions_id = Column(Integer)
    transaction_date = Column(DateTime)
    credit_name = Column(String(128))
    is_expired = Column(Boolean)
    is_intro_offer = Column(Boolean)
    parent_credit_transaction_type = Column(String(64))
    parent_credit_transaction_id = Column(Integer)
    customer_ref_id = Column(Integer, ForeignKey("customers.id"))
    customer_id = Column(String(255))
    account_id = Column(Integer)
    location = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # Relationships
    creditTransactionReservations = relationship("Reservations", back_populates="creditTransaction", uselist=False)
    creditTransactionsCustomer = relationship("Customers", back_populates="credit_transactions", foreign_keys=[customer_ref_id])

    __table_args__ = (
        Index('ix_credit_transactions_credit_transactions_id', 'credit_transactions_id'),
        Index('ix_credit_transactions_location', 'location'),
    )


# ==========================================================
# credit_transactions_orders
# ==========================================================
class CreditTransactionsOrders(Base):
    __tablename__ = "credit_transactions_orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    credit_transactions_id = Column(Integer)
    transaction_date = Column(DateTime)
    credit_name = Column(String(128))
    is_expired = Column(Boolean)
    is_intro_offer = Column(Boolean)
    parent_credit_transaction_type = Column(String(64))
    parent_credit_transaction_id = Column(Integer)
    remaining_credits_cache = Column(Integer)
    customer_ref_id = Column(Integer, ForeignKey("customers.id"))
    customer_id = Column(String(255))
    location = Column(Integer)
    account_id = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # Relationships
    creditTransactionsOrdersCustomer = relationship("Customers", back_populates="credit_transactions_orders", foreign_keys=[customer_ref_id])
    creditTransactionOrders = relationship("OrderLines", back_populates="creditTransactionOrders", uselist=False)

    __table_args__ = (
        Index('ix_credit_transactions_orders_credit_transactions_id', 'credit_transactions_id'),
        Index('ix_credit_transactions_orders_location', 'location'),
    )


# ==========================================================
# membership_transactions_orders
# ==========================================================
class MembershipTransactionsOrders(Base):
    __tablename__ = "membership_transactions_orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    membership_transactions_id = Column(Integer)
    transaction_date = Column(DateTime)
    membership_name = Column(String(128))
    parent_membership_transaction_id = Column(Integer)
    membership_instances_id = Column(Integer)
    membership_instances_ref_id = Column(Integer, ForeignKey("membership_instances.id"))
    payment_interval_end_date = Column(DateTime)
    customer_id = Column(String(255))
    account_id = Column(Integer)
    location = Column(Integer)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # Relationships
    membershipInstance = relationship("MembershipInstances", back_populates="membership_transactions_orders")
    membershipTransactionOrders = relationship("OrderLines", back_populates="membershipTransactionOrders", uselist=False)

    __table_args__ = (
        Index('ix_membership_transactions_orders_membership_transactions_id', 'membership_transactions_id'),
        Index('ix_membership_transactions_orders_location', 'location'),
    )



from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ENUM
from models.base import Base
from models.enums import contact_status, membership_status, StateStatus


# ==========================================================
# membership_instances
# ==========================================================
class MembershipInstances(Base):
    __tablename__ = "membership_instances"

    id = Column(Integer, primary_key=True, autoincrement=True)
    membership_instances_id = Column(Integer)
    purchase_date = Column(DateTime)
    membership_name = Column(String(128))
    renewal_rate_incl_tax = Column(String(16))
    status = Column(String(128))
    location = Column(Integer)
    renewal_count = Column(Integer)
    next_charge_date = Column(DateTime)
    account_id = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # Relationships
    membershipTransactions = relationship("MembershipTransactions", back_populates="membershipInstance")
    membershipTransactionsOrders = relationship("MembershipTransactionsOrders", back_populates="membershipInstance")

    __table_args__ = (
        Index('ix_membership_instances_membership_instances_id', 'membership_instances_id'),
        Index('ix_membership_instances_location', 'location'),
    )


# ==========================================================
# membership_transactions
# ==========================================================
class MembershipTransactions(Base):
    __tablename__ = "membership_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    membership_transactions_id = Column(Integer)
    transaction_date = Column(DateTime)
    membership_name = Column(String(128))
    parent_membership_transaction_id = Column(Integer)
    membership_instances_id = Column(Integer)
    membership_instances_ref_id = Column(Integer, ForeignKey("membership_instances.id"))
    customer_ref_id = Column(Integer, ForeignKey("customers.id"))
    customer_id = Column(String(255))
    account_id = Column(Integer)
    location = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    # Relationships
    creditTransactionsCustomer = relationship(
        "Customers", back_populates="membership_transactions", foreign_keys=[customer_ref_id]
    )
    membershipTransactionReservations = relationship(
        "Reservations", back_populates="membershipTransaction", uselist=False
    )
    membershipInstance = relationship("MembershipInstances", back_populates="membershipTransactions")

    __table_args__ = (
        Index('ix_membership_transactions_membership_transactions_id', 'membership_transactions_id'),
        Index('ix_membership_transactions_location', 'location'),
    )


# ==========================================================
# contact
# ==========================================================
class Contact(Base):
    __tablename__ = "contact"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    phone_number = Column(String(16), nullable=False)
    first_name = Column(String(128), nullable=False)
    last_name = Column(String(128), nullable=False)
    email = Column(String(255), nullable=False)
    status = Column(ENUM(contact_status, name='contact_status_enum'), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    customer_id = Column(Integer)  # Note: No FK to customers in this batch

    # Relationships
    account = relationship("Accounts", back_populates="contacts")
    memberships = relationship("ContactGroupMembership", back_populates="contact")

    __table_args__ = (
        UniqueConstraint('account_id', 'phone_number', name='uq_contact_account_phone'),
    )


# ==========================================================
# contact_group
# ==========================================================
class ContactGroup(Base):
    __tablename__ = "contact_group"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    eztexting_group_id = Column(String(128))
    name = Column(String(128), nullable=False)
    note = Column(String(255))
    strict_validation = Column(Boolean, server_default="false", nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    memberships = relationship("ContactGroupMembership", back_populates="contact_group")
    account = relationship("Accounts", back_populates="contact_groups")
    states = relationship("States", back_populates="contact_group")

    __table_args__ = (
        Index('ix_contact_group_account_id', 'account_id'),
        UniqueConstraint('eztexting_group_id', 'account_id', name='uq_contact_group_per_account'),
    )


# ==========================================================
# contact_group_membership
# ==========================================================
class ContactGroupMembership(Base):
    __tablename__ = "contact_group_membership"

    id = Column(Integer, primary_key=True, autoincrement=True)
    contact_id = Column(Integer, ForeignKey("contact.id", ondelete="CASCADE"), nullable=False)
    contact_group_id = Column(Integer, ForeignKey("contact_group.id", ondelete="CASCADE"), nullable=False)
    added_at = Column(DateTime, server_default=func.now(), nullable=False)
    removed_at = Column(DateTime)
    status = Column(ENUM(membership_status, name='membership_status_enum'), nullable=False)

    # Relationships
    contact_group = relationship("ContactGroup", back_populates="memberships")
    contact = relationship("Contact", back_populates="memberships")

    __table_args__ = (
        Index('ix_contact_group_membership_contact_group_id', 'contact_group_id'),
        Index('ix_contact_group_membership_contact_id_group_id', 'contact_id', 'contact_group_id'),
    )


# ==========================================================
# eztexting_account
# ==========================================================
class EztextingAccount(Base):
    __tablename__ = "eztexting_account"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False, unique=True)
    password = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    accounts = relationship("Accounts", back_populates="eztexting_account")


# ==========================================================
# states
# ==========================================================
class States(Base):
    __tablename__ = "states"

    id = Column(Integer, primary_key=True, autoincrement=True)
    crm_id = Column(Integer)
    account_id = Column(Integer, ForeignKey("accounts.id", ondelete="CASCADE"))
    contact_group_id = Column(Integer, ForeignKey("contact_group.id", ondelete="NO ACTION"))
    priority = Column(Integer, nullable=False)
    state_name = Column(String(100), nullable=False)
    description = Column(String(255), server_default="", nullable=False)
    is_predefined = Column(Boolean, server_default="false", nullable=False)
    enabled_email = Column(Boolean, server_default="false", nullable=False)
    enabled_sms = Column(Boolean, server_default="true", nullable=False)
    status = Column(ENUM(StateStatus, name='state_status_enum'), server_default=StateStatus.WAITING_FOR_APPROVAL.value, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    # Relationships
    account = relationship("Accounts", back_populates="states")
    contact_group = relationship("ContactGroup", back_populates="states")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="statesCreated")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="statesUpdated")
    deletedBy = relationship("Users", foreign_keys=[deleted_by], back_populates="statesDeleted")
    stateTransitionsFrom = relationship("StateTransitions", back_populates="stateId", foreign_keys="StateTransitions.state_id")
    stateTransitionsTo = relationship("StateTransitions", back_populates="nextState", foreign_keys="StateTransitions.next_state_id")
    states_conditions = relationship("StatesConditions", back_populates="state")
    customers = relationship("Customers", back_populates="states")
    state_messages = relationship("StateMessages", back_populates="state")
    state_history = relationship("StateHistory", back_populates="state")
    states = relationship("BroadcastRecipients", back_populates="states")

    __table_args__ = (
        Index('ix_states_created_by', 'created_by'),
        Index('ix_states_updated_by', 'updated_by'),
        Index('ix_states_deleted_by', 'deleted_by'),
    )


# ==========================================================
# state_transitions
# ==========================================================
class StateTransitions(Base):
    __tablename__ = "state_transitions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_id = Column(Integer, ForeignKey("states.id", ondelete="CASCADE"), nullable=False)
    next_state_id = Column(Integer, ForeignKey("states.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    stateId = relationship("States", back_populates="stateTransitionsFrom", foreign_keys=[state_id])
    nextState = relationship("States", back_populates="stateTransitionsTo", foreign_keys=[next_state_id])

    __table_args__ = (
        Index('ix_state_transitions_state_id_next_state_id', 'state_id', 'next_state_id'),
    )



from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, UniqueConstraint, Index, Float
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ENUM, JSON
from models.base import Base
from models.enums import (
    Condition, logical_operator, SubscriptionStatusDb
)

# ==========================================================
# states_conditions
# ==========================================================
class StatesConditions(Base):
    __tablename__ = "states_conditions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_id = Column(Integer, ForeignKey("states.id", ondelete="CASCADE"), nullable=False)
    data_category = Column(String(100), nullable=False)
    field = Column(String(100), nullable=False)
    condition = Column(ENUM(Condition, name="condition_enum"), nullable=False)
    value = Column(String(100))
    logical_operator = Column(ENUM(logical_operator, name="logical_operator_enum"))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relations
    StateConditions = relationship("States", back_populates="states_conditions")

    __table_args__ = (
        Index('ix_states_conditions_state_id', 'state_id'),
    )


# ==========================================================
# eztexting_messages
# ==========================================================
class EztextingMessages(Base):
    __tablename__ = "eztexting_messages"

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
    is_escalated = Column(Boolean, server_default="false")
    is_answered_by_ai = Column(Boolean, server_default="false")
    escalated_answered_for = Column(JSON, nullable=False, server_default="[]")
    escalated_answered_at = Column(DateTime)
    message_group_id = Column(String(255))
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    customer_ref_id = Column(Integer, ForeignKey("customers.id", ondelete="SET NULL"))
    customer_id = Column(String(255))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, onupdate=func.now(), nullable=False)

    # Relations
    account = relationship("Accounts", back_populates="eztexting_messages")
    customer = relationship("Customers", back_populates="messages")

    __table_args__ = (
        Index('ix_ezm_account_id', 'account_id'),
        Index('ix_ezm_customer_id', 'customer_id'),
        Index('ix_ezm_contact_number', 'contact_number'),
        Index('ix_ezm_status', 'status'),
        Index('ix_ezm_sent_at', 'sent_at'),
    )


# ==========================================================
# batch_processing_state
# ==========================================================
class BatchProcessingState(Base):
    __tablename__ = "batch_processing_state"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_type = Column(String(255), nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    last_page = Column(Integer, nullable=False)
    updated_at = Column(DateTime, onupdate=func.now(), nullable=False)

    account = relationship("Accounts", back_populates="batch_processing_state")

    __table_args__ = (
        UniqueConstraint('batch_type', 'account_id', name='uq_batch_type_account_id'),
    )


# ==========================================================
# payment_setup
# ==========================================================
class PaymentSetup(Base):
    __tablename__ = "payment_setup"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stripe_customer_id = Column(String(255), nullable=False, unique=True)
    stripe_setup_intent_id = Column(String(255))
    stripe_payment_method_id = Column(String(255))
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, unique=True)
    credits_remaining = Column(Integer, server_default="0")
    has_used_trial = Column(Boolean, server_default="false")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, onupdate=func.now(), nullable=False)

    user = relationship("Users", back_populates="paymentSetups")


# ==========================================================
# subscription_plan
# ==========================================================
class SubscriptionPlan(Base):
    __tablename__ = "subscription_plan"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_name = Column(String(255), nullable=False)
    stripe_product_id = Column(String(255))
    stripe_price_id = Column(String(255))
    credits = Column(Integer, server_default="0")
    price = Column(Float, server_default="0")
    billing_interval = Column(String(10), server_default="month")
    trial_days = Column(Integer)
    is_active = Column(Boolean, server_default="true")
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))
    features = Column(JSON)

    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="subscriptionPlansCreated")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="subscriptionPlansUpdated")
    deletedBy = relationship("Users", foreign_keys=[deleted_by], back_populates="subscriptionPlansDeleted")
    user_subscriptions = relationship("UserSubscription", back_populates="plan", foreign_keys="UserSubscription.plan_id")
    user_subscriptions_prev = relationship("UserSubscription", back_populates="previousPlan", foreign_keys="UserSubscription.previous_plan_id")


# ==========================================================
# user_subscription
# ==========================================================
class UserSubscription(Base):
    __tablename__ = "user_subscription"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    plan_id = Column(Integer, ForeignKey("subscription_plan.id"), nullable=False)
    stripe_subscription_id = Column(String(255))
    status = Column(ENUM(SubscriptionStatusDb, name='subscriptionstatusdb_enum'), server_default=SubscriptionStatusDb.ACTIVE.value, nullable=False)
    started_at = Column(DateTime, server_default=func.now(), nullable=False)
    ended_at = Column(DateTime)
    reason = Column(String(100))
    updated_at = Column(DateTime, onupdate=func.now(), nullable=False)
    previous_plan_id = Column(Integer, ForeignKey("subscription_plan.id"))

    user = relationship("Users", back_populates="userSubscriptions")
    plan = relationship("SubscriptionPlan", back_populates="user_subscriptions", foreign_keys=[plan_id])
    previousPlan = relationship("SubscriptionPlan", back_populates="user_subscriptions_prev", foreign_keys=[previous_plan_id])


# ==========================================================
# self_realizations
# ==========================================================
class SelfRealizations(Base):
    __tablename__ = "self_realizations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    is_custom = Column(Boolean, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    self_realizations_accounts_mapping = relationship("SelfRealizationsAccountsMapping", back_populates="selfRealization")

    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdSelfRealizations")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedSelfRealizations")


# ==========================================================
# self_realizations_accounts_mapping
# ==========================================================
class SelfRealizationsAccountsMapping(Base):
    __tablename__ = "self_realizations_accounts_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    self_realizations_id = Column(Integer, ForeignKey("self_realizations.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="selfRealizations")
    selfRealization = relationship("SelfRealizations", back_populates="self_realizations_accounts_mapping")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdSelfRealizationMappings")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedSelfRealizationMappings")


# ==========================================================
# target_membership_demographic
# ==========================================================
class TargetMembershipDemographic(Base):
    __tablename__ = "target_membership_demographic"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    category = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    target_membership_demographic_mappings = relationship("TargetMembershipDemographicMapping", back_populates="targetMembershipDemographic")

    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdTargetMembershipDemographics")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedTargetMembershipDemographics")

    __table_args__ = (
        UniqueConstraint('name', 'category', name='uq_name_category'),
    )


# ==========================================================
# target_membership_demographic_mapping
# ==========================================================
class TargetMembershipDemographicMapping(Base):
    __tablename__ = "target_membership_demographic_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    target_membership_demographic_id = Column(Integer, ForeignKey("target_membership_demographic.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="targetMembershipDemographicMappings")
    targetMembershipDemographic = relationship("TargetMembershipDemographic", back_populates="target_membership_demographic_mappings")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdTargetMembershipDemographicMappings")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedTargetMembershipDemographicMappings")


# ==========================================================
# previous_campaigns
# ==========================================================
class PreviousCampaigns(Base):
    __tablename__ = "previous_campaigns"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="previous_campaigns")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdPreviousCampaigns")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedPreviousCampaigns")


from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Index
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ENUM
from models.base import Base
from models.enums import ScheduleType, StateHistoryType  # enums from earlier batches + this batch


# ==========================================================
# areas_to_improve
# ==========================================================
class AreasToImprove(Base):
    __tablename__ = "areas_to_improve"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="areas_to_improve")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdAreasToImprove")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedAreasToImprove")


# ==========================================================
# unique_phrases
# ==========================================================
class UniquePhrases(Base):
    __tablename__ = "unique_phrases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    phrase = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="unique_phrases")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdUniquePhrases")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedUniquePhrases")


# ==========================================================
# tone_of_communication
# ==========================================================
class ToneOfCommunication(Base):
    __tablename__ = "tone_of_communication"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(200), nullable=False)
    description = Column(String(200), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    tone_of_communication_mapping = relationship("ToneOfCommunicationMapping", back_populates="tone_of_communication")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdToneOfCommunications")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedToneOfCommunications")


# ==========================================================
# tone_of_communication_mapping
# ==========================================================
class ToneOfCommunicationMapping(Base):
    __tablename__ = "tone_of_communication_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    tone_of_communication_id = Column(Integer, ForeignKey("tone_of_communication.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer)

    account = relationship("Accounts", back_populates="tone_of_communication_mapping")
    tone_of_communication = relationship("ToneOfCommunication", back_populates="tone_of_communication_mapping")


# ==========================================================
# introductory_offer
# ==========================================================
class IntroductoryOffer(Base):
    __tablename__ = "introductory_offer"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    offer_name = Column(String(400), nullable=False)
    pricing = Column(Integer)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="introductory_offer")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdIntroductoryOffers")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedIntroductoryOffers")


# ==========================================================
# incentive_discount
# ==========================================================
class IncentiveDiscount(Base):
    __tablename__ = "incentive_discount"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="incentive_discount")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdIncentiveDiscounts")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedIncentiveDiscounts")


# ==========================================================
# offer_and_service
# ==========================================================
class OfferAndService(Base):
    __tablename__ = "offer_and_service"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    service_name = Column(String(400), nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="offer_and_service")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdOffersServices")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedOffersServices")


# ==========================================================
# state_messages
# ==========================================================
class StateMessages(Base):
    __tablename__ = "state_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_id = Column(Integer, ForeignKey("states.id", ondelete="CASCADE"), nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    message_frequency = Column(String(20), nullable=False)
    scheduled_at = Column(String(20))
    message_body = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="state_messages")
    stateMessages = relationship("States", back_populates="state_messages")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdStateMessages")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedStateMessages")


# ==========================================================
# state_history
# ==========================================================
class StateHistory(Base):
    __tablename__ = "state_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_id = Column(Integer, ForeignKey("states.id", ondelete="CASCADE"), nullable=False)
    contact_group_id = Column(Integer)
    customer_id = Column(String(255))
    customer_ref_id = Column(Integer, ForeignKey("customers.id", ondelete="SET NULL"))
    history_type = Column(ENUM(StateHistoryType, name="state_history_type_enum"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    stateHistory = relationship("States", back_populates="state_history")
    customer = relationship("Customers", back_populates="stateHistory")


# ==========================================================
# broadcasts
# ==========================================================
class Broadcasts(Base):
    __tablename__ = "broadcasts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    name = Column(String, nullable=False)
    schedule_type = Column(ENUM(ScheduleType, name="schedule_type_enum"), nullable=False)
    sent_at = Column(DateTime)
    message = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="broadcast")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdBroadcast")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedBroadcast")
    deletedBy = relationship("Users", foreign_keys=[deleted_by], back_populates="deletedBroadcast")
    broadcast_recipients = relationship("BroadcastRecipients", back_populates="broadcast")


# ==========================================================
# broadcast_recipients
# ==========================================================
class BroadcastRecipients(Base):
    __tablename__ = "broadcast_recipients"

    id = Column(Integer, primary_key=True, autoincrement=True)
    broadcast_id = Column(Integer, ForeignKey("broadcasts.id"), nullable=False)
    state_id = Column(Integer, ForeignKey("states.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    broadcast = relationship("Broadcasts", back_populates="broadcast_recipients")
    states = relationship("States", back_populates="states")
    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdBroadcastRecipients")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedBroadcastRecipients")



from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Index
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSON
from models.base import Base


# ==========================================================
# credit_transactions_testing
# ==========================================================
class CreditTransactionsTesting(Base):
    __tablename__ = "credit_transactions_testing"

    id = Column(Integer, primary_key=True, autoincrement=True)
    credit_transactions_id = Column(Integer)
    transaction_date = Column(DateTime)
    credit_name = Column(String(128))
    is_expired = Column(Boolean)
    is_intro_offer = Column(Boolean)
    parent_credit_transaction_type = Column(String(64))
    parent_credit_transaction_id = Column(Integer)
    customer_id = Column(String(255))
    location = Column(Integer)
    account_id = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now(), nullable=False)
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)


# ==========================================================
# credit_transactions_orders_testing
# ==========================================================
class CreditTransactionsOrdersTesting(Base):
    __tablename__ = "credit_transactions_orders_testing"

    id = Column(Integer, primary_key=True, autoincrement=True)
    credit_transactions_id = Column(Integer)
    transaction_date = Column(DateTime)
    credit_name = Column(String(128))
    is_expired = Column(Boolean)
    is_intro_offer = Column(Boolean)
    parent_credit_transaction_type = Column(String(64))
    parent_credit_transaction_id = Column(Integer)
    customer_id = Column(String(255))
    account_id = Column(Integer)
    location = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now(), nullable=False)
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)


# ==========================================================
# membership_transactions_orders_testing
# ==========================================================
class MembershipTransactionsOrdersTesting(Base):
    __tablename__ = "membership_transactions_orders_testing"

    id = Column(Integer, primary_key=True, autoincrement=True)
    membership_transactions_id = Column(Integer)
    transaction_date = Column(DateTime)
    membership_name = Column(String(128))
    parent_membership_transaction_id = Column(Integer)
    membership_instances_id = Column(Integer)
    membership_instances_ref_id = Column(Integer)
    payment_interval_end_date = Column(DateTime)
    customer_id = Column(String(255))
    location = Column(Integer)
    account_id = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now(), nullable=False)
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)


# ==========================================================
# membership_instances_testing
# ==========================================================
class MembershipInstancesTesting(Base):
    __tablename__ = "membership_instances_testing"

    id = Column(Integer, primary_key=True, autoincrement=True)
    membership_instances_id = Column(Integer)
    purchase_date = Column(DateTime)
    membership_name = Column(String(128))
    renewal_rate_incl_tax = Column(String(16))
    status = Column(String(128))
    location = Column(Integer)
    renewal_count = Column(Integer)
    account_id = Column(Integer)
    next_charge_date = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now(), nullable=False)
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)


# ==========================================================
# membership_transactions_testing
# ==========================================================
class MembershipTransactionsTesting(Base):
    __tablename__ = "membership_transactions_testing"

    id = Column(Integer, primary_key=True, autoincrement=True)
    membership_transactions_id = Column(Integer)
    transaction_date = Column(DateTime)
    membership_name = Column(String(128))
    parent_membership_transaction_id = Column(Integer)
    membership_instances_id = Column(Integer)
    membership_instances_ref_id = Column(Integer)
    customer_id = Column(String(255))
    location = Column(Integer)
    account_id = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now(), nullable=False)
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)


# ==========================================================
# eztexting_message_verification
# ==========================================================
class EztextingMessageVerification(Base):
    __tablename__ = "eztexting_message_verification"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    message_id = Column(String(255), nullable=False)
    scheduled_at = Column(DateTime, nullable=False)
    verified_at = Column(DateTime)
    status = Column(String(50))
    bounced = Column(Integer, server_default="0")
    queued = Column(Integer, server_default="0")
    total_delivered = Column(Integer, server_default="0")
    total_not_sent = Column(Integer, server_default="0")
    group_ids = Column(JSON)
    failed_contacts = Column(JSON)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now(), nullable=False)

    account = relationship("Accounts", back_populates="eztexting_message_verification")

    __table_args__ = (
        Index('ix_emv_account_id', 'account_id'),
    )


# ==========================================================
# popular_topics
# ==========================================================
class PopularTopics(Base):
    __tablename__ = "popular_topics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdPopularTopics")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedPopularTopics")
    faqs = relationship("Faqs", back_populates="topic")


from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Float, Index
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSON, ARRAY, ENUM
from models.base import Base
from models.enums import CreditStatus, Chats  # New enums + from before


# ==========================================================
# faqs
# ==========================================================
class Faqs(Base):
    __tablename__ = "faqs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    question = Column(String, unique=True, nullable=False)
    answer = Column(String, nullable=False)
    topic_id = Column(Integer, ForeignKey("popular_topics.id"))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdFaqs")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedFaqs")
    topic = relationship("PopularTopics", back_populates="faqs")


# ==========================================================
# account_faqs
# ==========================================================
class AccountFaqs(Base):
    __tablename__ = "account_faqs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    description = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    deleted_at = Column(DateTime)

    account = relationship("Accounts", back_populates="account_faqs")


# ==========================================================
# chat_threads
# ==========================================================
class ChatThreads(Base):
    __tablename__ = "chat_threads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))
    last_seen_message_id = Column(Integer)

    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdChatThreads")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedChatThreads")
    user = relationship("Users", foreign_keys=[user_id], back_populates="chatThreads")
    participants = relationship("ChatThreadParticipants", back_populates="thread")
    messages = relationship("ChatMessages", back_populates="thread")


# ==========================================================
# chat_thread_participants
# ==========================================================
class ChatThreadParticipants(Base):
    __tablename__ = "chat_thread_participants"

    id = Column(Integer, primary_key=True, autoincrement=True)
    thread_id = Column(Integer, ForeignKey("chat_threads.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))
    last_seen_message_id = Column(Integer)

    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdChatThreadParticipants")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedChatThreadParticipants")
    thread = relationship("ChatThreads", back_populates="participants")
    user = relationship("Users", foreign_keys=[user_id], back_populates="chatThreadParticipants")


# ==========================================================
# chat_messages
# ==========================================================
class ChatMessages(Base):
    __tablename__ = "chat_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    thread_id = Column(Integer, ForeignKey("chat_threads.id"), nullable=False)
    sender_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    message = Column(String)
    is_edited = Column(Boolean, server_default="false", nullable=False)
    attachment_url = Column(String)
    deleted_at = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    deleted_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    createdBy = relationship("Users", foreign_keys=[created_by], back_populates="createdChatMessages")
    deletedBy = relationship("Users", foreign_keys=[deleted_by], back_populates="deletedChatMessages")
    updatedBy = relationship("Users", foreign_keys=[updated_by], back_populates="updatedChatMessages")
    thread = relationship("ChatThreads", back_populates="messages")
    sender = relationship("Users", foreign_keys=[sender_id], back_populates="sentMessages")
    messageTags = relationship("MessageTags", back_populates="message")


# ==========================================================
# message_tags
# ==========================================================
class MessageTags(Base):
    __tablename__ = "message_tags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(Integer, ForeignKey("chat_messages.id", ondelete="CASCADE"), nullable=False)
    tagged_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    message = relationship("ChatMessages", back_populates="messageTags")
    taggedUser = relationship("Users", foreign_keys=[tagged_user_id], back_populates="taggedInMessages")


# ==========================================================
# notifications
# ==========================================================
class Notifications(Base):
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    userId = Column(Integer)
    notification_type = Column(ENUM(Chats, name="chats_enum"), nullable=False)
    notification_obj = Column(JSON, nullable=False)
    is_read = Column(Boolean, server_default="false", nullable=False)
    read_at = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    account = relationship("Accounts", back_populates="notifications")


# ==========================================================
# eztexting_transaction_history
# ==========================================================
class EztextingTransactionHistory(Base):
    __tablename__ = "eztexting_transaction_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer)
    stripe_customer_id = Column(String)
    payment_intent_id = Column(String)
    last_error_message = Column(String)
    credit_status = Column(ENUM(CreditStatus, name="creditstatus_enum"), nullable=False)
    invoice_id = Column(String)
    credit_amount = Column(Float)
    credit_value = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


# ==========================================================
# mindbody_locations
# ==========================================================
class MindbodyLocations(Base):
    __tablename__ = "mindbody_locations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer)
    address = Column(String)
    city = Column(String)
    state_prov_code = Column(String)
    postal_code = Column(String)
    location_id = Column(Integer)
    location_name = Column(String)
    site_id = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)


# ==========================================================
# mindbody_sites
# ==========================================================
class MindbodySites(Base):
    __tablename__ = "mindbody_sites"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer)
    time_zone = Column(String)
    site_id = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)


# ==========================================================
# mindbody_sale_services
# ==========================================================
class MindbodySaleServices(Base):
    __tablename__ = "mindbody_sale_services"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer)
    product_id = Column(Integer)
    service_id = Column(String)
    name = Column(String)
    count = Column(Integer)
    service_type = Column(String)
    membership_id = Column(Integer)
    is_intro_offer = Column(Boolean)
    intro_offer_type = Column(String)
    program = Column(String)
    price = Column(Float)
    program_id = Column(Integer)
    revenue_category = Column(String)
    sell_at_location_ids = Column(ARRAY(Integer), server_default="{}")
    use_at_location_ids = Column(ARRAY(Integer), server_default="{}")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)


# ==========================================================
# mindbody_sites_programs
# ==========================================================
class MindbodySitesPrograms(Base):
    __tablename__ = "mindbody_sites_programs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer)
    program_id = Column(Integer)
    program_name = Column(String)
    schedule_type = Column(String)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)


# ==========================================================
# mindbody_sites_session_types
# ==========================================================
class MindbodySitesSessionTypes(Base):
    __tablename__ = "mindbody_sites_session_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer)
    session_type_id = Column(Integer)
    session_type_name = Column(String)
    program_id = Column(Integer)
    category = Column(String)
    sub_category = Column(String)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
