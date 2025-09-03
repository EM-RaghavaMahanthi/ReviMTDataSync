from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Float, ForeignKey,
    UniqueConstraint, Index, JSON, ARRAY
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ENUM
from models.base import Base
from models.enums import (
    Entity, Status, Role, Personality, ScheduleType, StateStatus,
    CreditStatus, SubscriptionStatusDb, Condition, LogicalOperator,
    ContactStatus, MembershipStatus, StateHistoryType, Chats
)


# ==========================================================
# USERS — fully rewired
# ==========================================================
class Users(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), nullable=False, unique=True)
    name = Column(String(100))
    role = Column(ENUM(Role, name="role_enum"), nullable=False)
    email_verified = Column(Boolean, server_default="false")
    auth0_id = Column(String(60), nullable=False, unique=True)
    user_metadata = Column(JSON)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    # Self-relationships
    createdBy = relationship("models.sql_alchemy_models.Users", remote_side=[id], foreign_keys=[created_by], back_populates="createdUsers")
    createdUsers = relationship("models.sql_alchemy_models.Users", foreign_keys=[created_by], back_populates="createdBy")
    updatedBy = relationship("models.sql_alchemy_models.Users", remote_side=[id], foreign_keys=[updated_by], back_populates="updatedUsers")
    updatedUsers = relationship("models.sql_alchemy_models.Users", foreign_keys=[updated_by], back_populates="updatedBy")
    deletedBy = relationship("models.sql_alchemy_models.Users", remote_side=[id], foreign_keys=[deleted_by], back_populates="deletedUsers")
    deletedUsers = relationship("models.sql_alchemy_models.Users", foreign_keys=[deleted_by], back_populates="deletedBy")

    # Accounts
    accountsCreated = relationship("Accounts", back_populates="createdBy", foreign_keys="Accounts.created_by")
    accountsUpdated = relationship("Accounts", back_populates="updatedBy", foreign_keys="Accounts.updated_by")
    accountsDeleted = relationship("Accounts", back_populates="deletedBy", foreign_keys="Accounts.deleted_by")

    # Accounts mapping
    accounts_users_mapping = relationship("AccountsUsersMapping", back_populates="users", foreign_keys="AccountsUsersMapping.user_id")
    createdAccounts = relationship("AccountsUsersMapping", back_populates="createdBy", foreign_keys="AccountsUsersMapping.created_by")

    # Created/updated/deleted states
    statesCreated = relationship("States", back_populates="createdBy", foreign_keys="States.created_by")
    statesUpdated = relationship("States", back_populates="updatedBy", foreign_keys="States.updated_by")
    statesDeleted = relationship("States", back_populates="deletedBy", foreign_keys="States.deleted_by")

    # Payment setup
    paymentSetups = relationship("PaymentSetup", back_populates="user")

    # Subscription plans
    subscriptionPlansCreated = relationship("SubscriptionPlan", back_populates="createdBy", foreign_keys="SubscriptionPlan.created_by")
    subscriptionPlansUpdated = relationship("SubscriptionPlan", back_populates="updatedBy", foreign_keys="SubscriptionPlan.updated_by")
    subscriptionPlansDeleted = relationship("SubscriptionPlan", back_populates="deletedBy", foreign_keys="SubscriptionPlan.deleted_by")

    # Self realizations
    createdSelfRealizations = relationship("SelfRealizations", back_populates="createdBy", foreign_keys="SelfRealizations.created_by")
    updatedSelfRealizations = relationship("SelfRealizations", back_populates="updatedBy", foreign_keys="SelfRealizations.updated_by")

    # Self realization mappings
    createdSelfRealizationMappings = relationship("SelfRealizationsAccountsMapping", back_populates="createdBy", foreign_keys="SelfRealizationsAccountsMapping.created_by")
    updatedSelfRealizationMappings = relationship("SelfRealizationsAccountsMapping", back_populates="updatedBy", foreign_keys="SelfRealizationsAccountsMapping.updated_by")

    # Target membership demographics
    createdTargetMembershipDemographics = relationship("TargetMembershipDemographic", back_populates="createdBy", foreign_keys="TargetMembershipDemographic.created_by")
    updatedTargetMembershipDemographics = relationship("TargetMembershipDemographic", back_populates="updatedBy", foreign_keys="TargetMembershipDemographic.updated_by")

    # Target membership demographic mappings
    createdTargetMembershipDemographicMappings = relationship("TargetMembershipDemographicMapping", back_populates="createdBy", foreign_keys="TargetMembershipDemographicMapping.created_by")
    updatedTargetMembershipDemographicMappings = relationship("TargetMembershipDemographicMapping", back_populates="updatedBy", foreign_keys="TargetMembershipDemographicMapping.updated_by")

    # Campaigns & improvements
    createdPreviousCampaigns = relationship("PreviousCampaigns", back_populates="createdBy", foreign_keys="PreviousCampaigns.created_by")
    updatedPreviousCampaigns = relationship("PreviousCampaigns", back_populates="updatedBy", foreign_keys="PreviousCampaigns.updated_by")
    createdAreasToImprove = relationship("AreasToImprove", back_populates="createdBy", foreign_keys="AreasToImprove.created_by")
    updatedAreasToImprove = relationship("AreasToImprove", back_populates="updatedBy", foreign_keys="AreasToImprove.updated_by")
    createdUniquePhrases = relationship("UniquePhrases", back_populates="createdBy", foreign_keys="UniquePhrases.created_by")
    updatedUniquePhrases = relationship("UniquePhrases", back_populates="updatedBy", foreign_keys="UniquePhrases.updated_by")
    createdToneOfCommunications = relationship("ToneOfCommunication", back_populates="createdBy", foreign_keys="ToneOfCommunication.created_by")
    updatedToneOfCommunications = relationship("ToneOfCommunication", back_populates="updatedBy", foreign_keys="ToneOfCommunication.updated_by")

    # Membership accounts
    createdAccountMembership = relationship("MembershipAccounts", back_populates="createdBy", foreign_keys="MembershipAccounts.created_by")
    updatedAccountMembership = relationship("MembershipAccounts", back_populates="updatedBy", foreign_keys="MembershipAccounts.updated_by")
    deletedAccountMembership = relationship("MembershipAccounts", back_populates="deletedBy", foreign_keys="MembershipAccounts.deleted_by")

    # Introductory offers
    createdIntroductoryOffers = relationship("IntroductoryOffer", back_populates="createdBy", foreign_keys="IntroductoryOffer.created_by")
    updatedIntroductoryOffers = relationship("IntroductoryOffer", back_populates="updatedBy", foreign_keys="IntroductoryOffer.updated_by")

    # Incentive discounts
    createdIncentiveDiscounts = relationship("IncentiveDiscount", back_populates="createdBy", foreign_keys="IncentiveDiscount.created_by")
    updatedIncentiveDiscounts = relationship("IncentiveDiscount", back_populates="updatedBy", foreign_keys="IncentiveDiscount.updated_by")

    # Offers/services
    createdOffersServices = relationship("OfferAndService", back_populates="createdBy", foreign_keys="OfferAndService.created_by")
    updatedOffersServices = relationship("OfferAndService", back_populates="updatedBy", foreign_keys="OfferAndService.updated_by")

    # State messages
    createdStateMessages = relationship("StateMessages", back_populates="createdBy", foreign_keys="StateMessages.created_by")
    updatedStateMessages = relationship("StateMessages", back_populates="updatedBy", foreign_keys="StateMessages.updated_by")

    # Class packs
    createdAccountClassPack = relationship("ClassPackAccounts", back_populates="createdBy", foreign_keys="ClassPackAccounts.created_by")
    updatedAccountClassPack = relationship("ClassPackAccounts", back_populates="updatedBy", foreign_keys="ClassPackAccounts.updated_by")
    deletedAccountClassPack = relationship("ClassPackAccounts", back_populates="deletedBy", foreign_keys="ClassPackAccounts.deleted_by")

    # User subscriptions
    userSubscriptions = relationship("UserSubscription", back_populates="user")

    # Broadcasts
    createdBroadcast = relationship("Broadcasts", back_populates="createdBy", foreign_keys="Broadcasts.created_by")
    updatedBroadcast = relationship("Broadcasts", back_populates="updatedBy", foreign_keys="Broadcasts.updated_by")
    deletedBroadcast = relationship("Broadcasts", back_populates="deletedBy", foreign_keys="Broadcasts.deleted_by")

    # Broadcast recipients
    createdBroadcastRecipients = relationship("BroadcastRecipients", back_populates="createdBy", foreign_keys="BroadcastRecipients.created_by")
    updatedBroadcastRecipients = relationship("BroadcastRecipients", back_populates="updatedBy", foreign_keys="BroadcastRecipients.updated_by")

    # Popular topics
    createdPopularTopics = relationship("PopularTopics", back_populates="createdBy", foreign_keys="PopularTopics.created_by")
    updatedPopularTopics = relationship("PopularTopics", back_populates="updatedBy", foreign_keys="PopularTopics.updated_by")

    # Faqs
    createdFaqs = relationship("Faqs", back_populates="createdBy", foreign_keys="Faqs.created_by")
    updatedFaqs = relationship("Faqs", back_populates="updatedBy", foreign_keys="Faqs.updated_by")

    # Chat threads
    chatThreads = relationship("ChatThreads", back_populates="user", foreign_keys="ChatThreads.user_id")
    createdChatThreads = relationship("ChatThreads", back_populates="createdBy", foreign_keys="ChatThreads.created_by")
    updatedChatThreads = relationship("ChatThreads", back_populates="updatedBy", foreign_keys="ChatThreads.updated_by")

    # Chat participants
    chatThreadParticipants = relationship("ChatThreadParticipants", back_populates="user", foreign_keys="ChatThreadParticipants.user_id")
    createdChatThreadParticipants = relationship("ChatThreadParticipants", back_populates="createdBy", foreign_keys="ChatThreadParticipants.created_by")
    updatedChatThreadParticipants = relationship("ChatThreadParticipants", back_populates="updatedBy", foreign_keys="ChatThreadParticipants.updated_by")

    # Chat messages
    sentMessages = relationship("ChatMessages", back_populates="sender", foreign_keys="ChatMessages.sender_id")
    createdChatMessages = relationship("ChatMessages", back_populates="createdBy", foreign_keys="ChatMessages.created_by")
    deletedChatMessages = relationship("ChatMessages", back_populates="deletedBy", foreign_keys="ChatMessages.deleted_by")
    updatedChatMessages = relationship("ChatMessages", back_populates="updatedBy", foreign_keys="ChatMessages.updated_by")

    # AtmosphereOptions
    createdAtmosphereOptions = relationship("AtmosphereOptions", back_populates="createdBy", foreign_keys="[AtmosphereOptions.created_by]")
    updatedAtmosphereOptions = relationship("AtmosphereOptions", back_populates="updatedBy", foreign_keys="[AtmosphereOptions.updated_by]")
    createdAtmosphereOptionsAccountsMapping = relationship("AtmosphereOptionsAccountsMapping", back_populates="createdBy", foreign_keys="[AtmosphereOptionsAccountsMapping.created_by]")

    # ClassPackAccounts
    createdAccountClassPack = relationship("ClassPackAccounts", back_populates="createdBy", foreign_keys="[ClassPackAccounts.created_by]")
    updatedAccountClassPack = relationship("ClassPackAccounts", back_populates="updatedBy", foreign_keys="[ClassPackAccounts.updated_by]")
    deletedAccountClassPack = relationship("ClassPackAccounts", back_populates="deletedBy", foreign_keys="[ClassPackAccounts.deleted_by]")

    # KeySellingPoints
    createdKeySellingPoints = relationship("KeySellingPoints", back_populates="createdBy", foreign_keys="[KeySellingPoints.created_by]")
    updatedKeySellingPoints = relationship("KeySellingPoints", back_populates="updatedBy", foreign_keys="[KeySellingPoints.updated_by]")
    createdKeySellingPointsAccountsMapping = relationship("KeySellingPointsAccountsMapping", back_populates="createdBy", foreign_keys="[KeySellingPointsAccountsMapping.created_by]")

    # MembershipAccounts
    createdAccountMembership = relationship("MembershipAccounts", back_populates="createdBy", foreign_keys="[MembershipAccounts.created_by]")
    updatedAccountMembership = relationship("MembershipAccounts", back_populates="updatedBy", foreign_keys="[MembershipAccounts.updated_by]")
    deletedAccountMembership = relationship("MembershipAccounts", back_populates="deletedBy", foreign_keys="[MembershipAccounts.deleted_by]")

    # PaymentSetup
    paymentSetups = relationship("PaymentSetup", back_populates="user")

    # SelfRealizations
    createdSelfRealizations = relationship("SelfRealizations", back_populates="createdBy", foreign_keys="[SelfRealizations.created_by]")
    updatedSelfRealizations = relationship("SelfRealizations", back_populates="updatedBy", foreign_keys="[SelfRealizations.updated_by]")
    createdSelfRealizationMappings = relationship("SelfRealizationsAccountsMapping", back_populates="createdBy", foreign_keys="[SelfRealizationsAccountsMapping.created_by]")
    updatedSelfRealizationMappings = relationship("SelfRealizationsAccountsMapping", back_populates="updatedBy", foreign_keys="[SelfRealizationsAccountsMapping.updated_by]")

    # SubscriptionPlan
    createdSubscriptionPlans = relationship("SubscriptionPlan", back_populates="createdBy", foreign_keys="[SubscriptionPlan.created_by]")
    updatedSubscriptionPlans = relationship("SubscriptionPlan", back_populates="updatedBy", foreign_keys="[SubscriptionPlan.updated_by]")

    # TargetMembershipDemographics
    createdTargetMembershipDemographics = relationship("TargetMembershipDemographic", back_populates="createdBy", foreign_keys="[TargetMembershipDemographic.created_by]")
    updatedTargetMembershipDemographics = relationship("TargetMembershipDemographic", back_populates="updatedBy", foreign_keys="[TargetMembershipDemographic.updated_by]")
    createdTargetMembershipDemographicMappings = relationship("TargetMembershipDemographicMapping", back_populates="createdBy", foreign_keys="[TargetMembershipDemographicMapping.created_by]")
    updatedTargetMembershipDemographicMappings = relationship("TargetMembershipDemographicMapping", back_populates="updatedBy", foreign_keys="[TargetMembershipDemographicMapping.updated_by]")

    # UserSubscriptions
    userSubscriptions = relationship("UserSubscription", back_populates="user")


    # Message tags
    taggedInMessages = relationship("MessageTags", back_populates="taggedUser", foreign_keys="MessageTags.tagged_user_id")

    createdCreditTransactionsTesting = relationship("CreditTransactionsTesting", back_populates="createdBy", foreign_keys="[CreditTransactionsTesting.created_by]")
    updatedCreditTransactionsTesting = relationship("CreditTransactionsTesting", back_populates="updatedBy", foreign_keys="[CreditTransactionsTesting.updated_by]")

    createdCreditTransactionsOrdersTesting = relationship("CreditTransactionsOrdersTesting", back_populates="createdBy", foreign_keys="[CreditTransactionsOrdersTesting.created_by]")
    updatedCreditTransactionsOrdersTesting = relationship("CreditTransactionsOrdersTesting", back_populates="updatedBy", foreign_keys="[CreditTransactionsOrdersTesting.updated_by]")

    createdMembershipTransactionsOrdersTesting = relationship("MembershipTransactionsOrdersTesting", back_populates="createdBy", foreign_keys="[MembershipTransactionsOrdersTesting.created_by]")
    updatedMembershipTransactionsOrdersTesting = relationship("MembershipTransactionsOrdersTesting", back_populates="updatedBy", foreign_keys="[MembershipTransactionsOrdersTesting.updated_by]")

    createdMembershipInstancesTesting = relationship("MembershipInstancesTesting", back_populates="createdBy", foreign_keys="[MembershipInstancesTesting.created_by]")
    updatedMembershipInstancesTesting = relationship("MembershipInstancesTesting", back_populates="updatedBy", foreign_keys="[MembershipInstancesTesting.updated_by]")

    createdMembershipTransactionsTesting = relationship("MembershipTransactionsTesting", back_populates="createdBy", foreign_keys="[MembershipTransactionsTesting.created_by]")
    updatedMembershipTransactionsTesting = relationship("MembershipTransactionsTesting", back_populates="updatedBy", foreign_keys="[MembershipTransactionsTesting.updated_by]")



    __table_args__ = (
        {'extend_existing': True}
    )


# ==========================================================
# ACCOUNTS — fully rewired
# ==========================================================
class Accounts(Base):
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_name = Column(String(100), nullable=False)
    city = Column(String(60))
    address = Column(String(255))
    entity = Column(ENUM(Entity, name="entity_enum"))
    crm_integration_id = Column(Integer, ForeignKey("crm_integrations.id"), nullable=False, server_default="1")
    status = Column(ENUM(Status, name="status_enum"), server_default=Status.ONBOARDING.value, nullable=False)
    on_boarding_step = Column(Integer, server_default="1")
    site_url = Column(String(255))
    integration_id = Column(String(60))
    integration_name = Column(String(60))
    crm_config = Column(JSON)
    ez_texting_id = Column(Integer, ForeignKey("eztexting_account.id", ondelete="CASCADE"))
    is_syncing = Column(Boolean, server_default="false")
    last_sync_time = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))
    callback_url = Column(String(255))
    callback_secret = Column(String(255))
    personality = Column(ENUM(Personality, name="personality_enum"))
    crm_api_end_point = Column(String)
    password = Column(JSON)
    username = Column(String)

    # Relationships
    integration = relationship("CrmIntegrations", back_populates="accounts")
    eztexting_account = relationship("EztextingAccount", back_populates="accounts")
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="accountsCreated", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="accountsUpdated", foreign_keys=[updated_by])
    deletedBy = relationship("models.sql_alchemy_models.Users", back_populates="accountsDeleted", foreign_keys=[deleted_by])
    accounts_users_mapping = relationship("AccountsUsersMapping", back_populates="account")

    # Link to other entities
    contact_groups = relationship("ContactGroup", back_populates="account")
    contacts = relationship("Contact", back_populates="account")
    states = relationship("States", back_populates="account")
    eztexting_messages = relationship("EztextingMessages", back_populates="account")
    batch_processing_state = relationship("BatchProcessingState", back_populates="account")
    customers = relationship("Customers", back_populates="account")
    areas_to_improve = relationship("AreasToImprove", back_populates="account")
    unique_phrases = relationship("UniquePhrases", back_populates="account")
    tone_of_communication_mapping = relationship("ToneOfCommunicationMapping", back_populates="account")
    introductory_offer = relationship("IntroductoryOffer", back_populates="account")
    incentive_discount = relationship("IncentiveDiscount", back_populates="account")
    offer_and_service = relationship("OfferAndService", back_populates="account")
    state_messages = relationship("StateMessages", back_populates="account")
    previous_campaigns = relationship("PreviousCampaigns", back_populates="account")
    broadcast = relationship("Broadcasts", back_populates="account")
    # broadcast_recipients relationship removed due to missing foreign key
    accountMemberships = relationship("MembershipAccounts", back_populates="account")
    accountClassPacks = relationship("ClassPackAccounts", back_populates="account")
    account_faqs = relationship("AccountFaqs", back_populates="account")
    notifications = relationship("Notifications", back_populates="account")
    email_configurations = relationship("EmailConfigurations", back_populates="account", uselist=False)

    credit_transactions_testing = relationship("CreditTransactionsTesting", back_populates="account")
    credit_transactions_orders_testing = relationship("CreditTransactionsOrdersTesting", back_populates="account")
    membership_transactions_orders_testing = relationship("MembershipTransactionsOrdersTesting", back_populates="account")
    membership_instances_testing = relationship("MembershipInstancesTesting", back_populates="account")
    membership_transactions_testing = relationship("MembershipTransactionsTesting", back_populates="account")
    eztexting_message_verification = relationship("EztextingMessageVerification", back_populates="account")

    # AtmosphereOptions mapping table
    atmosphere_options_mapping = relationship("AtmosphereOptionsAccountsMapping", back_populates="account")

    # ClassPackAccounts
    accountClassPacks = relationship("ClassPackAccounts", back_populates="account")

    # EmailConfigurations (one to one)
    email_configurations = relationship("EmailConfigurations", back_populates="account", uselist=False)

    # KeySellingPoints mapping table
    key_selling_points_mapping = relationship("KeySellingPointsAccountsMapping", back_populates="account")

    # MembershipAccounts
    accountMemberships = relationship("MembershipAccounts", back_populates="account")

    # SelfRealizations mapping table
    self_realizations_mapping = relationship("SelfRealizationsAccountsMapping", back_populates="account")

    # Subscription plans
    subscriptionPlans = relationship("SubscriptionPlan", back_populates="account")

    # TargetMembershipDemographic mapping table
    target_membership_demographic_mapping = relationship("TargetMembershipDemographicMapping", back_populates="account")



    __table_args__ = (
        Index("ix_accounts_created_by", "created_by"),
        Index("ix_accounts_updated_by", "updated_by"),
        Index("ix_accounts_deleted_by", "deleted_by"),
        Index("ix_accounts_ez_texting_id", "ez_texting_id"),
        {'extend_existing': True}
    )


# ==========================================================
# CRM INTEGRATIONS
# ==========================================================
class CrmIntegrations(Base):
    __tablename__ = "crm_integrations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    status = Column(Boolean, server_default="true", nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    accounts = relationship("Accounts", back_populates="integration")


# ==========================================================
# EZTEXTING ACCOUNT
# ==========================================================
class EztextingAccount(Base):
    __tablename__ = "eztexting_account"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False, unique=True)
    password = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    accounts = relationship("Accounts", back_populates="eztexting_account")


# ==========================================================
# ACCOUNTS_USERS_MAPPING
# ==========================================================
class AccountsUsersMapping(Base):
    __tablename__ = "accounts_users_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="accounts_users_mapping")
    users = relationship("models.sql_alchemy_models.Users", back_populates="accounts_users_mapping", foreign_keys=[user_id])
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdAccounts", foreign_keys=[created_by])

    __table_args__ = (
        UniqueConstraint("account_id", "user_id", name="uq_account_user"),
        Index("ix_accounts_users_mapping_account_id", "account_id"),
        Index("ix_accounts_users_mapping_user_id", "user_id"),
    )

# ==========================================================
# CONTACT
# ==========================================================
class Contact(Base):
    __tablename__ = "contact"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    phone_number = Column(String(16), nullable=False)
    first_name = Column(String(128), nullable=False)
    last_name = Column(String(128), nullable=False)
    email = Column(String(255), nullable=False)
    status = Column(ENUM(ContactStatus, name="contact_status_enum"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    customer_id = Column(Integer)  # No FK defined in Prisma

    account = relationship("Accounts", back_populates="contacts")
    memberships = relationship("ContactGroupMembership", back_populates="contact")


# ==========================================================
# CONTACT GROUP
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

    memberships = relationship("ContactGroupMembership", back_populates="contact_group")
    account = relationship("Accounts", back_populates="contact_groups")
    states = relationship("States", back_populates="contact_group")

    __table_args__ = (
        Index("ix_contact_group_account_id", "account_id"),
        UniqueConstraint("eztexting_group_id", "account_id", name="uq_contact_group_per_account"),
    )


# ==========================================================
# CONTACT GROUP MEMBERSHIP
# ==========================================================
class ContactGroupMembership(Base):
    __tablename__ = "contact_group_membership"

    id = Column(Integer, primary_key=True, autoincrement=True)
    contact_id = Column(Integer, ForeignKey("contact.id", ondelete="CASCADE"), nullable=False)
    contact_group_id = Column(Integer, ForeignKey("contact_group.id", ondelete="CASCADE"), nullable=False)
    added_at = Column(DateTime, server_default=func.now(), nullable=False)
    removed_at = Column(DateTime)
    status = Column(ENUM(MembershipStatus, name="membership_status_enum"), nullable=False)

    contact_group = relationship("ContactGroup", back_populates="memberships")
    contact = relationship("Contact", back_populates="memberships")

    __table_args__ = (
        Index("ix_contact_group_membership_contact_group_id", "contact_group_id"),
        Index("ix_contact_group_membership_contact_id_group_id", "contact_id", "contact_group_id"),
    )



# ==========================================================
# STATES
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
    status = Column(ENUM(StateStatus, name="state_status_enum"), 
                    server_default=StateStatus.WAITING_FOR_APPROVAL.value, 
                    nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), 
                        onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    # Relations
    account = relationship("Accounts", back_populates="states")
    contact_group = relationship("ContactGroup", back_populates="states")
    createdBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[created_by], back_populates="statesCreated")
    updatedBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[updated_by], back_populates="statesUpdated")
    deletedBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[deleted_by], back_populates="statesDeleted")

    stateTransitionsFrom = relationship("StateTransitions", 
                                        back_populates="stateId", 
                                        foreign_keys="StateTransitions.state_id")
    stateTransitionsTo = relationship("StateTransitions", 
                                      back_populates="nextState", 
                                      foreign_keys="StateTransitions.next_state_id")
    states_conditions = relationship("StatesConditions", back_populates="StateConditions")
    customers = relationship("Customers", back_populates="states")
    state_messages = relationship("StateMessages", back_populates="stateMessages")
    state_history = relationship("StateHistory", back_populates="stateHistory")
    states = relationship("BroadcastRecipients", back_populates="states")

    __table_args__ = (
        Index("ix_states_created_by", "created_by"),
        Index("ix_states_updated_by", "updated_by"),
        Index("ix_states_deleted_by", "deleted_by"),
    )


# ==========================================================
# STATE TRANSITIONS
# ==========================================================
class StateTransitions(Base):
    __tablename__ = "state_transitions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_id = Column(Integer, ForeignKey("states.id", ondelete="CASCADE"), nullable=False)
    next_state_id = Column(Integer, ForeignKey("states.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), 
                        onupdate=func.now(), nullable=False)

    # Self-referencing state relations
    stateId = relationship("States", back_populates="stateTransitionsFrom", foreign_keys=[state_id])
    nextState = relationship("States", back_populates="stateTransitionsTo", foreign_keys=[next_state_id])

    __table_args__ = (
        Index("ix_state_transitions_state_id_next_state_id", "state_id", "next_state_id"),
    )


# ==========================================================
# STATES CONDITIONS
# ==========================================================
class StatesConditions(Base):
    __tablename__ = "states_conditions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_id = Column(Integer, ForeignKey("states.id", ondelete="CASCADE"), nullable=False)
    data_category = Column(String(100), nullable=False)
    field = Column(String(100), nullable=False)
    condition = Column(ENUM(Condition, name="condition_enum"), nullable=False)
    value = Column(String(100))
    logical_operator = Column(ENUM(LogicalOperator, name="logical_operator_enum"))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), 
                        onupdate=func.now(), nullable=False)

    StateConditions = relationship("States", back_populates="states_conditions")

    __table_args__ = (
        Index("ix_states_conditions_state_id", "state_id"),
    )



# ==========================================================
# EZTEXTING_MESSAGES
# ==========================================================
# ==========================================================
# EZTEXTING_MESSAGES
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
    escalated_answered_for = Column(JSON, server_default="[]", nullable=False)
    escalated_answered_at = Column(DateTime)
    message_group_id = Column(String(255))
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    customer_ref_id = Column(Integer, ForeignKey("customers_rmtest.id", ondelete="SET NULL"))
    customer_id = Column(String(255))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    account = relationship("Accounts", back_populates="eztexting_messages")
    customer = relationship("Customers", back_populates="messages")

    __table_args__ = (
        Index("ix_ezm_account_id", "account_id"),
        Index("ix_ezm_customer_id", "customer_id"),
        Index("ix_ezm_contact_number", "contact_number"),
        Index("ix_ezm_status", "status"),
        Index("ix_ezm_sent_at", "sent_at"),
    )


# ==========================================================
# BATCH_PROCESSING_STATE
# ==========================================================
class BatchProcessingState(Base):
    __tablename__ = "batch_processing_state"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_type = Column(String(255), nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    last_page = Column(Integer, nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    account = relationship("Accounts", back_populates="batch_processing_state")

    __table_args__ = (
        UniqueConstraint("batch_type", "account_id", name="uq_batch_type_account_id"),
    )


# ==========================================================
# CUSTOMERS
# ==========================================================
class Customers(Base):
    __tablename__ = "customers_rmtest"

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
    is_opted_in_to_sms = Column(Boolean, server_default="false")
    completed_class_count = Column(Integer, server_default="0")
    state_id = Column(Integer, ForeignKey("states.id"))
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

    # Relationships
    account = relationship("Accounts", back_populates="customers")
    states = relationship("States", back_populates="customers")
    credit_transactions = relationship(
        "CreditTransactions",
        back_populates="customer",
        foreign_keys=[CreditTransactions.customer_ref_id]  # needs real column object
    )
    credit_transactions_orders = relationship("CreditTransactionsOrders", back_populates="creditTransactionsOrdersCustomer")
    membership_transactions = relationship("MembershipTransactions", back_populates="creditTransactionsCustomer")
    orders = relationship("Orders", back_populates="orderCustomer")
    reservations = relationship("Reservations", back_populates="reservationCustomer")
    messages = relationship(
        "EztextingMessages",
        back_populates="customer",
        foreign_keys=[EztextingMessages.customer_ref_id]  
    )
    stateHistory = relationship("StateHistory", back_populates="customers_rmtest")

    __table_args__ = (
        Index("ix_customers_account_id", "account_id"),
        Index("ix_customers_city", "city"),
        Index("ix_customers_state_province", "state_province"),
        Index("ix_customers_email", "email"),
        Index("ix_customers_phone_number", "phone_number"),
    )


# ==========================================================
# ORDERS
# ==========================================================
# ==========================================================
# ORDERS
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
    account_id = Column(Integer, ForeignKey("accounts.id"))
    customer_id = Column(String(255))
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    orderLines = relationship("OrderLines", back_populates="order")
    orderCustomer = relationship("Customers", back_populates="orders", foreign_keys=[customer_ref_id])
    account = relationship("Accounts", back_populates="orders")

    __table_args__ = (
        Index("ix_orders_order_id", "order_id"),
        Index("ix_orders_location", "location"),
        Index("ix_orders_status", "status"),
    )


# ==========================================================
# ORDER_LINES
# ==========================================================
class OrderLines(Base):
    __tablename__ = "order_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_line_id = Column(String(64))
    order_id = Column(String)
    order_ref_id = Column(Integer, ForeignKey("orders.id"))
    account_id = Column(Integer, ForeignKey("accounts.id"))
    transaction_type = Column(String(255))
    location = Column(String(255))
    credit_transactions_id = Column(Integer)
    credit_transactions_ref_id = Column(Integer, unique=True)
    membership_transactions_id = Column(Integer)
    membership_transactions_ref_id = Column(Integer, unique=True)
    title = Column(String(255))
    processed_by = Column(Boolean, server_default="false", nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    order = relationship("Orders", back_populates="orderLines", foreign_keys=[order_ref_id])
    account = relationship("Accounts", back_populates="order_lines")
    creditTransactionOrders = relationship("CreditTransactionsOrders", back_populates="creditTransactionOrders")
    membershipTransactionOrders = relationship("MembershipTransactionsOrders", back_populates="membershipTransactionOrders")

    __table_args__ = (
        Index("ix_order_lines_order_id", "order_id"),
        Index("ix_order_lines_location", "location"),
    )


# ==========================================================
# RESERVATIONS
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
    account_id = Column(Integer, ForeignKey("accounts.id"))
    first_timer = Column(Boolean, server_default="false", nullable=False)
    reservation_type = Column(String(64))
    location = Column(Integer)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    account = relationship("Accounts", back_populates="reservations")
    creditTransaction = relationship("CreditTransactions", back_populates="creditTransactionReservations", foreign_keys=[credit_transactions_ref_id])
    membershipTransaction = relationship("MembershipTransactions", back_populates="membershipTransactionReservations", foreign_keys=[membership_transactions_ref_id])
    reservationCustomer = relationship("Customers", back_populates="reservations", foreign_keys=[customer_ref_id])
    classSession = relationship("ClassSessions", back_populates="reservations", foreign_keys=[class_session_ref_id])

    __table_args__ = (
        Index("ix_reservations_reservations_id", "reservations_id"),
        Index("ix_reservations_location", "location"),
    )


# ==========================================================
# CLASS_SESSIONS
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
    account_id = Column(Integer, ForeignKey("accounts.id"))
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    account = relationship("Accounts", back_populates="class_sessions")
    reservations = relationship("Reservations", back_populates="classSession")

    __table_args__ = (
        Index("ix_class_sessions_class_session_id", "class_session_id"),
        Index("ix_class_sessions_location", "location"),
    )


# ==========================================================
# CREDIT_TRANSACTIONS
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
    customer_ref_id = Column(Integer, ForeignKey("customers_rmtest.id"))
    customer_id = Column(String(255))
    account_id = Column(Integer, ForeignKey("accounts.id"))
    location = Column(Integer)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    account = relationship("Accounts", back_populates="credit_transactions")
    creditTransactionReservations = relationship("Reservations", back_populates="creditTransaction", uselist=False, foreign_keys="[Reservations.credit_transactions_ref_id]")
    creditTransactionsCustomer = relationship("Customers", back_populates="credit_transactions", foreign_keys=[customer_ref_id])

    __table_args__ = (
        Index("ix_credit_transactions_credit_transactions_id", "credit_transactions_id"),
        Index("ix_credit_transactions_location", "location"),
    )


# ==========================================================
# CREDIT_TRANSACTIONS_ORDERS
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
    account_id = Column(Integer, ForeignKey("accounts.id"))
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    account = relationship("Accounts", back_populates="credit_transactions_orders")
    creditTransactionsOrdersCustomer = relationship("Customers", back_populates="credit_transactions_orders", foreign_keys=[customer_ref_id])
    creditTransactionOrders = relationship("OrderLines", back_populates="creditTransactionOrders", uselist=False)

    __table_args__ = (
        Index("ix_credit_transactions_orders_credit_transactions_id", "credit_transactions_id"),
        Index("ix_credit_transactions_orders_location", "location"),
    )


# ==========================================================
# MEMBERSHIP_INSTANCES
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
    account_id = Column(Integer, ForeignKey("accounts.id"))
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="membership_instances")
    membershipTransactions = relationship("MembershipTransactions", back_populates="membershipInstance")
    membershipTransactionsOrders = relationship("MembershipTransactionsOrders", back_populates="membershipInstance")

    __table_args__ = (
        Index("ix_membership_instances_membership_instances_id", "membership_instances_id"),
        Index("ix_membership_instances_location", "location"),
    )


# ==========================================================
# MEMBERSHIP_TRANSACTIONS
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
    account_id = Column(Integer, ForeignKey("accounts.id"))
    location = Column(Integer)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    account = relationship("Accounts", back_populates="membership_transactions")
    creditTransactionsCustomer = relationship("Customers", back_populates="membership_transactions", foreign_keys=[customer_ref_id])
    membershipTransactionReservations = relationship("Reservations", back_populates="membershipTransaction", uselist=False, foreign_keys="[Reservations.membership_transactions_ref_id]")
    membershipInstance = relationship("MembershipInstances", back_populates="membershipTransactions")

    __table_args__ = (
        Index("ix_membership_transactions_membership_transactions_id", "membership_transactions_id"),
        Index("ix_membership_transactions_location", "location"),
    )


# ==========================================================
# MEMBERSHIP_TRANSACTIONS_ORDERS
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
    account_id = Column(Integer, ForeignKey("accounts.id"))
    location = Column(Integer)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer)
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer)

    account = relationship("Accounts", back_populates="membership_transactions_orders")
    membershipInstance = relationship("MembershipInstances", back_populates="membershipTransactionsOrders")
    membershipTransactionOrders = relationship("OrderLines", back_populates="membershipTransactionOrders", uselist=False)

    __table_args__ = (
        Index("ix_membership_transactions_orders_membership_transactions_id", "membership_transactions_id"),
        Index("ix_membership_transactions_orders_location", "location"),
    )


# ==========================================================
# AREAS_TO_IMPROVE
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
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdAreasToImprove", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedAreasToImprove", foreign_keys=[updated_by])


# ==========================================================
# UNIQUE_PHRASES
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
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdUniquePhrases", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedUniquePhrases", foreign_keys=[updated_by])


# ==========================================================
# TONE_OF_COMMUNICATION
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
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdToneOfCommunications", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedToneOfCommunications", foreign_keys=[updated_by])


# ==========================================================
# TONE_OF_COMMUNICATION_MAPPING
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
# INTRODUCTORY_OFFER
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
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdIntroductoryOffers", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedIntroductoryOffers", foreign_keys=[updated_by])


# ==========================================================
# INCENTIVE_DISCOUNT
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
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdIncentiveDiscounts", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedIncentiveDiscounts", foreign_keys=[updated_by])


# ==========================================================
# OFFER_AND_SERVICE
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
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdOffersServices", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedOffersServices", foreign_keys=[updated_by])


# ==========================================================
# STATE_MESSAGES
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
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdStateMessages", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedStateMessages", foreign_keys=[updated_by])


# ==========================================================
# STATE_HISTORY
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
# BROADCASTS
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
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdBroadcast", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedBroadcast", foreign_keys=[updated_by])
    deletedBy = relationship("models.sql_alchemy_models.Users", back_populates="deletedBroadcast", foreign_keys=[deleted_by])
    broadcast_recipients = relationship("BroadcastRecipients", back_populates="broadcast")


# ==========================================================
# BROADCAST_RECIPIENTS
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
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdBroadcastRecipients", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedBroadcastRecipients", foreign_keys=[updated_by])


# ==========================================================
# PREVIOUS_CAMPAIGNS
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
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdPreviousCampaigns", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedPreviousCampaigns", foreign_keys=[updated_by])



# ==========================================================
# FAQS
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

    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdFaqs", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedFaqs", foreign_keys=[updated_by])
    topic = relationship("PopularTopics", back_populates="faqs")


# ==========================================================
# POPULAR_TOPICS
# ==========================================================
class PopularTopics(Base):
    __tablename__ = "popular_topics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))

    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdPopularTopics", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedPopularTopics", foreign_keys=[updated_by])
    faqs = relationship("Faqs", back_populates="topic")


# ==========================================================
# ACCOUNT_FAQS
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
# CHAT_THREADS
# ==========================================================
class ChatThreads(Base):
    __tablename__ = "chat_threads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"))
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))
    last_seen_message_id = Column(Integer)

    account = relationship("Accounts", back_populates="chat_threads")
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdChatThreads", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedChatThreads", foreign_keys=[updated_by])
    user = relationship("models.sql_alchemy_models.Users", back_populates="chatThreads", foreign_keys=[user_id])
    participants = relationship("ChatThreadParticipants", back_populates="thread")
    messages = relationship("ChatMessages", back_populates="thread")


# ==========================================================
# CHAT_THREAD_PARTICIPANTS
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

    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdChatThreadParticipants", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedChatThreadParticipants", foreign_keys=[updated_by])
    thread = relationship("ChatThreads", back_populates="participants")
    user = relationship("models.sql_alchemy_models.Users", back_populates="chatThreadParticipants", foreign_keys=[user_id])


# ==========================================================
# CHAT_MESSAGES
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

    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdChatMessages", foreign_keys=[created_by])
    deletedBy = relationship("models.sql_alchemy_models.Users", back_populates="deletedChatMessages", foreign_keys=[deleted_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedChatMessages", foreign_keys=[updated_by])
    thread = relationship("ChatThreads", back_populates="messages")
    sender = relationship("models.sql_alchemy_models.Users", back_populates="sentMessages", foreign_keys=[sender_id])
    messageTags = relationship("MessageTags", back_populates="message")


# ==========================================================
# MESSAGE_TAGS
# ==========================================================
class MessageTags(Base):
    __tablename__ = "message_tags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(Integer, ForeignKey("chat_messages.id", ondelete="CASCADE"), nullable=False)
    tagged_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    message = relationship("ChatMessages", back_populates="messageTags")
    taggedUser = relationship("models.sql_alchemy_models.Users", back_populates="taggedInMessages", foreign_keys=[tagged_user_id])


# ==========================================================
# NOTIFICATIONS
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
# EZTEXTING_TRANSACTION_HISTORY
# ==========================================================
class EztextingTransactionHistory(Base):
    __tablename__ = "eztexting_transaction_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"))
    stripe_customer_id = Column(String)
    payment_intent_id = Column(String)
    last_error_message = Column(String)
    credit_status = Column(ENUM(CreditStatus, name="creditstatus_enum"), nullable=False)
    invoice_id = Column(String)
    credit_amount = Column(Float)
    credit_value = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    account = relationship("Accounts", back_populates="eztexting_transaction_history")


# ==========================================================
# MINDBODY MODELS
# ==========================================================
class MindbodyLocations(Base):
    __tablename__ = "mindbody_locations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"))
    address = Column(String)
    city = Column(String)
    state_prov_code = Column(String)
    postal_code = Column(String)
    location_id = Column(Integer)
    location_name = Column(String)
    site_id = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)

    account = relationship("Accounts", back_populates="mindbody_locations")


class MindbodySites(Base):
    __tablename__ = "mindbody_sites"
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"))
    time_zone = Column(String)
    site_id = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)

    account = relationship("Accounts", back_populates="mindbody_sites")


class MindbodySaleServices(Base):
    __tablename__ = "mindbody_sale_services"
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"))
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

    account = relationship("Accounts", back_populates="mindbody_sale_services")


class MindbodySitesPrograms(Base):
    __tablename__ = "mindbody_sites_programs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"))
    program_id = Column(Integer)
    program_name = Column(String)
    schedule_type = Column(String)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)

    account = relationship("Accounts", back_populates="mindbody_sites_programs")


class MindbodySitesSessionTypes(Base):
    __tablename__ = "mindbody_sites_session_types"
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"))
    session_type_id = Column(Integer)
    session_type_name = Column(String)
    program_id = Column(Integer)
    category = Column(String)
    sub_category = Column(String)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer)

    account = relationship("Accounts", back_populates="mindbody_sites_session_types")


# ==========================================================
# CREDIT_TRANSACTIONS_TESTING
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
    account_id = Column(Integer, ForeignKey("accounts.id"))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="credit_transactions_testing")
    createdBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[created_by], back_populates="createdCreditTransactionsTesting")
    updatedBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[updated_by], back_populates="updatedCreditTransactionsTesting")


# ==========================================================
# CREDIT_TRANSACTIONS_ORDERS_TESTING
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
    account_id = Column(Integer, ForeignKey("accounts.id"))
    location = Column(Integer)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="credit_transactions_orders_testing")
    createdBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[created_by], back_populates="createdCreditTransactionsOrdersTesting")
    updatedBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[updated_by], back_populates="updatedCreditTransactionsOrdersTesting")


# ==========================================================
# MEMBERSHIP_TRANSACTIONS_ORDERS_TESTING
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
    account_id = Column(Integer, ForeignKey("accounts.id"))
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="membership_transactions_orders_testing")
    createdBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[created_by], back_populates="createdMembershipTransactionsOrdersTesting")
    updatedBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[updated_by], back_populates="updatedMembershipTransactionsOrdersTesting")


# ==========================================================
# MEMBERSHIP_INSTANCES_TESTING
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
    account_id = Column(Integer, ForeignKey("accounts.id"))
    next_charge_date = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="membership_instances_testing")
    createdBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[created_by], back_populates="createdMembershipInstancesTesting")
    updatedBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[updated_by], back_populates="updatedMembershipInstancesTesting")


# ==========================================================
# MEMBERSHIP_TRANSACTIONS_TESTING
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
    account_id = Column(Integer, ForeignKey("accounts.id"))
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now(), nullable=False)
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="membership_transactions_testing")
    createdBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[created_by], back_populates="createdMembershipTransactionsTesting")
    updatedBy = relationship("models.sql_alchemy_models.Users", foreign_keys=[updated_by], back_populates="updatedMembershipTransactionsTesting")


# ==========================================================
# EZTEXTING_MESSAGE_VERIFICATION
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


# ==========================================================
# ATMOSPHERE_OPTIONS
# ==========================================================
class AtmosphereOptions(Base):
    __tablename__ = "atmosphere_options"

    id = Column(Integer, primary_key=True, autoincrement=True)
    option_name = Column(String(255), nullable=False)
    description = Column(String)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))

    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdAtmosphereOptions", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedAtmosphereOptions", foreign_keys=[updated_by])

    accounts_mapping = relationship("AtmosphereOptionsAccountsMapping", back_populates="atmosphere_option")


# ==========================================================
# ATMOSPHERE_OPTIONS_ACCOUNTS_MAPPING
# ==========================================================
class AtmosphereOptionsAccountsMapping(Base):
    __tablename__ = "atmosphere_options_accounts_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    atmosphere_option_id = Column(Integer, ForeignKey("atmosphere_options.id"), nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))

    atmosphere_option = relationship("AtmosphereOptions", back_populates="accounts_mapping")
    account = relationship("Accounts", back_populates="atmosphere_options_mapping")
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdAtmosphereOptionsAccountsMapping", foreign_keys=[created_by])


# ==========================================================
# CLASS_PACK_ACCOUNTS
# ==========================================================
class ClassPackAccounts(Base):
    __tablename__ = "class_pack_accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"))
    description = Column(String)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="accountClassPacks")
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdAccountClassPack", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedAccountClassPack", foreign_keys=[updated_by])
    deletedBy = relationship("models.sql_alchemy_models.Users", back_populates="deletedAccountClassPack", foreign_keys=[deleted_by])


# ==========================================================
# EMAIL_CONFIGURATIONS
# ==========================================================
class EmailConfigurations(Base):
    __tablename__ = "email_configurations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), unique=True)
    email_service = Column(String)
    configuration = Column(JSON)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    account = relationship("Accounts", back_populates="email_configurations")


# ==========================================================
# KEY_SELLING_POINTS
# ==========================================================
class KeySellingPoints(Base):
    __tablename__ = "key_selling_points"

    id = Column(Integer, primary_key=True, autoincrement=True)
    point_text = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))

    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdKeySellingPoints", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedKeySellingPoints", foreign_keys=[updated_by])
    accounts_mapping = relationship("KeySellingPointsAccountsMapping", back_populates="key_selling_point")


# ==========================================================
# KEY_SELLING_POINTS_ACCOUNTS_MAPPING
# ==========================================================
class KeySellingPointsAccountsMapping(Base):
    __tablename__ = "key_selling_points_accounts_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key_selling_point_id = Column(Integer, ForeignKey("key_selling_points.id"), nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))

    key_selling_point = relationship("KeySellingPoints", back_populates="accounts_mapping")
    account = relationship("Accounts", back_populates="key_selling_points_mapping")
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdKeySellingPointsAccountsMapping", foreign_keys=[created_by])


# ==========================================================
# MEMBERSHIP_ACCOUNTS
# ==========================================================
class MembershipAccounts(Base):
    __tablename__ = "membership_accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"))
    membership_name = Column(String)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    account = relationship("Accounts", back_populates="accountMemberships")
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdAccountMembership", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedAccountMembership", foreign_keys=[updated_by])
    deletedBy = relationship("models.sql_alchemy_models.Users", back_populates="deletedAccountMembership", foreign_keys=[deleted_by])


# ==========================================================
# PAYMENT_SETUP
# ==========================================================
class PaymentSetup(Base):
    __tablename__ = "payment_setup"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    payment_data = Column(JSON)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    user = relationship("models.sql_alchemy_models.Users", back_populates="paymentSetups")


# ==========================================================
# SELF_REALIZATIONS
# ==========================================================
class SelfRealizations(Base):
    __tablename__ = "self_realizations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    realization_text = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))

    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdSelfRealizations", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedSelfRealizations", foreign_keys=[updated_by])
    mappings = relationship("SelfRealizationsAccountsMapping", back_populates="self_realization")


# ==========================================================
# SELF_REALIZATIONS_ACCOUNTS_MAPPING
# ==========================================================
class SelfRealizationsAccountsMapping(Base):
    __tablename__ = "self_realizations_accounts_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    self_realization_id = Column(Integer, ForeignKey("self_realizations.id"), nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))

    self_realization = relationship("SelfRealizations", back_populates="mappings")
    account = relationship("Accounts", back_populates="self_realizations_mapping")
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdSelfRealizationMappings", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedSelfRealizationMappings", foreign_keys=[updated_by])


# ==========================================================
# SUBSCRIPTION_PLAN
# ==========================================================
class SubscriptionPlan(Base):
    __tablename__ = "subscription_plan"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_name = Column(String)
    details = Column(JSON)
    account_id = Column(Integer, ForeignKey("accounts.id"))
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdSubscriptionPlans", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedSubscriptionPlans", foreign_keys=[updated_by])
    deletedBy = relationship("models.sql_alchemy_models.Users", back_populates="subscriptionPlansDeleted", foreign_keys=[deleted_by])
    account = relationship("Accounts", back_populates="subscriptionPlans")
    __tablename__ = "subscription_plan"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_name = Column(String)
    details = Column(JSON)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))
    deleted_at = Column(DateTime)
    deleted_by = Column(Integer, ForeignKey("users.id"))

    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdSubscriptionPlans", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedSubscriptionPlans", foreign_keys=[updated_by])


# ==========================================================
# TARGET_MEMBERSHIP_DEMOGRAPHIC
# ==========================================================
class TargetMembershipDemographic(Base):
    __tablename__ = "target_membership_demographic"

    id = Column(Integer, primary_key=True, autoincrement=True)
    demographic_name = Column(String)
    description = Column(String)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))

    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdTargetMembershipDemographics", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedTargetMembershipDemographics", foreign_keys=[updated_by])
    mappings = relationship("TargetMembershipDemographicMapping", back_populates="target_membership_demographic")


# ==========================================================
# TARGET_MEMBERSHIP_DEMOGRAPHIC_MAPPING
# ==========================================================
class TargetMembershipDemographicMapping(Base):
    __tablename__ = "target_membership_demographic_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    target_membership_demographic_id = Column(Integer, ForeignKey("target_membership_demographic.id"), nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"))

    target_membership_demographic = relationship("TargetMembershipDemographic", back_populates="mappings")
    account = relationship("Accounts", back_populates="target_membership_demographic_mapping")
    createdBy = relationship("models.sql_alchemy_models.Users", back_populates="createdTargetMembershipDemographicMappings", foreign_keys=[created_by])
    updatedBy = relationship("models.sql_alchemy_models.Users", back_populates="updatedTargetMembershipDemographicMappings", foreign_keys=[updated_by])


# ==========================================================
# USER_SUBSCRIPTION
# ==========================================================
class UserSubscription(Base):
    __tablename__ = "user_subscription"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    subscription_plan_id = Column(Integer, ForeignKey("subscription_plan.id"), nullable=False)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    status = Column(String)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    user = relationship("models.sql_alchemy_models.Users", back_populates="userSubscriptions")
    subscription_plan = relationship("SubscriptionPlan")
