
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timezone
from enum import Enum

# -------------------------
# Enums
# -------------------------
class Entity(str, Enum):
    BODY_BAR_PILATES = "BODY_BAR_PILATES"
    CROSSFIT = "CROSSFIT"

class Status(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    ONBOARDING = "ONBOARDING"
    WAITING_PAYMENT = "WAITING_PAYMENT"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    APPROVED = "APPROVED"

class Role(str, Enum):
    ACCOUNT_MANAGER = "ACCOUNT_MANAGER"
    SUPER_ADMIN = "SUPER_ADMIN"
    USERS = "USERS"

class StateStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    WAITING_FOR_APPROVAL = "WAITING_FOR_APPROVAL"

class SubscriptionStatusDb(str, Enum):
    ACTIVE = "ACTIVE"
    ENDED = "ENDED"
    CANCELED = "CANCELED"
    PAST_DUE = "PAST_DUE"
    TRIAL = "TRIAL"
    IN_GRACE_PERIOD = "IN_GRACE_PERIOD"

class CreditStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    INTERMEDIATE = "INTERMEDIATE"

class Personality(str, Enum):
    Never = "Never"
    Sometimes = "Sometimes"
    Often = "Often"

class ScheduleType(str, Enum):
    Immediately = "Immediately"
    Schedule = "Schedule"

class contact_status(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"

class membership_status(str, Enum):
    ACTIVE = "ACTIVE"
    REMOVED = "REMOVED"

class Condition(str, Enum):
    EQUALS = "EQUALS"
    NOT_EQUALS = "NOT_EQUALS"
    GREATER_THAN = "GREATER_THAN"
    LESS_THAN = "LESS_THAN"
    GREATER_THAN_EQUALS = "GREATER_THAN_EQUALS"
    LESS_THAN_EQUALS = "LESS_THAN_EQUALS"

class logical_operator(str, Enum):
    AND = "AND"
    OR = "OR"

class StateHistoryType(str, Enum):
    ADDED = "ADDED"
    REMOVED = "REMOVED"

# -------------------------
# MODELS
# -------------------------
class ContactGroup(BaseModel):
    id: int
    account_id: int
    eztexting_group_id: Optional[str] = None
    name: str
    note: Optional[str] = None
    strict_validation: Optional[bool] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    memberships: List['ContactGroupMembership'] = Field(default_factory=list)
    account: Optional['Account'] = None
    states: List['State'] = Field(default_factory=list)

    class Config:
        from_attributes = True


class Contact(BaseModel):
    id: int
    account_id: int
    phone_number: str
    first_name: str
    last_name: str
    email: str
    status: contact_status
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    customer_id: Optional[int] = None
    account: Optional['Account'] = None
    memberships: List['ContactGroupMembership'] = Field(default_factory=list)

    class Config:
        from_attributes = True


class ContactGroupMembership(BaseModel):
    id: int
    contact_id: int
    contact_group_id: int
    added_at: Optional[datetime] = None
    removed_at: Optional[datetime] = None
    status: membership_status
    contact_group: Optional['ContactGroup'] = None
    contact: Optional['Contact'] = None

    class Config:
        from_attributes = True


class CrmIntegration(BaseModel):
    id: int
    name: str
    status: Optional[bool] = True
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    accounts: List['Account'] = Field(default_factory=list)

    class Config:
        from_attributes = True


class AtmosphereOptions(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True


class KeySellingPoints(BaseModel):
    id: int
    name: str
    is_custom: bool
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    key_selling_points_accounts_mapping: List['KeySellingPointsAccountsMapping'] = Field(default_factory=list)

    class Config:
        from_attributes = True


class KeySellingPointsAccountsMapping(BaseModel):
    id: int
    account_id: int
    key_selling_points_id: int
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    account: Optional['Account'] = None
    keySellingPointDetails: Optional['KeySellingPoints'] = None

    class Config:
        from_attributes = True


class AtmosphereOptionsAccountsMapping(BaseModel):
    id: int
    account_id: int
    atmosphere_option_id: int
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    account: Optional['Account'] = None

    class Config:
        from_attributes = True

class MembershipAccounts(BaseModel):
    id: int
    account_id: int
    plan_name: str
    pricing: float
    registration_fees: float
    credit_allocated: str
    credit_roll_over: bool
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None
    deletedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class ClassPackAccounts(BaseModel):
    id: int
    account_id: int
    plan_name: str
    pricing: float
    no_credits: str
    credit_expiration: int
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None
    deletedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class Account(BaseModel):
    id: int
    account_name: str
    city: Optional[str] = None
    address: Optional[str] = None
    entity: Optional['Entity'] = None
    crm_integration_id: int
    status: 'Status' = 'Status.ONBOARDING'
    on_boarding_step: int = 1
    site_url: Optional[str] = None
    integration_id: Optional[str] = None
    integration_name: Optional[str] = None
    crm_config: Optional[str] = None
    ez_texting_id: Optional[int] = None
    is_syncing: Optional[bool] = False
    last_sync_time: Optional[datetime] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    callback_url: Optional[str] = None
    callback_secret: Optional[str] = None
    personality: Optional['Personality'] = None
    crm_api_end_point: Optional[str] = None
    password: Optional[str] = None
    username: Optional[str] = None

    eztexting_account: Optional['EztextingAccount'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None
    deletedBy: Optional['User'] = None
    integration: Optional['CrmIntegration'] = None

    accounts_users_mapping: List['AccountsUsersMapping'] = Field(default_factory=list)
    contact_groups: List['ContactGroup'] = Field(default_factory=list)
    contacts: List['Contact'] = Field(default_factory=list)
    states: List['State'] = Field(default_factory=list)
    batch_processing_state: List['BatchProcessingState'] = Field(default_factory=list)
    eztexting_messages: List['EztextingMessage'] = Field(default_factory=list)
    customers: List['Customer'] = Field(default_factory=list)
    email_configurations: Optional['EmailConfigurations'] = None
    broadcast: List['Broadcast'] = Field(default_factory=list)
    broadcast_recipients: List['BroadcastRecipients'] = Field(default_factory=list)
    payment_setup: Optional['PaymentSetup'] = None
    subscription_plan: List['SubscriptionPlan'] = Field(default_factory=list)
    user_subscription: List['UserSubscription'] = Field(default_factory=list)
    self_realizations_accounts_mapping: List['SelfRealizationsAccountsMapping'] = Field(default_factory=list)
    target_membership_demographic_mapping: List['TargetMembershipDemographicMapping'] = Field(default_factory=list)
    tone_of_communication_mapping: List['ToneOfCommunicationMapping'] = Field(default_factory=list)
    introductory_offer: List['IntroductoryOffer'] = Field(default_factory=list)
    incentive_discount: List['IncentiveDiscount'] = Field(default_factory=list)
    offer_and_service: List['OfferAndService'] = Field(default_factory=list)
    state_messages: List['StateMessages'] = Field(default_factory=list)
    account_faqs: List['AccountFaqs'] = Field(default_factory=list)
    notifications: List['Notifications'] = Field(default_factory=list)
    chat_threads: List['ChatThreads'] = Field(default_factory=list)
    chat_thread_participants: List['ChatThreadParticipants'] = Field(default_factory=list)
    chat_messages: List['ChatMessages'] = Field(default_factory=list)
    mindbody_locations: List['MindbodyLocations'] = Field(default_factory=list)
    mindbody_sale_services: List['MindbodySaleServices'] = Field(default_factory=list)
    key_selling_points_accounts_mapping: List['KeySellingPointsAccountsMapping'] = Field(default_factory=list)
    atmosphere_options_accounts_mapping: List['AtmosphereOptionsAccountsMapping'] = Field(default_factory=list)
    membership_accounts: List['MembershipAccounts'] = Field(default_factory=list)
    class_pack_accounts: List['ClassPackAccounts'] = Field(default_factory=list)

    class Config:
        from_attributes = True


class AccountsUsersMapping(BaseModel):
    id: int
    account_id: int
    user_id: int
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    account: Optional['Account'] = None
    users: Optional['User'] = None

    class Config:
        from_attributes = True


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
    email_unsubscribe_hash: Optional[str] = None
    is_subscribed_to_email: Optional[bool] = True
    is_prospect: Optional[bool] = None
    is_company: Optional[bool] = None
    first_appointment_date: Optional[datetime] = None
    first_class_date: Optional[datetime] = None

    account: Optional['Account'] = None
    state: Optional['State'] = None
    state_history: List['StateHistory'] = Field(default_factory=list)
    reservations: List['Reservation'] = Field(default_factory=list)
    orders: List['Order'] = Field(default_factory=list)
    membership_transactions: List['MembershipTransaction'] = Field(default_factory=list)
    credit_transaction_orders: List['CreditTransactionOrder'] = Field(default_factory=list)
    membership_transactions_testing: List['MembershipTransactionsTesting'] = Field(default_factory=list)
    membership_instances_testing: List['MembershipInstancesTesting'] = Field(default_factory=list)
    membership_transactions_orders_testing: List['MembershipTransactionsOrdersTesting'] = Field(default_factory=list)
    credit_transactions_orders_testing: List['CreditTransactionsOrdersTesting'] = Field(default_factory=list)
    credit_transactions_testing: List['CreditTransactionsTesting'] = Field(default_factory=list)

    class Config:
        from_attributes = True


class EmailConfigurations(BaseModel):
    id: int
    custom_message: Optional[str] = None
    sendgrid_config: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account_id: int
    account: Optional['Account'] = None

    class Config:
        from_attributes = True


class State(BaseModel):
    id: int
    account_id: int
    name: str
    status: 'StateStatus'
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    account: Optional['Account'] = None
    state_transitions: List['StateTransitions'] = Field(default_factory=list)
    state_conditions: List['StateConditions'] = Field(default_factory=list)
    batch_processing_state: List['BatchProcessingState'] = Field(default_factory=list)
    state_messages: List['StateMessages'] = Field(default_factory=list)
    state_history: List['StateHistory'] = Field(default_factory=list)
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class StateTransitions(BaseModel):
    id: int
    state_id: int
    next_state_id: int
    transition_name: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    state: Optional['State'] = None
    next_state: Optional['State'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class StateConditions(BaseModel):
    id: int
    state_id: int
    condition_type: 'Condition'
    value: Optional[str] = None
    logical_operator: Optional['logical_operator'] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    state: Optional['State'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class BatchProcessingState(BaseModel):
    id: int
    account_id: int
    state_id: int
    batch_id: Optional[str] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    state: Optional['State'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class ToneOfCommunicationMapping(BaseModel):
    id: int
    account_id: int
    tone_of_communication_id: int
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    toneOfCommunication: Optional['ToneOfCommunication'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class Notifications(BaseModel):
    id: int
    user_id: int
    account_id: int
    message: str
    notification_type: Optional[str] = None
    read: Optional[bool] = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    user: Optional['User'] = None
    account: Optional['Account'] = None

    class Config:
        from_attributes = True


class SelfRealizations(BaseModel):
    id: int
    name: str
    is_custom: bool
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    self_realizations_accounts_mapping: List['SelfRealizationsAccountsMapping'] = Field(default_factory=list)
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class SelfRealizationsAccountsMapping(BaseModel):
    id: int
    account_id: int
    self_realizations_id: int
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    selfRealization: Optional['SelfRealizations'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class TargetMembershipDemographic(BaseModel):
    id: int
    name: str
    category: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    target_membership_demographic_mappings: List['TargetMembershipDemographicMapping'] = Field(default_factory=list)
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class TargetMembershipDemographicMapping(BaseModel):
    id: int
    account_id: int
    target_membership_demographic_id: int
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    targetMembershipDemographic: Optional['TargetMembershipDemographic'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class ToneOfCommunication(BaseModel):
    id: int
    title: str
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    tone_of_communication_mapping: List['ToneOfCommunicationMapping'] = Field(default_factory=list)
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True

class PopularTopics(BaseModel):
    id: int
    name: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class UniquePhrases(BaseModel):
    id: int
    account_id: int
    phrase: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class AreasToImprove(BaseModel):
    id: int
    account_id: int
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class PreviousCampaigns(BaseModel):
    id: int
    account_id: int
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class MessageTags(BaseModel):
    id: int
    message_id: int
    tagged_user_id: int
    message: Optional['ChatMessages'] = None
    taggedUser: Optional['User'] = None

    class Config:
        from_attributes = True


class Broadcast(BaseModel):
    id: int
    account_id: int
    title: Optional[str] = None
    message: Optional[str] = None
    scheduled_at: Optional[datetime] = None
    sent_at: Optional[datetime] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    recipients: List['BroadcastRecipients'] = Field(default_factory=list)

    class Config:
        from_attributes = True


class AccountFaqs(BaseModel):
    id: int
    account_id: int
    question: str
    answer: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    account: Optional['Account'] = None

    class Config:
        from_attributes = True


class ClassPackAccounts(BaseModel):
    id: int
    account_id: int
    plan_name: str
    pricing: float
    no_credits: str
    credit_expiration: int
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None
    deletedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class IntroductoryOffer(BaseModel):
    id: int
    account_id: int
    offer_name: str
    pricing: Optional[int] = None
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class IncentiveDiscount(BaseModel):
    id: int
    account_id: int
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class OfferAndService(BaseModel):
    id: int
    account_id: int
    service_name: str
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class StateMessages(BaseModel):
    id: int
    state_id: int
    account_id: int
    message_frequency: str
    scheduled_at: Optional[str] = None
    message_body: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    stateMessages: Optional['State'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class TargetMembershipDemographicMapping(BaseModel):
    id: int
    account_id: int
    target_membership_demographic_id: int
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    targetMembershipDemographic: Optional['TargetMembershipDemographic'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class ToneOfCommunication(BaseModel):
    id: int
    title: str
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    tone_of_communication_mapping: List['ToneOfCommunicationMapping'] = Field(default_factory=list)
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True

class PopularTopics(BaseModel):
    id: int
    name: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class UniquePhrases(BaseModel):
    id: int
    account_id: int
    phrase: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class AreasToImprove(BaseModel):
    id: int
    account_id: int
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class PreviousCampaigns(BaseModel):
    id: int
    account_id: int
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class MessageTags(BaseModel):
    id: int
    message_id: int
    tagged_user_id: int
    message: Optional['ChatMessages'] = None
    taggedUser: Optional['User'] = None

    class Config:
        from_attributes = True


class Broadcast(BaseModel):
    id: int
    account_id: int
    title: Optional[str] = None
    message: Optional[str] = None
    scheduled_at: Optional[datetime] = None
    sent_at: Optional[datetime] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    recipients: List['BroadcastRecipients'] = Field(default_factory=list)

    class Config:
        from_attributes = True


class AccountFaqs(BaseModel):
    id: int
    account_id: int
    question: str
    answer: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    account: Optional['Account'] = None

    class Config:
        from_attributes = True


class ClassPackAccounts(BaseModel):
    id: int
    account_id: int
    plan_name: str
    pricing: float
    no_credits: str
    credit_expiration: int
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None
    deletedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class IntroductoryOffer(BaseModel):
    id: int
    account_id: int
    offer_name: str
    pricing: Optional[int] = None
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class IncentiveDiscount(BaseModel):
    id: int
    account_id: int
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class OfferAndService(BaseModel):
    id: int
    account_id: int
    service_name: str
    description: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class StateMessages(BaseModel):
    id: int
    state_id: int
    account_id: int
    message_frequency: str
    scheduled_at: Optional[str] = None
    message_body: str
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    account: Optional['Account'] = None
    stateMessages: Optional['State'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class ChatThreads(BaseModel):
    id: int
    account_id: int
    user_id: int
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    last_seen_message_id: Optional[int] = None
    user: Optional['User'] = None
    account: Optional['Account'] = None
    participants: List['ChatThreadParticipants'] = Field(default_factory=list)
    messages: List['ChatMessages'] = Field(default_factory=list)
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True

class ChatThreadParticipants(BaseModel):
    id: int
    thread_id: int
    user_id: int
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    last_seen_message_id: Optional[int] = None
    thread: Optional['ChatThreads'] = None
    user: Optional['User'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class ChatMessages(BaseModel):
    id: int
    thread_id: int
    sender_id: int
    message: Optional[str] = None
    is_edited: Optional[bool] = False
    attachment_url: Optional[str] = None
    deleted_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    deleted_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    thread: Optional['ChatThreads'] = None
    sender: Optional['User'] = None
    createdBy: Optional['User'] = None
    deletedBy: Optional['User'] = None
    updatedBy: Optional['User'] = None
    messageTags: List['MessageTags'] = Field(default_factory=list)

    class Config:
        from_attributes = True


class MindbodyLocations(BaseModel):
    id: int
    account_id: int
    address: Optional[str] = None
    city: Optional[str] = None
    state_prov_code: Optional[str] = None
    postal_code: Optional[str] = None
    location_id: int
    location_name: Optional[str] = None
    site_id: int
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    account: Optional['Account'] = None

    class Config:
        from_attributes = True


class MindbodySaleServices(BaseModel):
    id: int
    account_id: int
    product_id: int
    service_id: Optional[str] = None
    name: Optional[str] = None
    count: Optional[int] = None
    service_type: Optional[str] = None
    membership_id: Optional[int] = None
    is_intro_offer: Optional[bool] = None
    intro_offer_type: Optional[str] = None
    program: Optional[str] = None
    price: Optional[float] = None
    program_id: Optional[int] = None
    revenue_category: Optional[str] = None
    sell_at_location_ids: Optional[List[int]] = Field(default_factory=list)
    use_at_location_ids: Optional[List[int]] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    account: Optional['Account'] = None

    class Config:
        from_attributes = True


class BroadcastRecipients(BaseModel):
    id: int
    broadcast_id: int
    contact_id: int
    sent_at: Optional[datetime] = None
    status: Optional[str] = None
    broadcast: Optional['Broadcast'] = None
    contact: Optional['Contact'] = None

    class Config:
        from_attributes = True


class PaymentSetup(BaseModel):
    id: int
    account_id: int
    payment_provider: Optional[str] = None
    provider_config: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    account: Optional['Account'] = None

    class Config:
        from_attributes = True


class SubscriptionPlan(BaseModel):
    id: int
    account_id: int
    plan_name: str
    description: Optional[str] = None
    price: Optional[float] = None
    duration_months: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    account: Optional['Account'] = None
    user_subscriptions: List['UserSubscription'] = Field(default_factory=list)

    class Config:
        from_attributes = True


class UserSubscription(BaseModel):
    id: int
    user_id: int
    subscription_plan_id: int
    status: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    user: Optional['User'] = None
    subscription_plan: Optional['SubscriptionPlan'] = None

    class Config:
        from_attributes = True

class CreditTransactionOrder(BaseModel):
    id: Optional[int] = None  # DB-generated PK, so optional on input
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


class Order(BaseModel):
    id: Optional[int] = None  # DB will generate
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
    parent_order: Optional[str] = None  # Parent order ID if this is a child order
    created_at: Optional[datetime] = None   # Let DB default
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None   # Let DB default
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    
    order_lines: List['OrderLines'] = Field(default_factory=list)
    orderCustomer: Optional['Customer'] = None

    class Config:
        from_attributes = True


class OrderLines(BaseModel):
    id: Optional[int] = None  # DB-generated primary key
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
    processed_by: bool = False
    child_orders: List[str] = Field(default_factory=list)
    is_valid: Optional[bool] = True  # Validation flag - set during ETL processing
    created_at: Optional[datetime] = None  
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    # Relations for application use, not for DB insert
    order: Optional['Order'] = None
    creditTransactionOrders: Optional['CreditTransactionOrder'] = None
    membershipTransactionOrders: Optional['MembershipTransactionsOrdersTesting'] = None

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
    account_id: Optional[int] = None
    created_at: Optional[datetime] = None  # Optional for DB default
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None  # Optional for DB default
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    reservations: List['Reservation'] = Field(default_factory=list)

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
    reservation_type_id: Optional[int] = None
    guest: bool = False
    customer_id: Optional[str] = None
    customer_ref_id: Optional[int] = None
    class_session_id: Optional[str] = None
    class_session_ref_id: Optional[int] = None
    account_id: Optional[int] = None
    first_timer: bool = False
    reservation_type: Optional[str] = None
    location: Optional[int] = None
    created_at: Optional[datetime] = None  
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None  # make optional, DB default manages it
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True


class EztextingMessage(BaseModel):
    id: int
    message_id: Optional[str] = None
    user_number: Optional[str] = None
    contact_number: Optional[str] = None
    inbound: Optional[bool] = None
    message: Optional[str] = None
    sent_at: Optional[datetime] = None
    opt_in: Optional[bool] = None
    opt_out: Optional[bool] = None
    status: Optional[str] = None
    type: Optional[str] = None
    is_group_message: Optional[bool] = None
    is_escalated: Optional[bool] = None
    is_answered_by_ai: Optional[bool] = None
    escalated_answered_for: Optional[List[int]] = Field(default_factory=list)
    escalated_answered_at: Optional[datetime] = None
    message_group_id: Optional[str] = None
    account_id: Optional[int] = None
    customer_ref_id: Optional[int] = None
    customer_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class MembershipTransactionsTesting(BaseModel):
    id: int
    membership_transactions_id: Optional[int] = None
    transaction_date: Optional[datetime] = None
    membership_name: Optional[str] = None
    parent_membership_transaction_id: Optional[int] = None
    membership_instances_id: Optional[int] = None
    membership_instances_ref_id: Optional[int] = None
    customer_id: Optional[str] = None
    account_id: Optional[int] = None
    location: Optional[int] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True



class MembershipInstancesTesting(BaseModel):
    id: int
    membership_instances_id: Optional[int] = None
    purchase_date: Optional[datetime] = None
    membership_name: Optional[str] = None
    renewal_rate_incl_tax: Optional[str] = None
    status: Optional[str] = None
    location: Optional[int] = None
    renewal_count: Optional[int] = None
    account_id: Optional[int] = None
    next_charge_date: Optional[datetime] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True

# --- Main model for membership_instances ---
class MembershipInstance(BaseModel):
    id: Optional[int] = None  # DB-generated PK, optional on input
    membership_instances_id: Optional[int] = None
    purchase_date: Optional[datetime] = None
    membership_name: Optional[str] = None
    renewal_rate_incl_tax: Optional[str] = None
    status: Optional[str] = None
    location: Optional[int] = None
    renewal_count: Optional[int] = None
    next_charge_date: Optional[datetime] = None
    account_id: Optional[int] = None
    created_at: Optional[datetime] = None    # Let DB default
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None    # Let DB default
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True

# --- Main model for membership_transactions ---
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
    created_at: Optional[datetime] = None  
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None 
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    creditTransactionsCustomer: Optional['Customer'] = None
    membershipTransactionReservations: Optional['Reservation'] = None
    membershipInstance: Optional['MembershipInstance'] = None

    class Config:
        from_attributes = True



class MembershipTransactionsOrdersTesting(BaseModel):
    id: int
    membership_transactions_id: Optional[int] = None
    transaction_date: Optional[datetime] = None
    membership_name: Optional[str] = None
    parent_membership_transaction_id: Optional[int] = None
    membership_instances_id: Optional[int] = None
    membership_instances_ref_id: Optional[int] = None
    payment_interval_end_date: Optional[datetime] = None
    customer_id: Optional[str] = None
    account_id: Optional[int] = None
    location: Optional[int] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True


class CreditTransactionsOrdersTesting(BaseModel):
    id: int
    credit_transactions_id: Optional[int] = None
    transaction_date: Optional[datetime] = None
    credit_name: Optional[str] = None
    is_expired: Optional[bool] = None
    is_intro_offer: Optional[bool] = None
    parent_credit_transaction_type: Optional[str] = None
    parent_credit_transaction_id: Optional[int] = None
    customer_id: Optional[str] = None
    account_id: Optional[int] = None
    location: Optional[int] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True


class CreditTransactionsTesting(BaseModel):
    id: int
    credit_transactions_id: Optional[int] = None
    transaction_date: Optional[datetime] = None
    credit_name: Optional[str] = None
    is_expired: Optional[bool] = None
    is_intro_offer: Optional[bool] = None
    parent_credit_transaction_type: Optional[str] = None
    parent_credit_transaction_id: Optional[int] = None
    customer_id: Optional[str] = None
    account_id: Optional[int] = None
    location: Optional[int] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True


class MindbodySitesSessionTypes(BaseModel):
    id: int
    account_id: int
    session_type_id: int
    session_type_name: Optional[str] = None
    program_id: Optional[int] = None
    category: Optional[str] = None
    sub_category: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None

    class Config:
        from_attributes = True


class Broadcasts(BaseModel):
    id: int
    account_id: int
    name: Optional[str] = None
    schedule_type: Optional[str] = None
    sent_at: Optional[datetime] = None
    message: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None

    class Config:
        from_attributes = True


class BroadcastRecipients(BaseModel):
    id: int
    broadcast_id: int
    state_id: int
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None

    class Config:
        from_attributes = True


class StateHistory(BaseModel):
    id: int
    state_id: int
    contact_group_id: int
    customer_id: Optional[str] = None
    customer_ref_id: Optional[int] = None
    history_type: StateHistoryType
    created_at: Optional[datetime] = None
    stateHistory: Optional['State'] = None
    customer: Optional['Customer'] = None

    class Config:
        from_attributes = True


class EztextingAccount(BaseModel):
    id: int
    username: str
    password: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    accounts: List['Account'] = Field(default_factory=list)

    class Config:
        from_attributes = True


class User(BaseModel):
    id: int
    email: str
    name: Optional[str] = None
    role: Role
    email_verified: Optional[bool] = None
    auth0_id: str
    user_metadata: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[int] = None
    accounts_users_mapping: List['AccountsUsersMapping'] = Field(default_factory=list)
    userSubscriptions: List['UserSubscription'] = Field(default_factory=list)
    createdBroadcast: List['Broadcast'] = Field(default_factory=list)
    updatedBroadcast: List['Broadcast'] = Field(default_factory=list)
    deletedBroadcast: List['Broadcast'] = Field(default_factory=list)
    createdBroadcastRecipients: List['BroadcastRecipients'] = Field(default_factory=list)
    updatedBroadcastRecipients: List['BroadcastRecipients'] = Field(default_factory=list)
    createdFaqs: List['AccountFaqs'] = Field(default_factory=list)
    updatedFaqs: List['AccountFaqs'] = Field(default_factory=list)
    chatThreads: List['ChatThreads'] = Field(default_factory=list)
    createdChatThreads: List['ChatThreads'] = Field(default_factory=list)
    updatedChatThreads: List['ChatThreads'] = Field(default_factory=list)
    chatThreadParticipants: List['ChatThreadParticipants'] = Field(default_factory=list)
    createdChatThreadParticipants: List['ChatThreadParticipants'] = Field(default_factory=list)
    updatedChatThreadParticipants: List['ChatThreadParticipants'] = Field(default_factory=list)
    sentMessages: List['ChatMessages'] = Field(default_factory=list)
    createdChatMessages: List['ChatMessages'] = Field(default_factory=list)
    deletedChatMessages: List['ChatMessages'] = Field(default_factory=list)
    updatedChatMessages: List['ChatMessages'] = Field(default_factory=list)
    taggedInMessages: List['MessageTags'] = Field(default_factory=list)
    createdAccountClassPack: List['ClassPackAccounts'] = Field(default_factory=list)
    updatedAccountClassPack: List['ClassPackAccounts'] = Field(default_factory=list)
    deletedAccountClassPack: List['ClassPackAccounts'] = Field(default_factory=list)
    createdIntroductoryOffers: List['IntroductoryOffer'] = Field(default_factory=list)
    updatedIntroductoryOffers: List['IntroductoryOffer'] = Field(default_factory=list)
    createdIncentiveDiscounts: List['IncentiveDiscount'] = Field(default_factory=list)
    updatedIncentiveDiscounts: List['IncentiveDiscount'] = Field(default_factory=list)
    createdOffersServices: List['OfferAndService'] = Field(default_factory=list)
    updatedOffersServices: List['OfferAndService'] = Field(default_factory=list)
    createdStateMessages: List['StateMessages'] = Field(default_factory=list)
    updatedStateMessages: List['StateMessages'] = Field(default_factory=list)
    createdSelfRealizations: List['SelfRealizations'] = Field(default_factory=list)
    updatedSelfRealizations: List['SelfRealizations'] = Field(default_factory=list)
    createdSelfRealizationMapping: List['SelfRealizationsAccountsMapping'] = Field(default_factory=list)
    updatedSelfRealizationMapping: List['SelfRealizationsAccountsMapping'] = Field(default_factory=list)
    createdTargetMembershipDemographic: List['TargetMembershipDemographic'] = Field(default_factory=list)
    updatedTargetMembershipDemographic: List['TargetMembershipDemographic'] = Field(default_factory=list)
    createdTargetMembershipDemographicMapping: List['TargetMembershipDemographicMapping'] = Field(default_factory=list)
    updatedTargetMembershipDemographicMapping: List['TargetMembershipDemographicMapping'] = Field(default_factory=list)
    createdToneOfCommunications: List['ToneOfCommunication'] = Field(default_factory=list)
    updatedToneOfCommunications: List['ToneOfCommunication'] = Field(default_factory=list)
    createdPopularTopics: List['PopularTopics'] = Field(default_factory=list)
    updatedPopularTopics: List['PopularTopics'] = Field(default_factory=list)
    createdUniquePhrases: List['UniquePhrases'] = Field(default_factory=list)
    updatedUniquePhrases: List['UniquePhrases'] = Field(default_factory=list)
    createdAreasToImprove: List['AreasToImprove'] = Field(default_factory=list)
    updatedAreasToImprove: List['AreasToImprove'] = Field(default_factory=list)
    createdPreviousCampaigns: List['PreviousCampaigns'] = Field(default_factory=list)
    updatedPreviousCampaigns: List['PreviousCampaigns'] = Field(default_factory=list)
    createdAccount: List['Account'] = Field(default_factory=list)
    updatedAccount: List['Account'] = Field(default_factory=list)
    deletedAccount: List['Account'] = Field(default_factory=list)
    createdSubscriptionPlan: List['SubscriptionPlan'] = Field(default_factory=list)
    updatedSubscriptionPlan: List['SubscriptionPlan'] = Field(default_factory=list)
    deletedSubscriptionPlan: List['SubscriptionPlan'] = Field(default_factory=list)
    createdState: List['State'] = Field(default_factory=list)
    updatedState: List['State'] = Field(default_factory=list)
    deletedState: List['State'] = Field(default_factory=list)
    createdStateTransitions: List['StateTransitions'] = Field(default_factory=list)
    updatedStateTransitions: List['StateTransitions'] = Field(default_factory=list)
    createdStateConditions: List['StateConditions'] = Field(default_factory=list)
    updatedStateConditions: List['StateConditions'] = Field(default_factory=list)
    createdBatchProcessingState: List['BatchProcessingState'] = Field(default_factory=list)
    updatedBatchProcessingState: List['BatchProcessingState'] = Field(default_factory=list)
    createdMindbodyLocations: List['MindbodyLocations'] = Field(default_factory=list)
    updatedMindbodyLocations: List['MindbodyLocations'] = Field(default_factory=list)
    createdMindbodySaleServices: List['MindbodySaleServices'] = Field(default_factory=list)
    updatedMindbodySaleServices: List['MindbodySaleServices'] = Field(default_factory=list)

    class Config:
        from_attributes = True


class EztextingTransactionHistory(BaseModel):
    id: int
    account_id: int
    stripe_customer_id: Optional[str] = None
    payment_intent_id: Optional[str] = None
    last_error_message: Optional[str] = None
    credit_status: Optional[CreditStatus] = None
    invoice_id: Optional[str] = None
    credit_amount: Optional[float] = None
    credit_value: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class Faqs(BaseModel):
    id: int
    question: str
    answer: str
    topic_id: Optional[int] = None
    created_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[int] = None
    topic: Optional['PopularTopics'] = None
    createdBy: Optional['User'] = None
    updatedBy: Optional['User'] = None

    class Config:
        from_attributes = True


class EztextingMessageVerification(BaseModel):
    id: int
    account_id: int
    message_id: str
    scheduled_at: datetime
    verified_at: Optional[datetime] = None
    status: Optional[str] = None
    bounced: Optional[int] = 0
    queued: Optional[int] = 0
    total_delivered: Optional[int] = 0
    total_not_sent: Optional[int] = 0
    group_ids: Optional[str] = None
    failed_contacts: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    account: Optional['Account'] = None

    class Config:
        from_attributes = True
