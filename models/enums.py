# models/enums.py
from enum import Enum as PyEnum


from enum import Enum

class Entity(Enum):
    BODY_BAR_PILATES = "BODY_BAR_PILATES"
    CROSSFIT = "CROSSFIT"

class Status(Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    ONBOARDING = "ONBOARDING"
    WAITING_PAYMENT = "WAITING_PAYMENT"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    APPROVED = "APPROVED"

class Role(Enum):
    ACCOUNT_MANAGER = "ACCOUNT_MANAGER"
    SUPER_ADMIN = "SUPER_ADMIN"
    USERS = "USERS"

class StateStatus(Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    WAITING_FOR_APPROVAL = "WAITING_FOR_APPROVAL"

class SubscriptionStatusDb(Enum):
    ACTIVE = "ACTIVE"
    ENDED = "ENDED"
    CANCELED = "CANCELED"
    PAST_DUE = "PAST_DUE"
    TRIAL = "TRIAL"
    IN_GRACE_PERIOD = "IN_GRACE_PERIOD"

class CreditStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    INTERMEDIATE = "INTERMEDIATE"

class Personality(Enum):
    Never = "Never"
    Sometimes = "Sometimes"
    Often = "Often"

class ScheduleType(Enum):
    Immediately = "Immediately"
    Schedule = "Schedule"

class Condition(Enum):
    EQUALS = "EQUALS"
    NOT_EQUALS = "NOT_EQUALS"
    GREATER_THAN = "GREATER_THAN"
    LESS_THAN = "LESS_THAN"
    GREATER_THAN_EQUALS = "GREATER_THAN_EQUALS"
    LESS_THAN_EQUALS = "LESS_THAN_EQUALS"

class LogicalOperator(Enum):
    AND = "AND"
    OR = "OR"

class ContactStatus(Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"

class MembershipStatus(Enum):
    ACTIVE = "ACTIVE"
    REMOVED = "REMOVED"

class StateHistoryType(Enum):
    ADDED = "ADDED"
    REMOVED = "REMOVED"

class Chats(Enum):
    chat = "chat"

class StateHistoryType(Enum):
    ADDED = "ADDED"
    REMOVED = "REMOVED"
