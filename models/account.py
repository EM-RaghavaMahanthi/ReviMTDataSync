from sqlalchemy import Column, Integer, String
from .base import Base

class Users(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), nullable=False, unique=True)
    name = Column(String(100))

class Account(Base):
    __tablename__ = 'accounts'
    id = Column(Integer, primary_key=True, index=True)
    integration_id = Column(String)
    status = Column(String)
    crm_config = Column(String)
    crm_api_end_point = Column(String)

class States(Base):
    __tablename__ = 'states'
    id = Column(Integer, primary_key=True, autoincrement=True)
    state_name = Column(String(100), nullable=False)
