import datetime

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class UserTable(Base):
    __tablename__ = "users"
    id: int = Column(Integer, primary_key=True, index=True)
    name: str = Column(String)
    email: str = Column(String, unique=True)
    city: str = Column(String)
    created_at: datetime = Column(DateTime, default=datetime.datetime.now(datetime.UTC))


class PurchaseTable(Base):
    __tablename__ = "purchases"
    id = Column(Integer, primary_key=True, index=True)
    total_amount = Column(Float)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    payment_method = Column(String)
    timestamp = Column(DateTime, default=datetime.datetime.now(datetime.UTC))


class ErrorsTable(Base):
    __tablename__ = "ingestion_errors"
    id = Column(Integer, primary_key=True, index=True)
    topic_name = Column(String)
    raw_data = Column(JSON)
    error_detail = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.now(datetime.UTC))
