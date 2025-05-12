from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, create_engine
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.ext.declarative import declared_attr

Base = declarative_base()

class Channel(Base):
    """Slack channel model."""
    __tablename__ = "channels"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    messages = relationship("Message", back_populates="channel", cascade="all, delete-orphan")

class Message(Base):
    """Slack message model."""
    __tablename__ = "messages"

    id = Column(String, primary_key=True)
    channel_id = Column(String, ForeignKey("channels.id"), nullable=False)
    user_id = Column(String, nullable=False)
    text = Column(Text, nullable=False)
    thread_ts = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    channel = relationship("Channel", back_populates="messages")
    candidates = relationship("Candidate", secondary="message_candidates", back_populates="messages")

class Candidate(Base):
    """Candidate model."""
    __tablename__ = "candidates"

    id = Column(Integer, primary_key=True)
    linkedin_url = Column(String, nullable=True)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    messages = relationship("Message", secondary="message_candidates", back_populates="candidates")

class MessageCandidate(Base):
    """Association table for messages and candidates."""
    __tablename__ = "message_candidates"

    message_id = Column(String, ForeignKey("messages.id"), primary_key=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id"), primary_key=True)
    confidence = Column(Integer, nullable=False)  # 0-100 confidence score
    created_at = Column(DateTime, default=datetime.utcnow)

class Embedding(Base):
    """Message embedding model."""
    __tablename__ = "embeddings"

    message_id = Column(String, ForeignKey("messages.id"), primary_key=True)
    embedding = Column(Text, nullable=False)  # JSON string of embedding vector
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Summary(Base):
    """Channel summary model."""
    __tablename__ = "summaries"

    id = Column(Integer, primary_key=True)
    channel_id = Column(String, ForeignKey("channels.id"), nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow) 