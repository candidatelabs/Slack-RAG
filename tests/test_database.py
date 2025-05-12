import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, Channel, Message, Candidate, MessageCandidate, Embedding, Summary
from database import DatabaseManager
from config import DatabaseConfig

@pytest.fixture
def db_config():
    return DatabaseConfig(url="sqlite:///:memory:")

@pytest.fixture
def db_manager(db_config):
    return DatabaseManager(db_config)

@pytest.fixture
def session(db_manager):
    with db_manager.get_session() as session:
        yield session

def test_create_tables(db_manager):
    """Test that tables are created successfully."""
    db_manager.create_tables()
    # Verify tables exist by querying them
    with db_manager.get_session() as session:
        session.execute("SELECT * FROM channels")
        session.execute("SELECT * FROM messages")
        session.execute("SELECT * FROM candidates")
        session.execute("SELECT * FROM message_candidates")
        session.execute("SELECT * FROM embeddings")
        session.execute("SELECT * FROM summaries")

def test_channel_crud(session):
    """Test CRUD operations for Channel model."""
    # Create
    channel = Channel(id="C123", name="test-channel")
    session.add(channel)
    session.commit()

    # Read
    result = session.query(Channel).filter_by(id="C123").first()
    assert result is not None
    assert result.name == "test-channel"

    # Update
    result.name = "updated-channel"
    session.commit()
    updated = session.query(Channel).filter_by(id="C123").first()
    assert updated.name == "updated-channel"

    # Delete
    session.delete(updated)
    session.commit()
    deleted = session.query(Channel).filter_by(id="C123").first()
    assert deleted is None

def test_message_crud(session):
    """Test CRUD operations for Message model."""
    # Create channel first
    channel = Channel(id="C123", name="test-channel")
    session.add(channel)
    session.commit()

    # Create message
    message = Message(
        id="M123",
        channel_id="C123",
        user_id="U123",
        text="Test message"
    )
    session.add(message)
    session.commit()

    # Read
    result = session.query(Message).filter_by(id="M123").first()
    assert result is not None
    assert result.text == "Test message"
    assert result.channel.name == "test-channel"

    # Update
    result.text = "Updated message"
    session.commit()
    updated = session.query(Message).filter_by(id="M123").first()
    assert updated.text == "Updated message"

    # Delete
    session.delete(updated)
    session.commit()
    deleted = session.query(Message).filter_by(id="M123").first()
    assert deleted is None

def test_candidate_crud(session):
    """Test CRUD operations for Candidate model."""
    # Create candidate
    candidate = Candidate(
        linkedin_url="https://linkedin.com/in/test",
        name="Test Candidate"
    )
    session.add(candidate)
    session.commit()

    # Read
    result = session.query(Candidate).filter_by(linkedin_url="https://linkedin.com/in/test").first()
    assert result is not None
    assert result.name == "Test Candidate"

    # Update
    result.name = "Updated Candidate"
    session.commit()
    updated = session.query(Candidate).filter_by(linkedin_url="https://linkedin.com/in/test").first()
    assert updated.name == "Updated Candidate"

    # Delete
    session.delete(updated)
    session.commit()
    deleted = session.query(Candidate).filter_by(linkedin_url="https://linkedin.com/in/test").first()
    assert deleted is None

def test_message_candidate_association(session):
    """Test message-candidate association."""
    # Create channel
    channel = Channel(id="C123", name="test-channel")
    session.add(channel)

    # Create message
    message = Message(
        id="M123",
        channel_id="C123",
        user_id="U123",
        text="Test message"
    )
    session.add(message)

    # Create candidate
    candidate = Candidate(
        linkedin_url="https://linkedin.com/in/test",
        name="Test Candidate"
    )
    session.add(candidate)
    session.commit()

    # Create association
    association = MessageCandidate(
        message_id="M123",
        candidate_id=candidate.id,
        confidence=80
    )
    session.add(association)
    session.commit()

    # Verify association
    result = session.query(Message).filter_by(id="M123").first()
    assert len(result.candidates) == 1
    assert result.candidates[0].name == "Test Candidate"

    candidate_result = session.query(Candidate).filter_by(linkedin_url="https://linkedin.com/in/test").first()
    assert len(candidate_result.messages) == 1
    assert candidate_result.messages[0].text == "Test message" 