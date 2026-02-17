import pytest
from pydantic import ValidationError

from aether_engine.scripts.ingestor import UserSchema


def test_user_schema_valid_email():
    user = UserSchema(name="Test User", email="test@example.com", city="Mexico City")

    assert user.email == "test@example.com"


def test_user_schema_invalid_email():
    with pytest.raises(ValidationError):
        UserSchema(name="Bad User", email="this-is-not-an-email", city="CDMX")
