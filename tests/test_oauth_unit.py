"""Unit tests for OAuth authentication - no credentials required."""

from __future__ import annotations

import pytest
from singer_sdk.exceptions import ConfigValidationError

from target_snowflake.connector import SnowflakeAuthMethod, SnowflakeConnector


class TestOAuthUnit:
    """Pure unit tests for OAuth functionality."""

    def test_oauth_auth_method_detection(self):
        """Test OAuth method detection without external dependencies."""
        config = {
            "account": "test_account",
            "user": "test_user",
            "database": "test_db",
            "oauth_access_token": "test_token_123",
        }
        connector = SnowflakeConnector(config=config)
        assert connector.auth_method == SnowflakeAuthMethod.OAUTH

    def test_empty_oauth_token_error(self):
        """Test that empty OAuth token fails during connect args generation."""
        config = {
            "account": "test_account",
            "user": "test_user",
            "database": "test_db",
            "oauth_access_token": "",  # Empty string
        }
        connector = SnowflakeConnector(config=config)
        assert connector.auth_method == SnowflakeAuthMethod.OAUTH
        # Should fail early during URL generation
        with pytest.raises(ConfigValidationError, match="OAuth access token is required but not provided or is empty"):
            connector.get_connect_args()

    def test_oauth_connect_args(self):
        """Test SQLAlchemy URL generation for OAuth."""
        config = {
            "account": "test_account",
            "user": "test_user",
            "database": "test_db",
            "oauth_access_token": "test_token_789",
        }
        connector = SnowflakeConnector(config=config)
        connect_args = connector.get_connect_args()

        assert connect_args["authenticator"] == "oauth"
        assert connect_args["token"] == "test_token_789"  # noqa: S105

    @pytest.mark.parametrize(
        ("auth_config", "expected_method"),
        [
            ({"password": "pwd123"}, SnowflakeAuthMethod.PASSWORD),
            ({"private_key": "key123"}, SnowflakeAuthMethod.KEY_PAIR),
            ({"use_browser_authentication": True}, SnowflakeAuthMethod.BROWSER),
            ({"oauth_access_token": "token123"}, SnowflakeAuthMethod.OAUTH),
        ],
    )
    def test_auth_method_precedence(self, auth_config, expected_method):
        """Test authentication method precedence."""
        base_config = {
            "account": "test_account",
            "user": "test_user",
            "database": "test_db",
        }
        config = {**base_config, **auth_config}

        connector = SnowflakeConnector(config=config)
        assert connector.auth_method == expected_method
