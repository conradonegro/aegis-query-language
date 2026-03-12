import os
import time
from unittest.mock import MagicMock, patch
import pytest

from hvac.exceptions import VaultError  # type: ignore[import-untyped]

from app.vault import (
    EnvFallbackProvider,
    HashiCorpVaultProvider,
    VaultConfigurationError,
    VaultMissingSecretError,
    get_secrets_manager,
)


def test_env_fallback_provider(monkeypatch):
    """Test standard local configuration correctly resolves passwords."""
    monkeypatch.setenv("DB_PASS_RUNTIME", "my_secure_dev_pass")
    monkeypatch.setenv("SIGNING_KEY_DEV", "dev_hmac_123")
    
    provider = EnvFallbackProvider()
    assert provider.get_database_password("user_aegis_runtime") == "my_secure_dev_pass"
    assert provider.get_signing_key("test_key") == "dev_hmac_123"
    
    with pytest.raises(VaultMissingSecretError):
        provider.get_database_password("unknown_role")


@patch("app.vault.hvac.Client")
def test_vault_provider_authentication_and_caching(MockClient):
    """Test Vault AppRole authenticate logic and Memory TTL caching behavior."""
    mock_client_instance = MockClient.return_value
    
    # Mock AppRole login response
    mock_client_instance.auth.approle.login.return_value = {
        "auth": {
            "client_token": "s.12345ABCD",
            "lease_duration": 60 # Seconds
        }
    }
    
    # Mock KV V2 read response
    mock_client_instance.secrets.kv.v2.read_secret_version.return_value = {
        "data": {
            "data": {
                "user_aegis_runtime": "vault_runtime_pass",
                "user_aegis_steward": "vault_steward_pass"
            }
        }
    }
    
    provider = HashiCorpVaultProvider(
        vault_addr="https://vault.local:8200",
        role_id="my_role",
        secret_id="my_secret",
        ttl_seconds=30
    )
    
    # 1. First fetch: should trigger AppRole login + KV read
    pass1 = provider.get_database_password("user_aegis_runtime")
    assert pass1 == "vault_runtime_pass"
    mock_client_instance.auth.approle.login.assert_called_once()
    mock_client_instance.secrets.kv.v2.read_secret_version.assert_called_once_with(path="aegis/database/credentials")
    
    # 2. Second fetch: should hit memory cache exactly (No new HTTP hits)
    pass2 = provider.get_database_password("user_aegis_steward")
    assert pass2 == "vault_steward_pass"
    mock_client_instance.auth.approle.login.assert_called_once() # Still 1
    mock_client_instance.secrets.kv.v2.read_secret_version.assert_called_once() # Still 1
    
    # 3. Simulate TTL Expiration (Move time forward 40 seconds)
    with patch("app.vault.time.time", return_value=time.time() + 40):
        # The TTL is expired, it must re-authenticate and re-fetch!
        pass3 = provider.get_database_password("user_aegis_runtime")
        assert pass3 == "vault_runtime_pass"
        assert mock_client_instance.auth.approle.login.call_count == 2
        assert mock_client_instance.secrets.kv.v2.read_secret_version.call_count == 2


@patch("app.vault.hvac.Client")
def test_vault_provider_missing_secret(MockClient):
    """Test that missing KV payloads raise explicit domain errors."""
    mock_client_instance = MockClient.return_value
    mock_client_instance.auth.approle.login.return_value = {"auth": {"client_token": "tok", "lease_duration": 60}}
    mock_client_instance.secrets.kv.v2.read_secret_version.return_value = {"data": {"data": {}}} # Empty KV Map
    
    provider = HashiCorpVaultProvider("https://v", "r", "s")
    with pytest.raises(VaultMissingSecretError):
        provider.get_database_password("user_aegis_runtime")


def test_vault_tls_enforcement(monkeypatch):
    """Test that Vault instantiation throws violent exceptions if production TLS is bypassed."""
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("TESTING", "false")
    
    with pytest.raises(VaultConfigurationError, match="TLS required"):
        HashiCorpVaultProvider(vault_addr="http://unsecured-vault.local:8200", role_id="r", secret_id="s")


def test_env_fallback_provider_get_api_key_present(monkeypatch):
    """EnvFallbackProvider.get_api_key returns the key when the env var is set."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-123")
    provider = EnvFallbackProvider()
    assert provider.get_api_key("openai") == "sk-test-123"


def test_env_fallback_provider_get_api_key_missing(monkeypatch):
    """EnvFallbackProvider.get_api_key raises VaultMissingSecretError when absent."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    provider = EnvFallbackProvider()
    with pytest.raises(VaultMissingSecretError, match="anthropic"):
        provider.get_api_key("anthropic")


def test_get_secrets_manager_factory(monkeypatch):
    """Test initialization routing based on SECRETS_PROVIDER configurations."""
    monkeypatch.setenv("SECRETS_PROVIDER", "env")
    manager = get_secrets_manager()
    assert isinstance(manager, EnvFallbackProvider)
    
    monkeypatch.setenv("ENVIRONMENT", "production")
    # Prod ENV fallback must fail safely
    with pytest.raises(VaultConfigurationError, match="CRITICAL"):
        get_secrets_manager()
        
    monkeypatch.setenv("SECRETS_PROVIDER", "vault")
    # Missing AppRole credentials must fail
    with pytest.raises(VaultConfigurationError, match="missing"):
        get_secrets_manager()
        
    monkeypatch.setenv("VAULT_ADDR", "https://v")
    monkeypatch.setenv("VAULT_APPROLE_ROLE_ID", "r")
    monkeypatch.setenv("VAULT_APPROLE_SECRET_ID", "s")
    
    manager2 = get_secrets_manager()
    assert isinstance(manager2, HashiCorpVaultProvider)
