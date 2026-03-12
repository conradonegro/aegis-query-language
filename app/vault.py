import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Any, Optional

import hvac  # type: ignore[import-untyped]
from hvac.exceptions import VaultError  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

class VaultConfigurationError(Exception):
    pass

class VaultMissingSecretError(Exception):
    pass

class SecretsManager(ABC):
    @abstractmethod
    def get_database_password(self, role_name: str) -> str:
        """Fetch the database password for the given PostgreSQL role."""
        pass
        
    @abstractmethod
    def get_signing_key(self, key_id: str) -> str:
        """Fetch the specific HMAC signing key."""
        pass
        
    @abstractmethod
    def get_current_signing_key_id(self) -> str:
        """Fetch the active key ID used for signing new artifacts."""
        pass

    @abstractmethod
    def get_api_key(self, provider_name: str) -> str:
        """
        Fetch an LLM provider API key.
        provider_name: lower-case provider slug, e.g. 'openai', 'anthropic'.
        Raises VaultMissingSecretError if the key is absent.
        """
        pass


class EnvFallbackProvider(SecretsManager):
    """
    Insecure, Developer-Only Dev/CI Provider.
    Extracts raw passwords directly from the environment.
    """
    def get_database_password(self, role_name: str) -> str:
        mapping = {
            "user_aegis_runtime": os.getenv("DB_PASS_RUNTIME", "runtime_pass"),
            "user_aegis_registry_runtime": os.getenv("DB_PASS_REGISTRY_RUNTIME", "registry_pass"),
            "user_aegis_steward": os.getenv("DB_PASS_STEWARD", "steward_pass"),
            "user_aegis_registry_admin": os.getenv("DB_PASS_REGISTRY_ADMIN", "admin_pass"),
            "user_aegis_data_owner": os.getenv("DB_PASS_DATA_OWNER", "data_owner_pass"),
            "user_aegis_meta_owner": os.getenv("DB_PASS_META_OWNER", "meta_owner_pass")
        }
        if role_name not in mapping:
            raise VaultMissingSecretError(f"No fallback password mapped for role {role_name}")
        return mapping[role_name]
        
    def get_signing_key(self, key_id: str) -> str:
        return os.getenv("SIGNING_KEY_DEV", "aegis_dev_hmac_secret_key_001")
        
    def get_current_signing_key_id(self) -> str:
        return os.getenv("SIGNING_KEY_ID_DEV", "dev-key-1")

    def get_api_key(self, provider_name: str) -> str:
        env_var = f"{provider_name.upper()}_API_KEY"
        key = os.getenv(env_var)
        if not key:
            raise VaultMissingSecretError(
                f"No API key found for provider '{provider_name}'. "
                f"Set {env_var} in your .env file."
            )
        return key


class HashiCorpVaultProvider(SecretsManager):
    """
    Production-Grade Vault Integration utilizing AppRole authentication.
    Enforces TLS strict verification and implements memory TTL caching.
    """
    def __init__(self, vault_addr: str, role_id: str, secret_id: str, ttl_seconds: int = 300):
        if not vault_addr.startswith("https://"):
            if os.getenv("TESTING") != "true" and os.getenv("ENVIRONMENT") == "production":
                raise VaultConfigurationError("TLS required: VAULT_ADDR must strictly use HTTPS in production.")
                
        self.vault_addr = vault_addr
        self.role_id = role_id
        self.secret_id = secret_id
        self.ttl_seconds = ttl_seconds
        
        self.client = hvac.Client(url=self.vault_addr, verify=True) # Always strictly verify=True

        # Memory caches to avoid hitting Vault per HTTP request
        self._auth_token: str = ""
        self._auth_expires_at: float = 0.0
        self._secret_cache: dict[str, dict[str, Any]] = {} # Path -> {"data": val, "expires_at": timestamp}

    def _authenticate(self) -> None:
        """Authenticates via AppRole and caches the auth token."""
        now = time.time()
        if self._auth_token and now < self._auth_expires_at:
            return # Still authentically valid

        try:
            response = self.client.auth.approle.login(
                role_id=self.role_id,
                secret_id=self.secret_id,
            )
            # Typically Vault tokens have leases, applying a flat conservative TTL cache client-side
            client_token = response['auth']['client_token']
            lease_duration = response['auth'].get('lease_duration', self.ttl_seconds)

            # Subtracted a buffer of 10 seconds to preempt expiration mid-flight
            cache_duration = min(self.ttl_seconds, max(1, lease_duration - 10))

            self.client.token = client_token
            self._auth_token = client_token
            self._auth_expires_at = now + cache_duration
            logger.info("Successfully refreshed Vault AppRole Lease.")
        except VaultError as e:
            logger.error(f"Failed to authenticate with Vault via AppRole: {e}")
            raise VaultConfigurationError(f"Vault AppRole Auth Failed: {e}")

    def _get_cached_secret(self, path: str, key_name: str) -> str:
        now = time.time()
        
        cache_hit = self._secret_cache.get(path)
        if cache_hit and now < cache_hit["expires_at"]:
            if key_name in cache_hit["data"]:
                return str(cache_hit["data"][key_name])
                
        # Cache Miss or Expired -> Refetch
        self._authenticate()
        try:
            # Assumes kv-v2 engine mounted at 'secret' engine path defaultly
            # Customizing mount_point can happen if user provides specialized mounts
            response = self.client.secrets.kv.v2.read_secret_version(path=path)
            data = response.get("data", {}).get("data", {})
            
            # Cache the whole path response map
            self._secret_cache[path] = {
                "data": data,
                "expires_at": now + self.ttl_seconds
            }
            
            if key_name not in data:
                raise VaultMissingSecretError(f"Vault key '{key_name}' missing at KV path '{path}'.")
                
            return str(data[key_name])
            
        except VaultError as e:
            logger.error(f"Vault error reading '{path}': {e}")
            raise VaultMissingSecretError(f"Failed fetching secret {path} -> {key_name}: {e}")

    def get_database_password(self, role_name: str) -> str:
        """
        Extracts native credentials per architectural role.
        Expects KV v2 format: 'aegis/database/credentials' where keys are user roles.
        """
        return self._get_cached_secret(path="aegis/database/credentials", key_name=role_name)
        
    def get_signing_key(self, key_id: str) -> str:
        """
        Retrieves specific HMAC keys used for compiled registry signature verification.
        Expects KV v2 format: 'aegis/artifacts/keys'
        """
        return self._get_cached_secret(path="aegis/artifacts/keys", key_name=key_id)
        
    def get_current_signing_key_id(self) -> str:
        """
        Retrieves the globally promoted active key ID.
        Expects KV v2 format: 'aegis/artifacts/config'
        """
        return self._get_cached_secret(path="aegis/artifacts/config", key_name="current_key_id")

    def get_api_key(self, provider_name: str) -> str:
        """
        Retrieves an LLM provider API key.
        Expects KV v2 format: secret/aegis/llm/credentials
        Keys stored as: OPENAI_API_KEY, ANTHROPIC_API_KEY, etc.
        """
        key_name = f"{provider_name.upper()}_API_KEY"
        return self._get_cached_secret(path="aegis/llm/credentials", key_name=key_name)


def get_secrets_manager() -> SecretsManager:
    """
    Factory resolving the Active Secrets Manager.
    Fails safely natively depending on explicit Provider flags.
    """
    provider_type = os.getenv("SECRETS_PROVIDER", "env").lower()
    
    if provider_type == "vault":
        vault_addr = os.environ.get("VAULT_ADDR")
        role_id = os.environ.get("VAULT_APPROLE_ROLE_ID")
        secret_id = os.environ.get("VAULT_APPROLE_SECRET_ID")
        
        if not (vault_addr and role_id and secret_id):
            # Deliberately crash Boot natively if AppRole isn't provided!
            raise VaultConfigurationError("SECRETS_PROVIDER is 'vault' but VAULT_ADDR, VAULT_APPROLE_ROLE_ID or VAULT_APPROLE_SECRET_ID are missing.")

        # Do not trap connection errors natively here: allow it to crash the process violently per Security Guidelines
        return HashiCorpVaultProvider(vault_addr=vault_addr, role_id=role_id, secret_id=secret_id)
        
    elif provider_type == "env":
        # Block ENV usage in prod
        if os.getenv("ENVIRONMENT") == "production":
            raise VaultConfigurationError("CRITICAL: Cannot use 'env' secrets provider in 'production' environments.")
        return EnvFallbackProvider()
        
    else:
        raise VaultConfigurationError(f"Unknown SECRETS_PROVIDER '{provider_type}'")
