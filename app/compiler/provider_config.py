"""
Server-side LLM provider governance.

Controls which LLM providers query clients are permitted to request via the
``provider_id`` field.  Clients may only select providers that the operator
has explicitly allowed; all other requests are rejected with HTTP 400 before
any data leaves the system.

Environment variables
---------------------
ALLOWED_LLM_PROVIDERS
    Comma-separated list of provider prefixes the server will accept from
    clients (e.g. ``openai,anthropic``).  Unset means only the default
    provider (``LLM_PROVIDER``) is permitted.  Set to ``*`` to allow all
    configured providers (development / testing only).

LLM_PROVIDER
    The default provider used when no ``provider_id`` is supplied.
    Implicitly added to the allowlist when ``ALLOWED_LLM_PROVIDERS`` is unset.
"""

import os

# The complete set of provider prefixes the factory understands.
# Kept here so provider_config is the single source of truth; llm_factory
# imports this set rather than maintaining its own parallel list.
KNOWN_PROVIDERS: frozenset[str] = frozenset(
    {"ollama", "openai", "anthropic", "google", "xai", "cli"}
)


class ProviderNotAllowedError(Exception):
    """Raised when a client requests a provider not in the server allowlist."""

    def __init__(self, requested: str) -> None:
        super().__init__(requested)
        self.requested = requested


class MalformedProviderIdError(ValueError):
    """Raised when a provider_id string cannot be parsed."""

    pass


def parse_provider_id(raw: str | None) -> str:
    """
    Validates and normalises a ``provider_id`` string.

    Accepted formats: ``"openai"``, ``"openai:gpt-4o"``.
    Rejected: empty, ``"openai:"``, ``":model"``, ``"openai::gpt"``,
    more than one colon, unknown prefix.

    Returns the normalised string (lower-cased, stripped).
    Raises :exc:`MalformedProviderIdError` for any invalid input.
    """
    if not raw or not raw.strip():
        raise MalformedProviderIdError("provider_id must not be empty.")

    normalised = raw.strip().lower()
    parts = normalised.split(":")

    if len(parts) > 2 or any(not p for p in parts):
        raise MalformedProviderIdError(
            f"Malformed provider_id: '{raw}'. "
            "Expected format: 'provider' or 'provider:model'."
        )

    prefix = parts[0]
    if prefix not in KNOWN_PROVIDERS:
        raise MalformedProviderIdError(
            f"Unknown provider prefix '{prefix}'. "
            f"Supported: {sorted(KNOWN_PROVIDERS)}."
        )

    return normalised


def get_allowed_providers() -> set[str] | None:
    """
    Returns the set of allowed provider prefixes, or ``None`` if all are
    permitted (explicit ``*`` opt-in).

    - ``ALLOWED_LLM_PROVIDERS`` unset → ``{default_provider}`` only.
    - ``ALLOWED_LLM_PROVIDERS=openai,anthropic`` → those two prefixes.
    - ``ALLOWED_LLM_PROVIDERS=*`` → unrestricted (dev / testing).
    """
    raw = os.getenv("ALLOWED_LLM_PROVIDERS", "").strip()
    if raw == "*":
        return None
    if raw:
        return {p.strip().lower() for p in raw.split(",") if p.strip()}
    # Default: only the operator-configured provider
    default = os.getenv("LLM_PROVIDER", "ollama").split(":")[0].lower()
    return {default}


def assert_provider_allowed(provider_id: str) -> None:
    """
    Raises :exc:`ProviderNotAllowedError` if *provider_id* (already normalised
    by :func:`parse_provider_id`) is not in the server allowlist.
    """
    allowed = get_allowed_providers()
    if allowed is None:
        return
    prefix = provider_id.split(":")[0]
    if prefix not in allowed:
        raise ProviderNotAllowedError(provider_id)
