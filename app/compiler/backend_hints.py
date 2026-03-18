from dataclasses import dataclass
from datetime import datetime

from app.compiler.hints import validate_hints


@dataclass
class BackendHintContext:
    tenant_id: str
    now: datetime          # UTC, supplied by router at request time
    timezone: str = "UTC"  # future: per-tenant config


def build_backend_hints(ctx: BackendHintContext) -> list[str]:
    """Build trusted, server-generated hints for the LLM compilation context.

    Framed as orientation context, not rules. All generated hints are passed
    through validate_hints() as a safety net against accidental misconfiguration.
    """
    hints = [
        f"Current date/time (UTC): {ctx.now.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    ]
    return validate_hints(hints)
