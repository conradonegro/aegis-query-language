import re

_HINT_ALLOWLIST = re.compile(r"^[a-zA-Z0-9 '\.,:!\?_\-\/\(\)=%]+$")
_MAX_HINTS = 5
_MAX_HINT_LEN = 200


def validate_hints(hints: list[str]) -> list[str]:
    """Validate a list of prompt hints against structural safety rules.

    Uses a character allowlist rather than a blacklist. Blacklists are
    incomplete — attackers use XML tags (<system>), HTML comments, and
    structural markers (---, ===) to inject new LLM context blocks. The
    allowlist permits only characters needed for legitimate business hints
    and cannot be bypassed by novel structural characters.

    Raises ValueError on the first violation. Returns hints unchanged if all
    pass. Safe to call on both backend-generated and external hints.
    """
    if len(hints) > _MAX_HINTS:
        raise ValueError(
            f"Too many hints: {len(hints)} supplied, max {_MAX_HINTS}."
        )
    for i, hint in enumerate(hints):
        if len(hint) > _MAX_HINT_LEN:
            raise ValueError(
                f"Hint {i} exceeds max length of {_MAX_HINT_LEN} characters."
            )
        if not _HINT_ALLOWLIST.match(hint):
            raise ValueError(
                f"Hint {i} contains disallowed characters. "
                f"Only alphanumeric text and basic punctuation are permitted."
            )
    return hints
