"""Value normalization for RAG indexing.

Normalization rules (applied at index time and documented here):
- Unicode: NFC canonical form (unicodedata.normalize)
- Whitespace: strip leading/trailing
- Max length: 200 characters (longer values are rejected)
- Empty: None returned for empty strings after strip
- Case: preserved for display; callers that need case-insensitive comparison
  should call normalize() on both sides

These rules are stable across builds — same input always produces same output.
"""

import unicodedata

_MAX_VALUE_LENGTH = 200


def normalize(value: str) -> str | None:
    """Normalize a categorical value for indexing.

    Returns None if the value is empty after stripping.
    Raises ValueError if the value exceeds the max length.
    """
    normalized = unicodedata.normalize("NFC", value).strip()
    if not normalized:
        return None
    if len(normalized) > _MAX_VALUE_LENGTH:
        raise ValueError(
            f"Value exceeds max length of {_MAX_VALUE_LENGTH}: "
            f"{normalized[:40]!r}..."
        )
    return normalized
