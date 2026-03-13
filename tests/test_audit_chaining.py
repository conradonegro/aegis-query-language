import json

from app.audit.chaining import (
    compute_artifact_hmac_signature,
    compute_audit_row_hash,
    get_canonical_json,
    verify_hmac_signature,
)


def test_canonical_json_drift() -> None:
    """Serialize -> deserialize -> serialize -> must produce identical bytes."""
    payload = {
        "zulu": 100,
        "alpha": "hello",
        "nested": {
            "c": True,
            "b": None,
            "a": [3, 1, 2] # Lists should not be sorted natively, only dict keys
        }
    }

    # Pass 1: Canonicalize
    canonical_1 = get_canonical_json(payload)

    # Output must strictly sort keys and omit whitespace padding
    assert canonical_1 == (
        '{"alpha":"hello","nested":{"a":[3,1,2],"b":null,"c":true},"zulu":100}'
    )

    # Pass 2: Deserialize
    reloaded = json.loads(canonical_1)

    # Pass 3: Canonicalize again
    canonical_2 = get_canonical_json(reloaded)

    assert canonical_1 == canonical_2
    assert canonical_1.encode("utf-8") == canonical_2.encode("utf-8") # Identical bytes

def test_audit_row_hash_chaining() -> None:
    """Ensure determinism and previous hash binding is strictly enforced."""
    payload_a = '{"user":"admin"}'
    created_at = "2026-03-04T12:00:00Z"

    # Genesis Row
    hash_1 = compute_audit_row_hash("", payload_a, created_at)

    # Secondary Row
    payload_b = '{"settings":"fast"}'
    hash_2 = compute_audit_row_hash(hash_1, payload_b, created_at)

    assert hash_1 != hash_2

    # Third Row
    payload_c = '{"logout":true}'
    hash_3 = compute_audit_row_hash(hash_2, payload_c, created_at)

    # Verify manual modification of middle chain breaks validation
    # If a malicious user modified payload_b to payload_b_hacked
    payload_b_hacked = '{"settings":"slow"}'
    hash_2_hacked = compute_audit_row_hash(hash_1, payload_b_hacked, created_at)

    assert hash_2 != hash_2_hacked

    # Downstream broken chain calculation
    hash_3_broken = compute_audit_row_hash(hash_2_hacked, payload_c, created_at)
    assert hash_3 != hash_3_broken


def test_hmac_signature_timing_resistance() -> None:
    """Verify standard logic matches natively with constant time `compare_digest`."""
    signing_key = "secret_key_007"
    payload = '{"test":true}'

    signature = compute_artifact_hmac_signature(signing_key, payload)

    # Verify standard pass
    assert verify_hmac_signature(signing_key, payload, signature) is True

    # Verify invalid reject
    assert verify_hmac_signature(signing_key, payload, "bad_sig") is False

    # Verify wrong payload reject
    assert verify_hmac_signature(signing_key, '{"test":false}', signature) is False

    # Note: `compare_digest` is internally called by `verify_hmac_signature`,
    # which guarantees timing resistance.

    # Verify wrong signing key reject
    assert verify_hmac_signature("different_key", payload, signature) is False


def test_audit_row_hash_genesis_accepts_none_previous_hash() -> None:
    """
    The genesis row (first entry in a chain) passes None as previous_hash.
    The implementation coerces None to '' via `previous_hash or ''`, so both
    None and '' must produce the same hash.
    """
    payload = '{"event":"init"}'
    created_at = "2026-03-01T00:00:00Z"

    hash_none = compute_audit_row_hash(None, payload, created_at)  # type: ignore[arg-type]
    hash_empty = compute_audit_row_hash("", payload, created_at)

    assert hash_none == hash_empty


def test_canonical_json_handles_non_ascii() -> None:
    """Non-ASCII characters must survive a round-trip without escaping."""
    payload = {"name": "héllo wörld", "emoji": "🔐"}
    canonical = get_canonical_json(payload)
    # ensure_ascii=False means characters are preserved, not escaped
    assert "héllo wörld" in canonical
    assert "🔐" in canonical
    # Round-trip must be stable
    assert get_canonical_json(payload) == canonical
