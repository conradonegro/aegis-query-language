import hashlib
import hmac
import json
import secrets
from typing import Any


def get_canonical_json(payload: dict[str, Any]) -> str:
    """
    Deterministically serializes a dictionary into a JSON string.
    Keys are strictly sorted natively preventing identical data representations
    from yielding disparate cryptographic hash footprints.
    """
    return json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def compute_audit_row_hash(previous_hash: str, canonical_payload: str, created_at_iso: str) -> str:
    """
    Calculates the strict SHA256 cryptographic chain hash tying an audit row to the previous event natively.
    """
    # previous_hash || '|' || canonical_payload || '|' || created_at
    # genesis row uses empty string for previous_hash natively.

    raw_material = f"{previous_hash or ''}|{canonical_payload}|{created_at_iso}"
    return hashlib.sha256(raw_material.encode("utf-8")).hexdigest()


def compute_artifact_hmac_signature(signing_key: str, canonical_payload: str) -> str:
    """
    Calculates an HMAC-SHA256 signature structurally binding the payload to the Active Key.
    """
    # signature = HMAC_SHA256(key, canonical_payload)
    # Output must be natively hex or base64. User specified hex via hdigest inside signature structure commonly.
    key_bytes = signing_key.encode("utf-8")
    payload_bytes = canonical_payload.encode("utf-8")

    return hmac.new(key_bytes, payload_bytes, hashlib.sha256).hexdigest()


def verify_hmac_signature(signing_key: str, canonical_payload: str, provided_signature: str) -> bool:
    """
    Secure constant-time evaluation of an artifact payload natively preventing timing attacks.
    """
    expected = compute_artifact_hmac_signature(signing_key, canonical_payload)
    return secrets.compare_digest(expected, provided_signature)
