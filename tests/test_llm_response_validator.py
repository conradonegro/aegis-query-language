"""
Tests for app/compiler/models.LLMQueryResponse.validate_refusal_contract

This is a critical security invariant: the model must enforce that
a response is either a valid SQL result or an unambiguous refusal —
never both, never neither.
"""
import pytest
from pydantic import ValidationError

from app.compiler.models import LLMQueryResponse


def test_valid_sql_response() -> None:
    r = LLMQueryResponse(sql="SELECT COUNT(*) FROM users")
    assert r.sql == "SELECT COUNT(*) FROM users"
    assert r.refused is False


def test_valid_refusal_with_reason() -> None:
    r = LLMQueryResponse(refused=True, reason="destructive intent detected")
    assert r.refused is True
    assert r.sql is None
    assert r.reason == "destructive intent detected"


def test_valid_refusal_without_reason() -> None:
    # reason is optional even on a refusal
    r = LLMQueryResponse(refused=True)
    assert r.refused is True
    assert r.reason is None


def test_refused_true_with_sql_is_invalid() -> None:
    """Ambiguous response: both refused and sql present must be rejected."""
    with pytest.raises(ValidationError, match="(?i)ambiguous"):
        LLMQueryResponse(refused=True, sql="SELECT 1")


def test_not_refused_with_no_sql_is_invalid() -> None:
    """If not refused, sql must be present and non-empty."""
    with pytest.raises(ValidationError, match="(?i)absent or empty"):
        LLMQueryResponse(refused=False)


def test_not_refused_with_empty_string_sql_is_invalid() -> None:
    """An empty sql string is treated as absent."""
    with pytest.raises(ValidationError, match="(?i)absent or empty"):
        LLMQueryResponse(refused=False, sql="")


def test_default_refused_is_false() -> None:
    r = LLMQueryResponse(sql="SELECT 1")
    assert r.refused is False


def test_whitespace_only_sql_passes_validator() -> None:
    """
    The validator checks `not self.sql` (falsy), which a whitespace-only string
    passes (it is truthy). Stripping is the engine's concern, not the model's.
    """
    r = LLMQueryResponse(refused=False, sql="   ")
    assert r.sql == "   "
