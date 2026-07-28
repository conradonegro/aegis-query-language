"""Tests for scripts/cli_batch_generate.py — the subscription-auth batch
generator must be a neutral passing layer: transport failures never masquerade
as model refusals, and only validated envelopes reach responses.jsonl."""

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

_SCRIPT = Path(__file__).parent.parent / "scripts" / "cli_batch_generate.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("cli_batch_generate", _SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_module()


# ---------------------------------------------------------------------------
# Subscription-auth guard
# ---------------------------------------------------------------------------


def test_require_subscription_auth_refuses_when_api_key_set() -> None:
    with pytest.raises(SystemExit):
        mod.require_subscription_auth({"ANTHROPIC_API_KEY": "sk-ant-xxx"})


def test_require_subscription_auth_passes_without_api_key() -> None:
    mod.require_subscription_auth({"PATH": "/usr/bin"})


def test_require_subscription_auth_ignores_empty_api_key() -> None:
    mod.require_subscription_auth({"ANTHROPIC_API_KEY": ""})


# ---------------------------------------------------------------------------
# Envelope extraction
# ---------------------------------------------------------------------------


def test_extract_envelope_plain_json() -> None:
    env = mod.extract_envelope('{"sql": "SELECT 1"}')
    assert env == {"sql": "SELECT 1"}


def test_extract_envelope_fenced_json() -> None:
    env = mod.extract_envelope('```json\n{"sql": "SELECT 1"}\n```')
    assert env == {"sql": "SELECT 1"}


def test_extract_envelope_with_prose_around_json() -> None:
    text = 'Here is the query:\n{"sql": "SELECT 1"}\nHope this helps!'
    env = mod.extract_envelope(text)
    assert env == {"sql": "SELECT 1"}


def test_extract_envelope_accepts_genuine_refusal() -> None:
    text = '{"sql": null, "refused": true, "reason": "destructive intent"}'
    env = mod.extract_envelope(text)
    assert env is not None
    assert env["refused"] is True


def test_extract_envelope_rejects_garbage() -> None:
    assert mod.extract_envelope("I could not produce SQL for this.") is None


def test_extract_envelope_rejects_json_without_sql_or_refusal() -> None:
    assert mod.extract_envelope('{"answer": 42}') is None


def test_extract_envelope_rejects_non_string_sql() -> None:
    assert mod.extract_envelope('{"sql": 42}') is None


# ---------------------------------------------------------------------------
# Retry behaviour — transport failures retry; model output does not
# ---------------------------------------------------------------------------


def test_generate_one_retries_transport_failure_then_succeeds() -> None:
    attempts = 0

    def runner(system_prompt: str, user_prompt: str, model: str) -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise mod.TransportFailure("CLI timeout")
        return '{"sql": "SELECT 1"}'

    entry = {"key": "k1", "system_prompt": "s", "user_prompt": "u"}
    result = mod.generate_one(entry, "haiku", runner=runner, backoff_s=0.0)
    assert attempts == 3
    assert json.loads(result["response"]) == {"sql": "SELECT 1"}


def test_generate_one_raises_after_exhausting_retries() -> None:
    def runner(system_prompt: str, user_prompt: str, model: str) -> str:
        raise mod.TransportFailure("CLI error: boom")

    entry = {"key": "k1", "system_prompt": "s", "user_prompt": "u"}
    with pytest.raises(mod.TransportFailure):
        mod.generate_one(entry, "haiku", runner=runner, backoff_s=0.0)


def test_generate_one_retries_unparseable_model_output() -> None:
    attempts = 0

    def runner(system_prompt: str, user_prompt: str, model: str) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return "sorry, no JSON here"
        return '{"sql": "SELECT 1"}'

    entry = {"key": "k1", "system_prompt": "s", "user_prompt": "u"}
    result = mod.generate_one(entry, "haiku", runner=runner, backoff_s=0.0)
    assert attempts == 2
    assert json.loads(result["response"]) == {"sql": "SELECT 1"}


def test_generate_one_does_not_retry_genuine_refusal() -> None:
    attempts = 0

    def runner(system_prompt: str, user_prompt: str, model: str) -> str:
        nonlocal attempts
        attempts += 1
        return '{"sql": null, "refused": true, "reason": "no such data"}'

    entry = {"key": "k1", "system_prompt": "s", "user_prompt": "u"}
    result = mod.generate_one(entry, "haiku", runner=runner, backoff_s=0.0)
    assert attempts == 1
    assert json.loads(result["response"])["refused"] is True


# ---------------------------------------------------------------------------
# Failure routing — failures never land in responses.jsonl
# ---------------------------------------------------------------------------


def test_run_batch_routes_failures_to_failures_file(tmp_path: Path) -> None:
    responses_path = tmp_path / "responses.jsonl"
    failures_path = tmp_path / "failures.jsonl"

    def runner(system_prompt: str, user_prompt: str, model: str) -> str:
        if user_prompt == "bad":
            raise mod.TransportFailure("CLI timeout")
        return '{"sql": "SELECT 1"}'

    entries = [
        {"key": "good", "system_prompt": "s", "user_prompt": "ok"},
        {"key": "broken", "system_prompt": "s", "user_prompt": "bad"},
    ]
    mod.run_batch(
        entries,
        model="haiku",
        responses_path=responses_path,
        failures_path=failures_path,
        concurrency=1,
        runner=runner,
        backoff_s=0.0,
    )

    responses = [json.loads(line) for line in responses_path.read_text().splitlines()]
    failures = [json.loads(line) for line in failures_path.read_text().splitlines()]
    assert [r["key"] for r in responses] == ["good"]
    assert [f["key"] for f in failures] == ["broken"]
    assert "timeout" in failures[0]["error"]
