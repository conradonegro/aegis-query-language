"""Tests for the dump/replay gateways' prompt keying.

The key must cover BOTH prompts: schema context (relationships, hints)
renders into the system prompt, so a key derived from the user prompt
alone serves stale responses after any schema-context change.
"""

from app.compiler.file_gateway import _prompt_key


def test_prompt_key_changes_with_system_prompt() -> None:
    k1 = _prompt_key("system A", "same question")
    k2 = _prompt_key("system B", "same question")
    assert k1 != k2


def test_prompt_key_changes_with_user_prompt() -> None:
    k1 = _prompt_key("same system", "question A")
    k2 = _prompt_key("same system", "question B")
    assert k1 != k2


def test_prompt_key_stable_for_identical_prompts() -> None:
    assert _prompt_key("s", "u") == _prompt_key("s", "u")
