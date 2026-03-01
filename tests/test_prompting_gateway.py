
import pytest

from app.compiler import FilteredSchema, PromptEnvelope, PromptHints, UserIntent
from app.compiler.gateway import MockLLMGateway
from app.compiler.prompting import PromptBuilder
from app.steward import AbstractIdentifierDef, SafetyClassification


def test_prompt_builder() -> None:
    schema = FilteredSchema(
        version="v1.0.0",
        active_identifiers=[
            AbstractIdentifierDef(
                alias="users",
                description="User accounts",
                safety=SafetyClassification(allowed_in_select=True),
                physical_target="auth.users"
            )
        ],
        omitted_identifiers={}
    )
    intent = UserIntent(natural_language_query="Count all users")
    hints = PromptHints(column_hints=["Always consider active = true"])

    # Needs to be able to find the relative `templates` folder during pytest
    builder = PromptBuilder()
    envelope = builder.build_prompt(schema=schema, intent=intent, hints=hints)

    # Verify the envelope is built correctly
    assert isinstance(envelope, PromptEnvelope)
    assert envelope.user_prompt == "Count all users"

    # Verify Jinja rendering worked and contains abstract data but NO physical targets
    assert "users" in envelope.system_instruction
    assert "User accounts" in envelope.system_instruction
    assert "auth.users" not in envelope.system_instruction
    assert "Always consider active = true" in envelope.system_instruction

@pytest.mark.asyncio
async def test_mock_gateway() -> None:
    envelope = PromptEnvelope(
        system_instruction="System template",
        schema_context="",
        user_prompt="Count users",
        hints=""
    )

    gateway = MockLLMGateway(mock_response_sql="SELECT COUNT(*) FROM users")
    result = await gateway.generate(envelope)

    assert result.raw_text == "SELECT COUNT(*) FROM users"
    assert result.model_id == "mock-aegis-v1"
    assert result.latency_ms > 0
    assert result.prompt_tokens > 0
    assert result.completion_tokens > 0
