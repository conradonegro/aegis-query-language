from app.compiler.models import ChatHistoryItem, FilteredSchema, PromptHints, UserIntent
from app.compiler.prompting import PromptBuilder
from app.steward import AbstractColumnDef, AbstractTableDef, SafetyClassification


def test_prompt_builder_history_truncation() -> None:
    builder = PromptBuilder()

    intent = UserIntent(natural_language_query="Current intent")
    schema = FilteredSchema(
        version="1.0", tables=[], relationships=[], omitted_columns={}
    )
    hints = PromptHints(column_hints=[])

    # Create 15 messages (more than the 10 message limit)
    history = []
    for i in range(15):
        history.append(
            ChatHistoryItem(
                role="user" if i % 2 == 0 else "assistant",
                content=f"Message {i}",
            )
        )

    envelope = builder.build_prompt(intent, schema, hints, chat_history=history)

    # Assert truncation to last 10
    assert len(envelope.chat_history) == 10
    assert envelope.chat_history[0].content == "Message 5"
    assert envelope.chat_history[-1].content == "Message 14"


def test_prompt_builder_renders_column_data_type() -> None:
    """Column data_type must appear in the rendered system prompt."""
    builder = PromptBuilder()
    intent = UserIntent(natural_language_query="test")
    schema = FilteredSchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="users",
                description="Users table",
                physical_target="public.users",
                columns=[
                    AbstractColumnDef(
                        alias="name",
                        description="User name",
                        data_type="text",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="full_name",
                    ),
                    AbstractColumnDef(
                        alias="created_at",
                        description="Creation date",
                        data_type="timestamp",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="created_at",
                    ),
                ],
            )
        ],
        relationships=[],
        omitted_columns={},
    )
    hints = PromptHints(column_hints=[])

    envelope = builder.build_prompt(intent, schema, hints, chat_history=[])

    assert "text" in envelope.system_instruction
    assert "timestamp" in envelope.system_instruction
