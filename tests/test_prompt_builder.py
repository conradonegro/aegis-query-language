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


def test_prompt_renders_exhaustive_label_for_low_cardinality() -> None:
    """A column with sample_values_exhaustive=True must render with the
    'Allowed values (complete list, case-sensitive)' label."""
    builder = PromptBuilder()
    intent = UserIntent(natural_language_query="test")
    schema = FilteredSchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="members",
                description="Club members",
                physical_target="public.members",
                columns=[
                    AbstractColumnDef(
                        alias="position",
                        description="Member position",
                        data_type="text",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="position",
                        sample_values=["President", "Vice President", "Member"],
                        sample_values_exhaustive=True,
                    ),
                ],
            )
        ],
        relationships=[],
        omitted_columns={},
    )
    hints = PromptHints(column_hints=[])
    envelope = builder.build_prompt(intent, schema, hints, chat_history=[])

    # The rendered label is unique (the rule wording uses quotes around
    # "Allowed values (complete list)"; the template has the longer form
    # `Allowed values (complete list, case-sensitive): ...`).
    assert (
        "Allowed values (complete list, case-sensitive):"
        in envelope.system_instruction
    )
    assert "Vice President" in envelope.system_instruction
    # The non-exhaustive rendered label must NOT appear for this column.
    assert "Example values (NOT exhaustive" not in envelope.system_instruction


def test_prompt_renders_non_exhaustive_label_for_high_cardinality() -> None:
    """A column with sample_values_exhaustive=False must render with the
    prominent 'Example values (NOT exhaustive...)' warning label."""
    builder = PromptBuilder()
    intent = UserIntent(natural_language_query="test")
    schema = FilteredSchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="customers",
                description="Customers",
                physical_target="public.customers",
                columns=[
                    AbstractColumnDef(
                        alias="city",
                        description="Customer city",
                        data_type="text",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="city",
                        sample_values=["Prague", "Brno", "Ostrava"],
                        sample_values_exhaustive=False,
                    ),
                ],
            )
        ],
        relationships=[],
        omitted_columns={},
    )
    hints = PromptHints(column_hints=[])
    envelope = builder.build_prompt(intent, schema, hints, chat_history=[])

    assert "Example values (NOT exhaustive" in envelope.system_instruction
    assert "Prague" in envelope.system_instruction
    # The exhaustive rendered label must NOT appear for this column.
    assert (
        "Allowed values (complete list, case-sensitive):"
        not in envelope.system_instruction
    )


def test_prompt_omits_value_block_when_no_samples() -> None:
    """A column with sample_values=[] must render NEITHER rendered label."""
    builder = PromptBuilder()
    intent = UserIntent(natural_language_query="test")
    schema = FilteredSchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="t",
                description="t",
                physical_target="public.t",
                columns=[
                    AbstractColumnDef(
                        alias="c",
                        description="c",
                        data_type="text",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="c",
                        sample_values=[],
                        sample_values_exhaustive=False,
                    ),
                ],
            )
        ],
        relationships=[],
        omitted_columns={},
    )
    hints = PromptHints(column_hints=[])
    envelope = builder.build_prompt(intent, schema, hints, chat_history=[])

    # Neither rendered value label should appear (the rule 10 wording mentions
    # both phrases, so we check for the rendered template forms specifically).
    assert "Example values (NOT exhaustive" not in envelope.system_instruction
    assert (
        "Allowed values (complete list, case-sensitive):"
        not in envelope.system_instruction
    )


def test_prompt_relationships_render_as_join_templates() -> None:
    """Relationships must render as ready-to-paste JOIN templates so the LLM
    is biased toward explicit JOIN syntax over comma-separated FROM clauses.
    """
    from app.steward import AbstractRelationshipDef

    builder = PromptBuilder()
    intent = UserIntent(natural_language_query="test")
    schema = FilteredSchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="yearmonth",
                description="Monthly billing periods",
                physical_target="public.yearmonth",
                columns=[
                    AbstractColumnDef(
                        alias="customerid",
                        description="FK",
                        data_type="integer",
                        safety=SafetyClassification(
                            join_participation_allowed=True
                        ),
                        physical_target="customerid",
                    ),
                ],
            ),
            AbstractTableDef(
                alias="customers",
                description="Customers",
                physical_target="public.customers",
                columns=[
                    AbstractColumnDef(
                        alias="customerid",
                        description="PK",
                        data_type="integer",
                        safety=SafetyClassification(
                            join_participation_allowed=True
                        ),
                        physical_target="customerid",
                    ),
                ],
            ),
        ],
        relationships=[
            AbstractRelationshipDef(
                source_table="yearmonth",
                source_column="customerid",
                target_table="customers",
                target_column="customerid",
            )
        ],
        omitted_columns={},
    )
    hints = PromptHints(column_hints=[])
    envelope = builder.build_prompt(intent, schema, hints, chat_history=[])

    assert (
        "JOIN customers ON yearmonth.customerid = customers.customerid"
        in envelope.system_instruction
    )
    # The arrow form must NOT appear — it was the misleading old format
    assert (
        "yearmonth.customerid -> customers.customerid"
        not in envelope.system_instruction
    )


def test_prompt_rule_7_contains_counter_example() -> None:
    """Rule 7 must contain the concrete WRONG/CORRECT counter-example pair."""
    builder = PromptBuilder()
    intent = UserIntent(natural_language_query="test")
    schema = FilteredSchema(
        version="1.0", tables=[], relationships=[], omitted_columns={}
    )
    hints = PromptHints(column_hints=[])
    envelope = builder.build_prompt(intent, schema, hints, chat_history=[])

    assert "WRONG:" in envelope.system_instruction
    assert "CORRECT:" in envelope.system_instruction
    assert "FROM yearmonth, customers" in envelope.system_instruction
    assert "FROM yearmonth JOIN customers" in envelope.system_instruction
