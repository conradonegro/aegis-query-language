from app.compiler.prompting import PromptBuilder
from app.compiler.models import UserIntent, FilteredSchema, PromptHints, ChatHistoryItem
from app.steward.models import RegistrySchema

def test_prompt_builder_history_truncation():
    builder = PromptBuilder()
    
    intent = UserIntent(natural_language_query="Current intent")
    schema = FilteredSchema(version="1.0", tables=[], relationships=[], omitted_columns={})
    hints = PromptHints(column_hints=[])
    
    # Create 15 messages (more than the 10 message limit)
    history = []
    for i in range(15):
        history.append(
            ChatHistoryItem(role="user" if i % 2 == 0 else "assistant", content=f"Message {i}")
        )
        
    envelope = builder.build_prompt(intent, schema, hints, chat_history=history)
    
    # Assert truncation to last 10
    assert len(envelope.chat_history) == 10
    assert envelope.chat_history[0].content == "Message 5"
    assert envelope.chat_history[-1].content == "Message 14"
