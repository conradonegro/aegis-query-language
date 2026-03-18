import os

from jinja2 import Environment, FileSystemLoader

from app.compiler.models import (
    ChatHistoryItem,
    FilteredSchema,
    PromptEnvelope,
    PromptHints,
    UserIntent,
)


class PromptBuilder:
    """
    Constructs the PromptEnvelope from a fixed template. All dynamic content
    injected by trusted internal pipeline stages only — never from raw external input.
    Strictly reads static Jinja2 templates.
    """
    def __init__(self, template_dir: str | None = None):
        if not template_dir:
             # Default to app/compiler/templates relative to this module
             template_dir = os.path.join(os.path.dirname(__file__), "templates")

        self.env = Environment(
             loader=FileSystemLoader(template_dir),
             autoescape=False # We are generating raw text blocks, not HTML
        )

    def build_prompt(
        self,
        intent: UserIntent,
        schema: FilteredSchema,
        hints: PromptHints,
        chat_history: list[ChatHistoryItem] | None = None,
    ) -> PromptEnvelope:
        """
        Renders the fixed template into a frozen PromptEnvelope. Template
        structure is static; content is supplied by trusted internal sources.
        """
        # 1. Load the fixed system instruction template (structure is static)
        template = self.env.get_template("system.jinja")

        # 2. Render the system block directly
        system_block = template.render(
             schema=schema,
             hints=hints,
        )

        # 3. We'll decompose the rendered text slightly to fit the discrete envelope,
        # or just hold the assembled prompt inside `system_instruction` / `user_prompt`.
        # For our Custom LLMGateway format, we structure it logically:

        # 4. Truncate Chat History to prevent context exhaustion
        # We'll retain the last 10 messages (5 turns)
        history = chat_history or []
        if len(history) > 10:
            history = history[-10:]

        return PromptEnvelope(
            system_instruction=system_block,
            user_prompt=intent.natural_language_query,
            chat_history=history,
        )
