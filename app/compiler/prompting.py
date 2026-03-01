import os

from jinja2 import Environment, FileSystemLoader

from app.compiler.models import FilteredSchema, PromptEnvelope, PromptHints, UserIntent


class PromptBuilder:
    """
    Constructs the secure, immutable PromptEnvelope required by the LLMGateway.
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
        self, intent: UserIntent, schema: FilteredSchema, hints: PromptHints
    ) -> PromptEnvelope:
        """
        Renders the static template into an immutable PromptEnvelope.
        """
        # 1. Load the immutable static system instruction template
        template = self.env.get_template("system.jinja")

        # 2. Render the system block directly
        system_block = template.render(
             schema=schema,
             intent=intent,
             hints=hints
        )

        # 3. We'll decompose the rendered text slightly to fit the discrete envelope,
        # or just hold the assembled prompt inside `system_instruction` / `user_prompt`.
        # For our Custom LLMGateway format, we structure it logically:

        return PromptEnvelope(
            system_instruction=system_block,
            # We explicitly pass user intent so the gateway can map it to a user message
            user_prompt=intent.natural_language_query,
            # We omit schema_context and hints as distinct strings
            # since we templated them into the system instruction
            # for tight coupling.
            schema_context="",
            hints=""
        )
