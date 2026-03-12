from app.compiler.base_gateway import OpenAICompatibleGateway


class OpenAILLMGateway(OpenAICompatibleGateway):
    """Gateway for OpenAI models (e.g. gpt-4o)."""

    def __init__(self, model: str = "gpt-4o", strict_json: bool = True) -> None:
        super().__init__(model, strict_json)

    @property
    def _provider_name(self) -> str:
        return "openai"

    @property
    def _endpoint_url(self) -> str:
        return "https://api.openai.com/v1/chat/completions"
