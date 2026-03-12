from app.compiler.base_gateway import OpenAICompatibleGateway


class XAILLMGateway(OpenAICompatibleGateway):
    """Gateway for xAI models (e.g. grok-2). xAI's API is OpenAI-compatible."""

    def __init__(self, model: str = "grok-2-latest", strict_json: bool = True) -> None:
        super().__init__(model, strict_json)

    @property
    def _provider_name(self) -> str:
        return "xai"

    @property
    def _endpoint_url(self) -> str:
        return "https://api.x.ai/v1/chat/completions"
