import time
import uuid

from app.compiler.interfaces import (
    LLMGatewayProtocol,
    PromptBuilderProtocol,
    SafetyEngineProtocol,
    SchemaFilterProtocol,
    SQLParserProtocol,
    TranslatorProtocol,
)
from app.compiler.models import (
    AbstractQuery,
    ExecutableQuery,
    PromptHints,
    UserIntent,
)
from app.steward.models import RegistrySchema


class CompilerEngine:
    """
    Orchestrates the internal compilation pipeline from Natural Language to physical SQL.
    """
    def __init__(
        self,
        schema_filter: SchemaFilterProtocol,
        prompt_builder: PromptBuilderProtocol,
        llm_gateway: LLMGatewayProtocol,
        parser: SQLParserProtocol,
        safety_engine: SafetyEngineProtocol,
        translator: TranslatorProtocol,
    ):
        self.schema_filter = schema_filter
        self.prompt_builder = prompt_builder
        self.llm_gateway = llm_gateway
        self.parser = parser
        self.safety_engine = safety_engine
        self.translator = translator

    async def compile(
        self, intent: UserIntent, schema: RegistrySchema, hints: PromptHints
    ) -> ExecutableQuery:
        """
        Executes the full pipeline.
        Raises TranslationError or SafetyViolationError on failure.
        """
        start = time.perf_counter()
        
        # 1. Scope Schema
        filtered_schema = self.schema_filter.filter_schema(intent, schema)
        
        # 2. Build Prompt Envelope
        prompt_envelope = self.prompt_builder.build_prompt(intent, filtered_schema, hints)
        
        # 3. Call LLM
        llm_result = await self.llm_gateway.generate(prompt_envelope)
        abstract_query = AbstractQuery(sql=llm_result.raw_text)
        
        # 4. Parse 
        ast = self.parser.parse(abstract_query)
        
        # 5. Safety Validation
        validated_ast = self.safety_engine.validate(ast)
        
        # 6. Physical Translation
        executable = self.translator.translate(validated_ast, schema)
        
        # Decorate with metadata 
        executable.query_id = str(uuid.uuid4())
        executable.compilation_latency_ms = (time.perf_counter() - start) * 1000.0
        
        return executable
