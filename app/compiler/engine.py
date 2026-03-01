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
from app.rag.interfaces import VectorStoreProtocol
from app.rag.models import RAGOutcome
from app.steward.models import RegistrySchema

class RAGUncertaintyError(Exception):
    """Raised when RAG returns an ambiguous match or no match and strict fallback is disabled."""
    pass


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
        self.vector_store: VectorStoreProtocol | None = None

    def set_vector_store(self, store: VectorStoreProtocol) -> None:
        self.vector_store = store

    async def compile(
        self, intent: UserIntent, schema: RegistrySchema, hints: PromptHints, explain: bool = False
    ) -> ExecutableQuery:
        """
        Executes the full pipeline.
        Raises TranslationError or SafetyViolationError on failure.
        """
        start = time.perf_counter()
        
        # 1. Scope Schema
        filtered_schema = self.schema_filter.filter_schema(intent, schema)
        
        # 1.5 Evaluate RAG 
        if self.vector_store:
            rag_result = self.vector_store.search(intent.natural_language_query, tenant_id="default_tenant", limit=5)
            
            if rag_result.outcome in (RAGOutcome.NO_MATCH, RAGOutcome.AMBIGUOUS_MATCH):
                raise RAGUncertaintyError(f"RAG lookup failed with strict policy outcome: {rag_result.outcome.value}. Reason: {rag_result.reason}")
                
            if rag_result.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH and rag_result.match:
                match_val = rag_result.match.categorical_value
                hints.column_hints.append(f"Always consider value '{match_val.value}' maps to abstract column '{match_val.abstract_column}'")
                hints.rag_provenance = {
                    "rag_outcome": rag_result.outcome.value,
                    "rag_matched_value": match_val.value,
                    "rag_abstract_column": match_val.abstract_column,
                    "rag_similarity_score": rag_result.match.similarity_score
                }
        
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
        
        # Build Explainability Traces if requested
        if explain:
            # Reconstruct RAG context explicitly 
            rag_trace = {"outcome": "NOT_EVALUATED", "matches": [], "scores": [], "reason": "No vector store or execution required"}
            if self.vector_store:
                if hints.rag_provenance:
                    rag_trace = {
                        "outcome": hints.rag_provenance["rag_outcome"],
                        "matches": [hints.rag_provenance["rag_matched_value"]],
                        "scores": [hints.rag_provenance.get("rag_similarity_score", 1.0)],
                        "reason": "Single High Confidence Match Injected"
                    }

            executable.explainability = {
                "rag": rag_trace,
                "schema_filter": {
                    "included_aliases": [i.alias for i in filtered_schema.active_identifiers],
                    "excluded_aliases": list(filtered_schema.omitted_identifiers.keys()),
                    "reasons": list(filtered_schema.omitted_identifiers.values())
                },
                "prompt": {
                    "system_prompt_redacted": True,
                    "user_prompt": prompt_envelope.user_prompt
                },
                "llm": {
                    "provider": llm_result.model_id,
                    "model": llm_result.model_id,
                    "latency_ms": llm_result.latency_ms
                },
                "translation": {
                    "llm_abstract_query": abstract_query.sql,
                    "parameterized_sql": executable.sql,
                    "parameters": executable.parameters
                }
            }
        
        return executable
