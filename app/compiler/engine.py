import hashlib
import time
import uuid
from typing import Any

from app.compiler.interfaces import (
    LLMGatewayProtocol,
    PromptBuilderProtocol,
    SafetyEngineProtocol,
    SchemaFilterProtocol,
    SQLParserProtocol,
    TranslatorProtocol,
)
from pydantic import ValidationError
from app.compiler.models import (
    AbstractQuery,
    ExecutableQuery,
    LLMQueryResponse,
    PromptHints,
    UserIntent,
    RAGIncludedColumns,
    ChatHistoryItem,
    SessionQueryContext,
)
from app.compiler.ollama import LLMGenerationError
from app.compiler.llm_factory import get_llm_gateway
from app.compiler.session_store import SessionStore
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
        self.session_store: SessionStore = SessionStore()

    def set_vector_store(self, store: VectorStoreProtocol) -> None:
        self.vector_store = store

    async def compile(
        self, intent: UserIntent, schema: RegistrySchema, hints: PromptHints, explain: bool = False, chat_history: list[ChatHistoryItem] | None = None, provider_id: str | None = None, session_id: str | None = None
    ) -> ExecutableQuery:
        """
        Executes the full pipeline.
        Raises TranslationError or SafetyViolationError on failure.
        """
        start = time.perf_counter()
        
        explain_context: dict[str, Any] = {
            "rag": {"outcome": "NOT_EVALUATED", "matches": [], "scores": [], "reason": "No vector store or execution required"},
            "schema_filter": {"included_aliases": [], "excluded_aliases": [], "reasons": []},
            "prompt": {"system_prompt_redacted": True, "user_prompt": intent.natural_language_query, "raw_system": "", "raw_user": ""},
            "llm": {"provider": "pending", "model": "pending", "latency_ms": 0.0, "raw_response": ""},
            "translation": {"llm_abstract_query": "", "parameterized_sql": "", "parameters": {}}
        }

        try:
            # Look up prior session
            prior_context = await self.session_store.get(session_id) if session_id else None
            is_follow_up = False
            
            # Check detector if applicable
            if prior_context and hasattr(self.schema_filter, "is_follow_up"):
                is_follow_up = self.schema_filter.is_follow_up(intent, prior_context.last_filtered_schema, full_schema=schema)

            included_cols = RAGIncludedColumns(columns=[])
            
            if is_follow_up and prior_context:
                filtered_schema = prior_context.last_filtered_schema
                explain_context["schema_filter"] = {
                    "included_aliases": [f"{t.alias}.{c.alias}" for t in filtered_schema.tables for c in t.columns],
                    "excluded_aliases": list(filtered_schema.omitted_columns.keys()),
                    "reasons": ["Reused precisely from prior SessionQueryContext (Follow-up)"]
                }
            else:
                # 1. Evaluate RAG First
                if self.vector_store:
                    rag_result = self.vector_store.search(intent.natural_language_query, tenant_id="default_tenant", limit=5)
                    
                    if rag_result.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH and rag_result.match:
                        match_val = rag_result.match.categorical_value
                        hints.column_hints.append(f"Always consider value '{match_val.value}' maps to abstract column '{match_val.abstract_column}'")
                        hints.rag_provenance = {
                            "rag_outcome": rag_result.outcome.value,
                            "rag_matched_value": match_val.value,
                            "rag_abstract_column": match_val.abstract_column,
                            "rag_similarity_score": rag_result.match.similarity_score
                        }
                        included_cols.columns.append(match_val.abstract_column)
                    else:
                        hints.rag_provenance = {
                            "rag_outcome": rag_result.outcome.value,
                            "rag_reason": rag_result.reason
                        }

                    if hints.rag_provenance:
                        explain_context["rag"] = {
                            "outcome": hints.rag_provenance.get("rag_outcome", "UNKNOWN"),
                            "matches": [hints.rag_provenance["rag_matched_value"]] if "rag_matched_value" in hints.rag_provenance else [],
                            "scores": [hints.rag_provenance["rag_similarity_score"]] if "rag_similarity_score" in hints.rag_provenance else [],
                            "reason": hints.rag_provenance.get("rag_reason", "Single High Confidence Match Injected")
                        }

                # 2. Scope Schema
                filtered_schema = self.schema_filter.filter_schema(intent, schema, included_columns=included_cols)
                explain_context["schema_filter"] = {
                    "included_aliases": [f"{t.alias}.{c.alias}" for t in filtered_schema.tables for c in t.columns],
                    "excluded_aliases": list(filtered_schema.omitted_columns.keys()),
                    "reasons": list(filtered_schema.omitted_columns.values())
                }
            
            # 3. Build Prompt Envelope
            prompt_envelope = self.prompt_builder.build_prompt(intent, filtered_schema, hints, chat_history=chat_history)
            explain_context["prompt"]["raw_system"] = prompt_envelope.system_instruction
            explain_context["prompt"]["raw_user"] = prompt_envelope.user_prompt
            explain_context["prompt"]["system_prompt_redacted"] = False
            
            # 4. Call LLM
            gateway = get_llm_gateway(provider_id) if provider_id else self.llm_gateway
            llm_result = await gateway.generate(prompt_envelope)
            explain_context["llm"] = {
                "provider": llm_result.model_id,
                "model": llm_result.model_id,
                "latency_ms": llm_result.latency_ms,
                "raw_response": llm_result.raw_text
            }
            
            import json
            import re
            
            raw_text = llm_result.raw_text.strip()
            
            # Robust JSON extraction: Strip potential markdown code blocks
            json_match = re.search(r'```json\s*(.*?)\s*```', raw_text, re.DOTALL)
            if json_match:
                raw_text = json_match.group(1).strip()
            elif raw_text.startswith("```") and raw_text.endswith("```"):
                # Handle generic backticks
                raw_text = re.sub(r'^```(\w+)?\n?', '', raw_text)
                raw_text = re.sub(r'\n?```$', '', raw_text).strip()
                
            try:
                payload = json.loads(raw_text)
                if isinstance(payload, dict):
                    try:
                        llm_response = LLMQueryResponse.model_validate(payload)
                    except ValidationError as e:
                        raise LLMGenerationError(
                            f"Invalid LLM response structure: {e}",
                            raw_response=raw_text,
                        )
                    if llm_response.refused:
                        raise LLMGenerationError(
                            f"Request refused: {llm_response.reason or 'destructive or modifying intent'}.",
                            raw_response=raw_text,
                        )
                    abstract_sql = (llm_response.sql or "").strip()
                else:
                    abstract_sql = str(payload)
            except json.JSONDecodeError:
                # Fallback to direct text if JSON parsing fails (e.g. if LLM ignored instructions)
                abstract_sql = raw_text
                if ";" in abstract_sql and len([s for s in abstract_sql.split(";") if s.strip()]) > 1:
                    raise LLMGenerationError("Multi-statement SQL detected in fallback path.", raw_response=raw_text)
                
            abstract_query = AbstractQuery(sql=abstract_sql)
            explain_context["translation"]["llm_abstract_query"] = abstract_query.sql

            # 5. Parse
            ast = self.parser.parse(abstract_query)

            # 6. Safety Validation
            validated_ast = self.safety_engine.validate(ast)

            # 7. Physical Translation
            # Hash the abstract query before translation so the provenance digest
            # reflects what the LLM actually produced, not the physical form.
            abstract_query_hash = hashlib.sha256(abstract_sql.encode()).hexdigest()
            executable = self.translator.translate(
                validated_ast, schema,
                abstract_query_hash=abstract_query_hash,
                relationships=filtered_schema.relationships,
            )
            explain_context["translation"]["parameterized_sql"] = executable.sql
            explain_context["translation"]["parameters"] = executable.parameters
            explain_context["translation_repairs"] = [r.model_dump() for r in executable.translation_repairs]

            # Decorate with metadata
            executable.abstract_sql = abstract_query.sql
            executable.query_id = str(uuid.uuid4())
            executable.compilation_latency_ms = (time.perf_counter() - start) * 1000.0

            if explain:
                executable.explainability = explain_context
                
            # Finalize Session state (only if compilation succeeds entirely without exception)
            if session_id:
                await self.session_store.set(session_id, SessionQueryContext(
                    last_filtered_schema=filtered_schema,
                    last_successful_sql=executable.sql,
                    timestamp=time.time(),
                ))
            
            return executable

        except Exception as e:
            if explain:
                explain_context["llm"]["raw_response"] = getattr(e, "raw_response", "")
                setattr(e, "explainability", explain_context)
            raise e
