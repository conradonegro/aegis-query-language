import hashlib
import json
import os
import re
import time
import uuid
from typing import Any

from pydantic import ValidationError
from sqlglot import exp as sqlglot_exp

from app.compiler.interfaces import (
    LLMGatewayProtocol,
    PromptBuilderProtocol,
    SafetyEngineProtocol,
    SchemaFilterProtocol,
    SQLParserProtocol,
    TranslatorProtocol,
)
from app.compiler.llm_factory import get_llm_gateway
from app.compiler.models import (
    AbstractQuery,
    ChatHistoryItem,
    ExecutableQuery,
    LLMQueryResponse,
    PromptHints,
    RAGIncludedColumns,
    SessionQueryContext,
    UserIntent,
)
from app.compiler.ollama import LLMGenerationError
from app.compiler.session_store import SessionStore
from app.rag.interfaces import VectorStoreProtocol
from app.rag.models import RAGOutcome, RAGResult
from app.steward import RegistrySchema


class RAGUncertaintyError(Exception):
    """Raised when RAG returns ambiguous or no match and strict fallback is disabled."""

    pass


class CompilerEngine:
    """
    Orchestrates the internal compilation pipeline from Natural Language
    to physical SQL.
    """

    def __init__(
        self,
        schema_filter: SchemaFilterProtocol,
        prompt_builder: PromptBuilderProtocol,
        llm_gateway: LLMGatewayProtocol,
        parser: SQLParserProtocol,
        safety_engine: SafetyEngineProtocol,
        translator: TranslatorProtocol,
    ) -> None:
        self.schema_filter = schema_filter
        self.prompt_builder = prompt_builder
        self.llm_gateway = llm_gateway
        self.parser = parser
        self.safety_engine = safety_engine
        self.translator = translator
        self._vector_stores: dict[str, VectorStoreProtocol] = {}
        self.session_store: SessionStore = SessionStore()

    def set_vector_store(self, store: VectorStoreProtocol, tenant_id: str) -> None:
        self._vector_stores[tenant_id] = store

    # ------------------------------------------------------------------
    # Public compilation entry point
    # ------------------------------------------------------------------

    async def compile(
        self,
        intent: UserIntent,
        schema: RegistrySchema,
        hints: PromptHints,
        tenant_id: str,
        explain: bool = False,
        chat_history: list[ChatHistoryItem] | None = None,
        provider_id: str | None = None,
        session_id: str | None = None,
    ) -> ExecutableQuery:
        """
        Executes the full pipeline.
        Raises TranslationError or SafetyViolationError on failure.
        """
        start = time.perf_counter()
        explain_context = self._init_explain_context(intent)

        try:
            prior_context = (
                await self.session_store.get(session_id) if session_id else None
            )
            is_follow_up = (
                prior_context is not None
                and prior_context.registry_version == schema.version
                and hasattr(self.schema_filter, "is_follow_up")
                and self.schema_filter.is_follow_up(
                    intent,
                    prior_context.last_filtered_schema,
                    full_schema=schema,
                )
            )
            explain_context["session"]["session_id"] = session_id
            explain_context["session"]["is_follow_up"] = is_follow_up
            explain_context["session"]["prior_schema_reused"] = is_follow_up

            included_cols = RAGIncludedColumns(columns=[])

            # RAG runs on every query — follow-up or not — so value hints are
            # always available to the LLM even when the schema is reused.
            self._apply_rag_hints(
                intent, hints, included_cols, explain_context, tenant_id
            )

            if is_follow_up and prior_context:
                filtered_schema = prior_context.last_filtered_schema
                explain_context["schema_filter"] = {
                    "included_aliases": [
                        f"{t.alias}.{c.alias}"
                        for t in filtered_schema.tables
                        for c in t.columns
                    ],
                    "excluded_aliases": list(
                        filtered_schema.omitted_columns.keys()
                    ),
                    "reasons": [
                        "Reused precisely from prior SessionQueryContext (Follow-up)"
                    ],
                }
            else:
                filtered_schema = self.schema_filter.filter_schema(
                    intent, schema, included_columns=included_cols
                )
                explain_context["schema_filter"] = {
                    "included_aliases": [
                        f"{t.alias}.{c.alias}"
                        for t in filtered_schema.tables
                        for c in t.columns
                    ],
                    "excluded_aliases": list(
                        filtered_schema.omitted_columns.keys()
                    ),
                    "reasons": list(filtered_schema.omitted_columns.values()),
                    "source_database_used": filtered_schema.source_database_used,
                    "source_database_mode": filtered_schema.source_database_mode,
                    "db_detection_scores": filtered_schema.db_detection_scores,
                }

            # 3. Build Prompt Envelope
            prompt_envelope = self.prompt_builder.build_prompt(
                intent, filtered_schema, hints, chat_history=chat_history
            )
            explain_context["prompt"]["raw_system"] = (
                prompt_envelope.system_instruction
            )
            explain_context["prompt"]["raw_user"] = prompt_envelope.user_prompt
            explain_context["prompt"]["system_prompt_redacted"] = False
            explain_context["prompt"]["chat_history_turns"] = (
                len(chat_history) if chat_history else 0
            )

            # 4. Call LLM
            gateway = (
                get_llm_gateway(provider_id) if provider_id else self.llm_gateway
            )
            llm_result = await gateway.generate(prompt_envelope)
            explain_context["llm"] = {
                "provider": llm_result.model_id,
                "model": llm_result.model_id,
                "latency_ms": llm_result.latency_ms,
                "raw_response": llm_result.raw_text,
            }

            # 5. Parse LLM response → abstract SQL
            abstract_sql = self._parse_llm_response(llm_result.raw_text)
            abstract_query = AbstractQuery(sql=abstract_sql)
            explain_context["translation"]["llm_abstract_query"] = (
                abstract_query.sql
            )

            # 6. Parse AST
            ast = self.parser.parse(abstract_query)

            # 7. Safety Validation
            validated_ast = self.safety_engine.validate(ast)

            # 8. Physical Translation
            abstract_query_hash = hashlib.sha256(
                abstract_sql.encode()
            ).hexdigest()
            scoped_schema = RegistrySchema(
                version=filtered_schema.version,
                tables=filtered_schema.tables,
                relationships=filtered_schema.relationships,
            )
            executable = self.translator.translate(
                validated_ast,
                scoped_schema,
                abstract_query_hash=abstract_query_hash,
                relationships=filtered_schema.relationships,
            )
            explain_context["translation"]["parameterized_sql"] = executable.sql
            explain_context["translation"]["parameters"] = executable.parameters
            explain_context["translation"]["abstract_query_hash"] = (
                abstract_query_hash
            )
            explain_context["translation"]["row_limit_applied"] = (
                executable.row_limit_applied
            )
            explain_context["translation"]["joins_validated"] = len(
                list(validated_ast.tree.find_all(sqlglot_exp.Join))
            )
            explain_context["translation"]["temporal_expressions_validated"] = len(
                list(validated_ast.tree.find_all(sqlglot_exp.Extract))
            )
            explain_context["translation_repairs"] = [
                r.model_dump() for r in executable.translation_repairs
            ]
            explain_context["compilation"]["registry_version"] = (
                executable.registry_version
            )
            explain_context["compilation"]["safety_engine_version"] = (
                executable.safety_engine_version
            )

            executable.abstract_sql = abstract_query.sql
            executable.llm_prompt_tokens = llm_result.prompt_tokens
            executable.llm_completion_tokens = llm_result.completion_tokens
            executable.query_id = str(uuid.uuid4())
            executable.source_database_used = filtered_schema.source_database_used
            executable.compilation_latency_ms = (
                (time.perf_counter() - start) * 1000.0
            )

            if explain:
                executable.explainability = explain_context

            if session_id:
                await self.session_store.set(
                    session_id,
                    SessionQueryContext(
                        last_filtered_schema=filtered_schema,
                        last_successful_sql=executable.sql,
                        timestamp=time.time(),
                        registry_version=schema.version,
                    ),
                )

            return executable

        except Exception as e:
            if explain:
                explain_context["llm"]["raw_response"] = getattr(
                    e, "raw_response", ""
                )
                e.explainability = explain_context  # type: ignore[attr-defined]
            raise e

    # ------------------------------------------------------------------
    # Explain context initialisation
    # ------------------------------------------------------------------

    @staticmethod
    def _init_explain_context(intent: UserIntent) -> dict[str, Any]:
        return {
            "session": {
                "session_id": None,
                "is_follow_up": False,
                "prior_schema_reused": False,
            },
            "rag": {
                "outcome": "NOT_EVALUATED",
                "matches": [],
                "scores": [],
                "reason": "No vector store or execution required",
            },
            "schema_filter": {
                "included_aliases": [],
                "excluded_aliases": [],
                "reasons": [],
            },
            "prompt": {
                "chat_history_turns": 0,
                "system_prompt_redacted": True,
                "user_prompt": intent.natural_language_query,
                "raw_system": "",
                "raw_user": "",
            },
            "llm": {
                "provider": "pending",
                "model": "pending",
                "latency_ms": 0.0,
                "raw_response": "",
            },
            "translation": {
                "llm_abstract_query": "",
                "abstract_query_hash": "",
                "parameterized_sql": "",
                "parameters": {},
                "row_limit_applied": False,
                "joins_validated": 0,
                "temporal_expressions_validated": 0,
            },
            "compilation": {
                "registry_version": "",
                "safety_engine_version": "",
            },
        }

    # ------------------------------------------------------------------
    # RAG hint injection
    # ------------------------------------------------------------------

    def _apply_rag_hints(
        self,
        intent: UserIntent,
        hints: PromptHints,
        included_cols: RAGIncludedColumns,
        explain_context: dict[str, Any],
        tenant_id: str,
    ) -> None:
        """Runs RAG lookup and injects matching column hints into PromptHints.

        By default RAG is a best-effort hint enrichment step: ambiguous or
        no-match outcomes let the query proceed without hints.

        When RAG_STRICT_MODE=true the engine fails closed: ambiguous and
        no-match outcomes raise RAGUncertaintyError (→ HTTP 400) so that
        queries whose categorical values cannot be confidently resolved are
        rejected rather than silently passed to the LLM with incomplete hints.
        """
        store = self._vector_stores.get(tenant_id)
        if not store:
            return

        strict = os.getenv("RAG_STRICT_MODE", "").lower() == "true"
        rag_result = store.search(
            intent.natural_language_query, tenant_id=tenant_id, limit=5
        )
        self._inject_rag_result(
            rag_result, hints, included_cols, strict
        )
        self._record_rag_explain(hints, explain_context)

    def _inject_rag_result(
        self,
        rag_result: RAGResult,
        hints: PromptHints,
        included_cols: RAGIncludedColumns,
        strict: bool,
    ) -> None:
        """Inject RAG matches into prompt hints and record provenance."""
        if (
            rag_result.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
            and rag_result.match
        ):
            mv = rag_result.match.categorical_value
            hints.column_hints.append(
                f"Always consider value '{mv.value}' maps to abstract"
                f" column '{mv.abstract_column}'"
            )
            hints.rag_provenance = {
                "rag_outcome": rag_result.outcome.value,
                "rag_matched_value": mv.value,
                "rag_abstract_column": mv.abstract_column,
                "rag_similarity_score": rag_result.match.similarity_score,
            }
            included_cols.columns.append(mv.abstract_column)
        elif (
            rag_result.outcome == RAGOutcome.AMBIGUOUS_MATCH
            and rag_result.candidates
        ):
            if strict:
                raise RAGUncertaintyError(
                    f"Ambiguous RAG match for query — "
                    f"{len(rag_result.candidates)} candidate values found. "
                    "Refine your query or disable RAG_STRICT_MODE."
                )
            self._inject_ambiguous_hints(
                rag_result, hints, included_cols
            )
        else:
            if strict:
                raise RAGUncertaintyError(
                    f"No RAG match for query — "
                    f"{rag_result.reason or 'no categorical values found'}. "
                    "Refine your query or disable RAG_STRICT_MODE."
                )
            hints.rag_provenance = {
                "rag_outcome": rag_result.outcome.value,
                "rag_reason": rag_result.reason,
            }

    @staticmethod
    def _inject_ambiguous_hints(
        rag_result: RAGResult,
        hints: PromptHints,
        included_cols: RAGIncludedColumns,
    ) -> None:
        """Group ambiguous candidates by column and inject one hint each."""
        by_col: dict[str, list[str]] = {}
        for m in (rag_result.candidates or [])[:10]:
            col = m.categorical_value.abstract_column
            by_col.setdefault(col, []).append(m.categorical_value.value)
        all_candidates: list[str] = []
        all_columns: list[str] = []
        for col, vals in by_col.items():
            hints.column_hints.append(
                f"Possible values for column '{col}':"
                f" {', '.join(repr(v) for v in vals)}"
            )
            included_cols.columns.append(col)
            all_candidates.extend(vals)
            all_columns.append(col)
        hints.rag_provenance = {
            "rag_outcome": rag_result.outcome.value,
            "rag_abstract_column": all_columns[0] if all_columns else "",
            "rag_candidates": all_candidates,
            "rag_reason": rag_result.reason,
        }

    @staticmethod
    def _record_rag_explain(
        hints: PromptHints, explain_context: dict[str, Any]
    ) -> None:
        """Populate the explainability dict from RAG provenance."""
        if not hints.rag_provenance:
            return
        prov = hints.rag_provenance
        if "rag_matched_value" in prov:
            matches = [prov["rag_matched_value"]]
            scores = [prov["rag_similarity_score"]]
            reason = "Single High Confidence Match Injected"
        elif "rag_candidates" in prov:
            matches = prov["rag_candidates"]
            scores = []
            reason = prov.get("rag_reason", "Ambiguous candidates injected")
        else:
            matches = []
            scores = []
            reason = prov.get("rag_reason", "")
        explain_context["rag"] = {
            "outcome": prov.get("rag_outcome", "UNKNOWN"),
            "matches": matches,
            "scores": scores,
            "reason": reason,
        }

    # ------------------------------------------------------------------
    # LLM response parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_llm_response(raw_text: str) -> str:
        """
        Extracts abstract SQL from the raw LLM output.

        All supported gateways must return the structured JSON envelope:
            {"sql": "<abstract SQL>", "refused": false}
        or
            {"refused": true, "reason": "<explanation>"}

        Markdown JSON fencing (```json ... ```) is unwrapped before parsing.
        Non-JSON output is rejected — there is no plain-SQL fallback.
        """
        raw = raw_text.strip()

        json_match = re.search(r"```json\s*(.*?)\s*```", raw, re.DOTALL)
        if json_match:
            raw = json_match.group(1).strip()
        elif raw.startswith("```") and raw.endswith("```"):
            raw = re.sub(r"^```(\w+)?\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw).strip()

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            raise LLMGenerationError(
                "LLM response is not valid JSON. All supported gateways must "
                "return the structured JSON envelope.",
                raw_response=raw,
            ) from e

        if not isinstance(payload, dict):
            raise LLMGenerationError(
                f"LLM response JSON must be an object, got {type(payload).__name__}.",
                raw_response=raw,
            )

        try:
            llm_response = LLMQueryResponse.model_validate(payload)
        except ValidationError as e:
            raise LLMGenerationError(
                f"Invalid LLM response structure: {e}",
                raw_response=raw,
            ) from e

        if llm_response.refused:
            raise LLMGenerationError(
                f"Request refused:"
                f" {llm_response.reason or 'destructive or modifying intent'}.",
                raw_response=raw,
            )
        return (llm_response.sql or "").strip()
