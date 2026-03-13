import re

from app.compiler.models import FilteredSchema, RAGIncludedColumns, UserIntent
from app.steward import AbstractTableDef, RegistrySchema


class DeterministicSchemaFilter:
    """
    A lightweight, static filter that reduces a RegistrySchema to just the
    elements likely relevant to the UserIntent. Uses token overlap and
    substring matching.
    """

    def __init__(self, cutoff_threshold: int = 1) -> None:
        self.cutoff_threshold = cutoff_threshold

    @staticmethod
    def _tokenize(text: str) -> set[str]:
        """Normalizes and extracts alphanumeric vocabulary tokens."""
        clean = re.sub(r"[^a-z0-9\s]", "", text.lower())
        stop_words = {
            "select", "show", "get", "find", "all", "the", "a", "an",
            "and", "or", "of", "in", "to", "for", "with", "by",
            "is", "are", "do", "does",
        }
        return {w for w in clean.split() if w and w not in stop_words}

    @staticmethod
    def token_match_score(tokens_a: set[str], tokens_b: set[str]) -> int:
        return sum(
            1 for a in tokens_a for b in tokens_b
            if a == b or (len(a) > 3 and len(b) > 3 and (a in b or b in a))
        )

    # ------------------------------------------------------------------
    # Follow-up detection
    # ------------------------------------------------------------------

    def _tables_have_structural_match(
        self,
        intent_tokens: set[str],
        all_tables: list[AbstractTableDef],
    ) -> bool:
        """Returns True if any table or column has a token match with intent."""
        for table in all_tables:
            table_tokens = (
                self._tokenize(table.alias) | self._tokenize(table.description)
            )
            if (
                self.token_match_score(intent_tokens, table_tokens)
                >= self.cutoff_threshold
            ):
                return True
            for col in table.columns:
                col_tokens = (
                    self._tokenize(col.alias)
                    | self._tokenize(col.description)
                )
                if (
                    self.token_match_score(intent_tokens, col_tokens)
                    >= self.cutoff_threshold
                ):
                    return True
        return False

    def is_follow_up(
        self,
        intent: UserIntent,
        last_schema: FilteredSchema | None,
        full_schema: RegistrySchema | None = None,
    ) -> bool:
        """
        Determines strictly if the intent is a follow-up query relying on prior
        context. Checks intent tokens against BOTH the prior filtered schema AND
        the full registry schema to detect topic drift.
        """
        if not last_schema:
            return False

        intent_tokens = self._tokenize(intent.natural_language_query)
        if len(intent_tokens) == 0:
            return False

        all_tables: list[AbstractTableDef] = list(last_schema.tables)
        if full_schema:
            prior_aliases = {t.alias for t in last_schema.tables}
            for table in full_schema.tables:
                if table.alias not in prior_aliases:
                    all_tables.append(table)

        if self._tables_have_structural_match(intent_tokens, all_tables):
            return False

        return len(intent_tokens) < 8

    # ------------------------------------------------------------------
    # Schema filtering
    # ------------------------------------------------------------------

    def _find_matched_table_aliases(
        self,
        schema: RegistrySchema,
        intent_tokens: set[str],
        forced_columns: set[str],
    ) -> set[str]:
        """First pass: find table aliases that match the intent tokens."""
        matched_tables: set[str] = set()

        # RAG-forced columns unconditionally promote their parent table.
        for fcol in forced_columns:
            if "." in fcol:
                matched_tables.add(fcol.split(".")[0])

        for table in schema.tables:
            table_tokens = (
                self._tokenize(table.alias) | self._tokenize(table.description)
            )
            table_overlap = self.token_match_score(intent_tokens, table_tokens)
            col_overlap_total = sum(
                self.token_match_score(
                    intent_tokens,
                    self._tokenize(col.alias) | self._tokenize(col.description),
                )
                for col in table.columns
            )
            if (
                table_overlap >= self.cutoff_threshold
                or col_overlap_total >= self.cutoff_threshold
            ):
                matched_tables.add(table.alias)

        return matched_tables

    def _augment_with_relationships(
        self,
        schema: RegistrySchema,
        matched_tables: set[str],
    ) -> set[str]:
        """Adds 1-degree neighbours via relationships so JOINs can happen."""
        augmented = set(matched_tables)
        for rel in schema.relationships:
            if rel.source_table in matched_tables:
                augmented.add(rel.target_table)
            if rel.target_table in matched_tables:
                augmented.add(rel.source_table)
        return augmented

    def _compute_rel_columns(
        self,
        schema: RegistrySchema,
        augmented_tables: set[str],
    ) -> set[str]:
        """Returns FK columns for relationships where both endpoints are in scope."""
        rel_columns: set[str] = set()
        for r in schema.relationships:
            if (
                r.source_table in augmented_tables
                and r.target_table in augmented_tables
            ):
                if r.source_column:
                    rel_columns.add(f"{r.source_table}.{r.source_column}")
                if r.target_column:
                    rel_columns.add(f"{r.target_table}.{r.target_column}")
        return rel_columns

    def _build_filtered_tables(
        self,
        schema: RegistrySchema,
        intent_tokens: set[str],
        augmented_tables: set[str],
        rel_columns: set[str],
        forced_columns: set[str],
    ) -> tuple[list[AbstractTableDef], dict[str, str]]:
        """Filters tables/columns down to those relevant to the intent."""
        allowed_tables: list[AbstractTableDef] = []
        rejected_columns: dict[str, str] = {}

        for table in schema.tables:
            if table.alias not in augmented_tables:
                continue
            table_tokens = (
                self._tokenize(table.alias) | self._tokenize(table.description)
            )
            table_overlap = self.token_match_score(intent_tokens, table_tokens)

            allowed_columns = []
            for col in table.columns:
                col_tokens = (
                    self._tokenize(col.alias) | self._tokenize(col.description)
                )
                col_overlap = self.token_match_score(intent_tokens, col_tokens)
                full_col_name = f"{table.alias}.{col.alias}"
                if (
                    col_overlap >= self.cutoff_threshold
                    or table_overlap >= self.cutoff_threshold
                    or full_col_name in rel_columns
                    or full_col_name in forced_columns
                ):
                    allowed_columns.append(col)
                else:
                    rejected_columns[full_col_name] = (
                        "Low token overlap and not a join key or RAG match"
                    )

            if allowed_columns:
                allowed_tables.append(
                    AbstractTableDef(
                        alias=table.alias,
                        description=table.description,
                        columns=allowed_columns,
                        physical_target=table.physical_target,
                    )
                )

        return allowed_tables, rejected_columns

    def filter_schema(
        self,
        intent: UserIntent,
        schema: RegistrySchema,
        included_columns: RAGIncludedColumns | None = None,
    ) -> FilteredSchema:
        intent_tokens = self._tokenize(intent.natural_language_query)
        forced_columns = set(
            included_columns.columns if included_columns else []
        )

        matched_tables = self._find_matched_table_aliases(
            schema, intent_tokens, forced_columns
        )
        augmented_tables = self._augment_with_relationships(
            schema, matched_tables
        )
        rel_columns = self._compute_rel_columns(schema, augmented_tables)
        allowed_tables, rejected_columns = self._build_filtered_tables(
            schema, intent_tokens, augmented_tables, rel_columns, forced_columns
        )

        allowed_table_aliases = {t.alias for t in allowed_tables}
        allowed_relationships = [
            r for r in schema.relationships
            if (
                r.source_table in allowed_table_aliases
                and r.target_table in allowed_table_aliases
            )
        ]

        return FilteredSchema(
            version=schema.version,
            tables=allowed_tables,
            relationships=allowed_relationships,
            omitted_columns=rejected_columns,
        )
