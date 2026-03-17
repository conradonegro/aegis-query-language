import functools
import re

from app.compiler.exceptions import (
    AmbiguousSourceDatabaseError,
    UnknownSourceDatabaseError,
)
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
    @functools.lru_cache(maxsize=8192)
    def _tokenize(text: str) -> frozenset[str]:
        """Normalizes and extracts alphanumeric vocabulary tokens.

        Decorated with lru_cache so each unique string (table alias, column
        description, etc.) is tokenized exactly once per process lifetime.
        Returns frozenset to prevent accidental mutation of cached results.
        """
        clean = re.sub(r"[^a-z0-9\s]", "", text.lower())
        stop_words = {
            # SQL / UI verbs
            "select", "show", "get", "find", "all", "list", "give",
            # Articles / prepositions / conjunctions
            "the", "a", "an", "and", "or", "of", "in", "to", "for",
            "with", "by", "its", "their",
            # Auxiliary verbs
            "is", "are", "do", "does", "did", "was", "were",
            "has", "have", "had",
            # Question words (never a table/column discriminator)
            "how", "many", "what", "which", "when", "where", "who",
            # Filler quantifiers
            "each", "per", "total", "count",
        }
        return frozenset(w for w in clean.split() if w and w not in stop_words)

    @staticmethod
    def token_match_score(
        tokens_a: frozenset[str], tokens_b: frozenset[str]
    ) -> int:
        return sum(
            1 for a in tokens_a for b in tokens_b
            if a == b or (len(a) > 3 and len(b) > 3 and (a in b or b in a))
        )

    # ------------------------------------------------------------------
    # Follow-up detection
    # ------------------------------------------------------------------

    def _tables_have_structural_match(
        self,
        intent_tokens: frozenset[str],
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
        intent_tokens: frozenset[str],
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
        intent_tokens: frozenset[str],
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
                        source_database=table.source_database,
                    )
                )

        return allowed_tables, rejected_columns

    @staticmethod
    def _apply_database_scope(
        schema: RegistrySchema,
        source_database: str,
    ) -> list[AbstractTableDef]:
        """Returns only tables belonging to the specified logical database."""
        return [t for t in schema.tables if t.source_database == source_database]

    def _detect_source_database(
        self,
        schema: RegistrySchema,
        intent_tokens: frozenset[str],
    ) -> tuple[str | None, dict[str, int]]:
        """
        Auto-detects the target source_database using combined table+column scoring.

        For each database, takes the MAX across tables of (table_score +
        max_col_score). Returns (winner, all_db_scores). winner is None when no
        database clears the cutoff threshold. Raises AmbiguousSourceDatabaseError
        (with scores) when multiple databases tie and no 2× margin winner exists.
        """
        db_scores: dict[str, int] = {}

        for table in schema.tables:
            db = table.source_database
            if db is None:
                continue
            table_tokens = (
                self._tokenize(table.alias) | self._tokenize(table.description)
            )
            table_score = self.token_match_score(intent_tokens, table_tokens)
            max_col_score = 0
            for col in table.columns:
                col_tokens = (
                    self._tokenize(col.alias) | self._tokenize(col.description)
                )
                max_col_score = max(
                    max_col_score,
                    self.token_match_score(intent_tokens, col_tokens),
                )
            # Combine table-level and best column-level signals: they are
            # independent semantic dimensions pointing at the same table.
            score = table_score + max_col_score
            if db not in db_scores or score > db_scores[db]:
                db_scores[db] = score

        candidates = {
            db: s for db, s in db_scores.items() if s >= self.cutoff_threshold
        }
        if not candidates:
            return None, db_scores

        sorted_dbs = sorted(candidates.items(), key=lambda x: x[1], reverse=True)
        if len(sorted_dbs) == 1:
            return sorted_dbs[0][0], db_scores

        best_db, best_score = sorted_dbs[0]
        _, second_score = sorted_dbs[1]
        if best_score >= 2 * second_score:
            return best_db, db_scores

        raise AmbiguousSourceDatabaseError(
            candidates=[db for db, _ in sorted_dbs],
            scores=dict(sorted_dbs),
        )

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
        resolved_db: str | None = None
        source_database_mode = "none"
        db_detection_scores: dict[str, int] = {}

        # Explicit database scope: restrict to matching tables before token scoring.
        if intent.source_database:
            candidate_tables = self._apply_database_scope(
                schema, intent.source_database
            )
            if not candidate_tables:
                raise UnknownSourceDatabaseError(intent.source_database)
            schema = RegistrySchema(
                version=schema.version,
                tables=candidate_tables,
                relationships=schema.relationships,
            )
            resolved_db = intent.source_database
            source_database_mode = "explicit"
        else:
            detected, db_detection_scores = self._detect_source_database(
                schema, intent_tokens
            )
            if detected:
                candidate_tables = self._apply_database_scope(schema, detected)
                schema = RegistrySchema(
                    version=schema.version,
                    tables=candidate_tables,
                    relationships=schema.relationships,
                )
                resolved_db = detected
                source_database_mode = "auto_detected"

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
            source_database_used=resolved_db,
            source_database_mode=source_database_mode,
            db_detection_scores=db_detection_scores,
        )
