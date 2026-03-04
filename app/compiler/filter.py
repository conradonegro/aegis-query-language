import re

from app.compiler.models import FilteredSchema, UserIntent, RAGIncludedColumns
from app.steward import RegistrySchema, AbstractTableDef, AbstractColumnDef


class DeterministicSchemaFilter:
    """
    A lightweight, static filter that reduces a RegistrySchema to just the elements
    likely relevant to the UserIntent. Uses token overlap and substring matching.
    """

    def __init__(self, cutoff_threshold: int = 1):
        self.cutoff_threshold = cutoff_threshold

    @staticmethod
    def _tokenize(text: str) -> set[str]:
        """Normalizes and extracts alphanumeric vocabulary tokens."""
        # Lowercase, remove non-alphanumeric except spaces, split
        clean = re.sub(r'[^a-z0-9\s]', '', text.lower())
        # Drop common english stop words
        stop_words = {
            "select", "show", "get", "find", "all", "the", "a", "an",
            "and", "or", "of", "in", "to", "for", "with", "by", "is", "are", "do", "does"
        }
        return {word for word in clean.split() if word and word not in stop_words}

    @staticmethod
    def token_match_score(tokens_a: set[str], tokens_b: set[str]) -> int:
        return sum(
            1 for a in tokens_a for b in tokens_b
            if a == b or (len(a) > 3 and len(b) > 3 and (a in b or b in a))
        )

    def is_follow_up(self, intent: UserIntent, last_schema: FilteredSchema | None, full_schema: RegistrySchema | None = None) -> bool:
        """
        Determines strictly if the intent is a follow-up query relying on prior context.
        Checks intent tokens against BOTH the prior filtered schema AND the full registry
        schema to detect topic drift toward any table in the system, not just previously
        surfaced ones.
        """
        if not last_schema:
            return False

        intent_tokens = self._tokenize(intent.natural_language_query)
        if len(intent_tokens) == 0:
            return False

        # Build the combined table pool: prior schema + full registry (if provided)
        all_tables = list(last_schema.tables)
        if full_schema:
            prior_aliases = {t.alias for t in last_schema.tables}
            for table in full_schema.tables:
                if table.alias not in prior_aliases:
                    all_tables.append(table)

        # 1. Rule Priority: Check for ANY structural alias matches against all known tables.
        # If the user introduces any structural tokens, it's ALWAYS a fresh query.
        has_structural_match = False
        for table in all_tables:
            table_tokens = self._tokenize(table.alias) | self._tokenize(table.description)
            if self.token_match_score(intent_tokens, table_tokens) >= self.cutoff_threshold:
                has_structural_match = True
                break

            for col in table.columns:
                col_tokens = self._tokenize(col.alias) | self._tokenize(col.description)
                if self.token_match_score(intent_tokens, col_tokens) >= self.cutoff_threshold:
                    has_structural_match = True
                    break

            if has_structural_match:
                break

        if has_structural_match:
            return False

        # 2. Heuristic check: No structural matches AND token count < 8 implies follow-up.
        if len(intent_tokens) < 8:
            return True

        return False

    def filter_schema(self, intent: UserIntent, schema: RegistrySchema, included_columns: RAGIncludedColumns | None = None) -> FilteredSchema:

        intent_tokens = self._tokenize(intent.natural_language_query)
        forced_columns = set(included_columns.columns if included_columns else [])

        allowed_tables = []
        rejected_columns = {}

        # 1. First pass to find directly matched tables
        matched_tables = set()
        
        # 1a. RAG Matches are unconditionally promoted to root tables 
        # (e.g., 'users.name' strictly promotes 'users')
        for fcol in forced_columns:
            if "." in fcol:
                matched_tables.add(fcol.split(".")[0])
                
        for table in schema.tables:
            table_tokens = self._tokenize(table.alias) | self._tokenize(table.description)
            table_overlap = self.token_match_score(intent_tokens, table_tokens)
            
            col_overlap_total = 0
            for col in table.columns:
                col_tokens = self._tokenize(col.alias) | self._tokenize(col.description)
                col_overlap_total += self.token_match_score(intent_tokens, col_tokens)
            
            if table_overlap >= self.cutoff_threshold or col_overlap_total >= self.cutoff_threshold:
                matched_tables.add(table.alias)

        # 2. Add 1-degree augmented tables via relationships (ensure Joins can happen)
        augmented_tables = set(matched_tables)
        for rel in schema.relationships:
            if rel.source_table in matched_tables:
                augmented_tables.add(rel.target_table)
            if rel.target_table in matched_tables:
                augmented_tables.add(rel.source_table)
                
        # 3. Determine all columns involved in relationships to protect them
        rel_columns = {f"{r.source_table}.{r.source_column}" for r in schema.relationships} | \
                      {f"{r.target_table}.{r.target_column}" for r in schema.relationships}

        # 4. Filter structures down
        for table in schema.tables:
            if table.alias not in augmented_tables:
                continue

            table_tokens = self._tokenize(table.alias) | self._tokenize(table.description)
            table_overlap = self.token_match_score(intent_tokens, table_tokens)
            
            allowed_columns = []
            for col in table.columns:
                col_tokens = self._tokenize(col.alias) | self._tokenize(col.description)
                col_overlap = self.token_match_score(intent_tokens, col_tokens)
                
                full_col_name = f"{table.alias}.{col.alias}"
                
                # We keep the column if the column matches OR if the parent table strongly matches OR if it's a join key OR if explicitly forced by RAG
                if col_overlap >= self.cutoff_threshold or table_overlap >= self.cutoff_threshold or full_col_name in rel_columns or full_col_name in forced_columns:
                    allowed_columns.append(col)
                else:
                    rejected_columns[full_col_name] = "Low token overlap and not a join key or RAG match"
            
            if allowed_columns:
                filtered_table = AbstractTableDef(
                    alias=table.alias,
                    description=table.description,
                    columns=allowed_columns,
                    physical_target=table.physical_target
                )
                allowed_tables.append(filtered_table)

        # 5. Prune relationships where the source or target table was completely dropped.
        allowed_table_aliases = {t.alias for t in allowed_tables}
        allowed_relationships = [
            r for r in schema.relationships
            if r.source_table in allowed_table_aliases and r.target_table in allowed_table_aliases
        ]

        return FilteredSchema(
            version=schema.version,
            tables=allowed_tables,
            relationships=allowed_relationships,
            omitted_columns=rejected_columns
        )
