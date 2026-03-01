import re

from app.compiler.models import FilteredSchema, UserIntent
from app.steward import RegistrySchema


class DeterministicSchemaFilter:
    """
    A lightweight, static filter that reduces a RegistrySchema to just the elements
    likely relevant to the UserIntent. Uses token overlap and substring matching.
    """

    def __init__(self, cutoff_threshold: int = 1):
        self.cutoff_threshold = cutoff_threshold

    def _tokenize(self, text: str) -> set[str]:
        """Normalizes and extracts alphanumeric vocabulary tokens."""
        # Lowercase, remove non-alphanumeric except spaces, split
        clean = re.sub(r'[^a-z0-9\s]', '', text.lower())
        # Drop common english stop words
        stop_words = {
            "select", "show", "get", "find", "all", "the", "a", "an",
            "and", "or", "of", "in", "to", "for", "with", "by"
        }
        return {word for word in clean.split() if word and word not in stop_words}

    def filter_schema(self, intent: UserIntent, schema: RegistrySchema) -> FilteredSchema:
        intent_tokens = self._tokenize(intent.natural_language_query)

        allowed_aliases = []
        rejected_aliases = {}

        for identifier in schema.identifiers:
            # Tokenize alias and description
            id_tokens = (
                self._tokenize(identifier.alias) |
                self._tokenize(identifier.description)
            )

            # Substring matching (e.g. "users" matches "user")
            overlap_score = 0
            for itok in intent_tokens:
                for dtok in id_tokens:
                    if itok in dtok or dtok in itok:
                        overlap_score += 1

            if overlap_score >= self.cutoff_threshold:
                # To protect the physical schema, we strip the `physical_target`
                # when moving to the FilteredSchema. The LLM NEVER sees the
                # physical target.
                allowed_aliases.append(identifier)
            else:
                rejected_aliases[identifier.alias] = "Low token overlap"

        # To maintain the `FilteredSchema` protocol shape:
        return FilteredSchema(
            version=schema.version,
            active_identifiers=allowed_aliases,
            omitted_identifiers=rejected_aliases
        )
