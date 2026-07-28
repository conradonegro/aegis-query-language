"""Tests for discovery's RAG/sample configuration rules."""

import importlib.util
from pathlib import Path
from types import ModuleType

_SCRIPT = Path(__file__).parent.parent / "scripts" / "discover_metadata.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("discover_metadata", _SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_module()


def test_rag_enabled_for_entity_name_column() -> None:
    enabled, hint, limit, strategy, refresh = mod._rag_config(
        "text", False, 10_848, avg_len=14.0
    )
    assert enabled and hint == "high" and strategy == "distinct"
    assert limit == 10_848


def test_rag_disabled_for_document_column() -> None:
    """XML/prose blob columns (e.g. european_football_2 match.foulcommit)
    are documents, not categorical values — indexing them floods the
    store and the prompt."""
    enabled, *_ = mod._rag_config("text", False, 5_000, avg_len=2_400.0)
    assert not enabled


def test_rag_midcard_keeps_most_frequent() -> None:
    enabled, hint, limit, strategy, refresh = mod._rag_config(
        "text", False, 150, avg_len=12.0
    )
    assert enabled and strategy == "most_frequent" and limit == 100


def test_truncate_samples_caps_value_length() -> None:
    vals = ["short", "x" * 500]
    out = mod._truncate_samples(vals)
    assert out[0] == "short"
    assert len(out[1]) <= 83 and out[1].endswith("...")
