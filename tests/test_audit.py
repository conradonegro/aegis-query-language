import json
import logging

import pytest

from app.audit import QueryAuditEvent
from app.audit.logger import JSONAuditLogger


@pytest.mark.asyncio
async def test_json_audit_logger_succeeds(caplog: pytest.LogCaptureFixture) -> None:
    logger = JSONAuditLogger()
    event = QueryAuditEvent(
        query_id="q1",
        tenant_id="t1",
        user_id="u1",
        natural_language_query="Count users",
        abstract_query="SELECT COUNT(*) FROM abstract_users",
        physical_query="SELECT COUNT(*) FROM auth.users",
        registry_version="v1",
        safety_engine_version="v1",
        abstract_query_hash="hash",
        latency_ms=105.5,
        prompt_tokens=50,
        completion_tokens=10,
        status="success",
        row_limit_applied=False
    )

    with caplog.at_level(logging.INFO, logger="aegis.audit"):
        await logger.record(event)

    assert len(caplog.records) == 1
    record = caplog.records[0]

    # Ensure it's valid JSON
    payload = json.loads(record.message)
    assert payload["query_id"] == "q1"
    assert payload["latency_ms"] == 105.5

@pytest.mark.asyncio
async def test_json_audit_logger_does_not_raise() -> None:
    logger = JSONAuditLogger()
    # Sending something completely invalid to the json dumps
    # This might be tricky because we use pydantic for the struct.
    # We will simulate a crash using a patched method.
    import typing
    class BadEvent:
        def model_dump(
            self, *args: list[typing.Any], **kwargs: dict[str, typing.Any]
        ) -> dict[str, typing.Any]:
            raise ValueError("Artificial explosion for test")

    # The logger should catch the serialization issue and NOT raise
    await logger.record(BadEvent()) # type: ignore
