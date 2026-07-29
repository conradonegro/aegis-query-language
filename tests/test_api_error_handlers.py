"""DB execution errors must map to structured 400s, not bare 500s."""

import json
from typing import Any, cast

import pytest
from fastapi import Request
from sqlalchemy.exc import DBAPIError, ProgrammingError

from app.main import dbapi_error_handler


@pytest.mark.asyncio
async def test_dbapi_error_maps_to_structured_400() -> None:
    exc = ProgrammingError(
        "SELECT bad", {}, Exception('syntax error at or near "Date"')
    )
    resp = await dbapi_error_handler(cast(Request, cast(Any, None)), exc)
    assert resp.status_code == 400
    body = json.loads(bytes(resp.body))
    assert body["message"].startswith("Execution Error:")
    assert "syntax error" in body["message"]


@pytest.mark.asyncio
async def test_generic_dbapi_error_maps_to_structured_400() -> None:
    """SQLAlchemy's asyncpg dialect leaves several asyncpg exceptions as a
    plain DBAPIError rather than mapping them to ProgrammingError/DataError,
    so they escaped the handler and surfaced as bare 500s. Observed in the
    benchmark: InvalidTextRepresentationError from CAST('18:55.797' AS
    numeric), and multi-statement input."""
    exc = DBAPIError(
        "SELECT bad",
        {},
        Exception('invalid input syntax for type numeric: "18:55.797"'),
    )
    resp = await dbapi_error_handler(cast(Request, cast(Any, None)), exc)
    assert resp.status_code == 400
    body = json.loads(bytes(resp.body))
    assert body["message"].startswith("Execution Error:")
    assert "invalid input syntax" in body["message"]


@pytest.mark.asyncio
async def test_statement_timeout_maps_to_structured_400() -> None:
    """A query cancelled by statement_timeout is a property of the generated
    query, not an application fault."""
    exc = DBAPIError(
        "SELECT slow",
        {},
        Exception("canceling statement due to statement timeout"),
    )
    resp = await dbapi_error_handler(cast(Request, cast(Any, None)), exc)
    assert resp.status_code == 400
    body = json.loads(bytes(resp.body))
    assert "statement timeout" in body["message"]


@pytest.mark.asyncio
async def test_invalidated_connection_stays_5xx() -> None:
    """A dropped connection is genuine infrastructure failure and must not be
    reported to the caller as a query problem."""
    exc = DBAPIError("SELECT x", {}, Exception("connection was closed"))
    exc.connection_invalidated = True
    resp = await dbapi_error_handler(cast(Request, cast(Any, None)), exc)
    assert resp.status_code >= 500
