"""DB execution errors must map to structured 400s, not bare 500s."""

import json
from typing import Any, cast

import pytest
from fastapi import Request
from sqlalchemy.exc import ProgrammingError

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
