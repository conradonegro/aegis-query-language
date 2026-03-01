import json
import logging

from app.audit.models import QueryAuditEvent

# Configure the logger format to cleanly just output the message block
logger = logging.getLogger("aegis.audit")
logger.setLevel(logging.INFO)

class JSONAuditLogger:
    """
    Serializes QueryAuditEvent's and outputs them as single JSON logs.
    Designed to never raise serialization errors and remain decoupled.
    """

    async def record(self, event: QueryAuditEvent) -> None:
        """Fully serializes and logs the audit event without failing."""
        try:
            # We serialize models natively using pydantic's model_dump
            data = event.model_dump(exclude_none=True)

            # Serialize the dictionary to a single JSON line
            json_line = json.dumps(data, ensure_ascii=False)

            logger.info(json_line)
        except Exception as e:
            # We fall back to standard error logger if extreme serialization
            # failure happens but NEVER raise exceptions that bubble up to 
            # disrupt the user API flow.
            # Using repr string fallback to capture what happened
            fallback = (
                f"AUDIT SERIALIZATION FAILURE ({str(e)}). "
                f"Event payload: {repr(event)}"
            )
            logger.error(fallback)
