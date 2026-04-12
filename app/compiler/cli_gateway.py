"""LLM gateway that shells out to the ``claude`` CLI in pipe mode.

Uses ``claude -p --model <model> --system-prompt <file> --output-format json
--tools "" --bare`` to generate SQL, billing against the user's Claude
subscription instead of prepaid API credits.

Concurrency: each ``generate()`` call spawns a subprocess. The CLI starts
fresh each time (no connection pooling), so this is slower than the HTTP
gateways (~5-10s per question vs ~1-2s). Designed for benchmark runs where
cost matters more than speed.
"""

import asyncio
import json
import logging
import os
import tempfile
import time

from app.compiler.interfaces import LLMGatewayProtocol
from app.compiler.models import LLMResult, PromptEnvelope
from app.compiler.ollama import LLMGenerationError

logger = logging.getLogger(__name__)

_CLAUDE_BIN = os.getenv("CLAUDE_CLI_PATH", "claude")


class ClaudeCLIGateway:
    """LLM gateway that invokes the ``claude`` CLI for each request."""

    def __init__(self, model: str = "haiku") -> None:
        self.model = model

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        start = time.perf_counter()

        # Write the system prompt to a temp file to avoid shell-quoting
        # issues with long multi-line prompts that contain special chars.
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False
        ) as sys_f:
            sys_f.write(prompt.system_instruction)
            sys_prompt_path = sys_f.name

        # Build the user message. For multi-turn (chat history), prepend
        # the history to the user prompt — the CLI only takes one prompt.
        user_text = prompt.user_prompt
        if prompt.chat_history:
            history_lines = [
                f"[{m.role}]: {m.content}" for m in prompt.chat_history
            ]
            user_text = "\n".join(history_lines) + "\n\n" + user_text

        cmd = [
            _CLAUDE_BIN,
            "-p",
            "--model", self.model,
            "--system-prompt-file", sys_prompt_path,
            "--output-format", "json",
            "--tools", "",
            "--bare",
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(input=user_text.encode("utf-8")),
                timeout=120,
            )
        except TimeoutError as e:
            raise LLMGenerationError(
                "Claude CLI timed out after 120s.",
                raw_response="",
            ) from e
        except FileNotFoundError as e:
            raise LLMGenerationError(
                f"Claude CLI not found at '{_CLAUDE_BIN}'. "
                "Install with: npm install -g @anthropic-ai/claude-code",
                raw_response="",
            ) from e
        finally:
            os.unlink(sys_prompt_path)

        if proc.returncode != 0:
            err_text = stderr.decode("utf-8", errors="replace")[:500]
            raise LLMGenerationError(
                f"Claude CLI exited with code {proc.returncode}: {err_text}",
                raw_response=err_text,
            )

        raw_stdout = stdout.decode("utf-8", errors="replace")

        # --output-format json wraps the response in a JSON object with
        # a "result" field containing the assistant's text.
        try:
            envelope = json.loads(raw_stdout)
            text = envelope.get("result", raw_stdout)
        except json.JSONDecodeError:
            text = raw_stdout

        # The LLM is expected to return a JSON object like {"sql": "..."}
        # (with the JSON prefill pattern, the opening "{" is already there).
        # Ensure the text starts with "{" for the downstream JSON parser.
        text = text.strip()
        if not text.startswith("{"):
            text = "{" + text

        elapsed = (time.perf_counter() - start) * 1000
        return LLMResult(
            raw_text=text,
            model_id=self.model,
            latency_ms=elapsed,
            prompt_tokens=0,
            completion_tokens=0,
        )


# Satisfy the protocol check at import time.
_: LLMGatewayProtocol = ClaudeCLIGateway()
