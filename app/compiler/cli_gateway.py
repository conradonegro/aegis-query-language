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
import time

from app.compiler.interfaces import LLMGatewayProtocol
from app.compiler.models import LLMResult, PromptEnvelope
from app.compiler.ollama import LLMGenerationError

logger = logging.getLogger(__name__)

_CLAUDE_BIN = os.getenv("CLAUDE_CLI_PATH", "claude")


def _strip_markdown_fences(text: str) -> str:
    """Remove markdown code fences the CLI sometimes wraps around JSON."""
    text = text.strip()
    if text.startswith("```json"):
        text = text[len("```json"):].strip()
    if text.startswith("```"):
        text = text[len("```"):].strip()
    if text.endswith("```"):
        text = text[:-len("```")].strip()
    return text


def _parse_cli_envelope(raw: str) -> tuple[str, int, int]:
    """Parse the CLI JSON envelope. Returns (text, prompt_tokens, completion_tokens)."""
    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError as e:
        raise LLMGenerationError(
            f"Claude CLI returned non-JSON output: {raw[:200]}",
            raw_response=raw,
        ) from e

    if envelope.get("is_error"):
        raise LLMGenerationError(
            f"Claude CLI error: {envelope.get('result', 'unknown')}",
            raw_response=raw,
        )

    text = _strip_markdown_fences(envelope.get("result", ""))
    if not text.startswith("{"):
        text = "{" + text

    usage = envelope.get("usage", {})
    return text, usage.get("input_tokens", 0), usage.get("output_tokens", 0)


class ClaudeCLIGateway:
    """LLM gateway that invokes the ``claude`` CLI for each request."""

    # Map short aliases to full model names that bill to the Claude
    # subscription (not API credits). The bare "haiku" alias routes
    # through API billing; the full name routes through subscription.
    _MODEL_MAP: dict[str, str] = {
        "haiku": "claude-haiku-4-5-20251001",
        "sonnet": "sonnet",
        "opus": "opus",
    }

    def __init__(self, model: str = "haiku") -> None:
        self.model = self._MODEL_MAP.get(model, model)

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        start = time.perf_counter()
        user_text = self._build_user_text(prompt)

        cmd = [
            _CLAUDE_BIN, "-p",
            "--model", self.model,
            "--system-prompt", prompt.system_instruction,
            "--output-format", "json",
            "--permission-mode", "acceptEdits",
            "--no-session-persistence",
        ]

        # Ensure HOME is set so the CLI can find its OAuth tokens.
        env = os.environ.copy()
        if "HOME" not in env:
            import pathlib
            env["HOME"] = str(pathlib.Path.home())

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(input=user_text.encode("utf-8")),
                timeout=120,
            )
        except TimeoutError as e:
            raise LLMGenerationError(
                "Claude CLI timed out after 120s.", raw_response="",
            ) from e
        except FileNotFoundError as e:
            raise LLMGenerationError(
                f"Claude CLI not found at '{_CLAUDE_BIN}'. "
                "Install with: npm install -g @anthropic-ai/claude-code",
                raw_response="",
            ) from e

        raw = stdout.decode("utf-8", errors="replace")

        if proc.returncode != 0:
            # The CLI may still return JSON on stdout with error details.
            err = stderr.decode("utf-8", errors="replace")[:500]
            raise LLMGenerationError(
                f"Claude CLI exited with code {proc.returncode}: "
                f"{err or raw[:500]}",
                raw_response=raw or err,
            )
        text, p_tok, c_tok = _parse_cli_envelope(raw)
        elapsed = (time.perf_counter() - start) * 1000
        return LLMResult(
            raw_text=text,
            model_id=self.model,
            latency_ms=elapsed,
            prompt_tokens=p_tok,
            completion_tokens=c_tok,
        )

    @staticmethod
    def _build_user_text(prompt: PromptEnvelope) -> str:
        """Build the user-turn text, prepending chat history if present."""
        user_text = prompt.user_prompt
        if prompt.chat_history:
            history = [
                f"[{m.role}]: {m.content}" for m in prompt.chat_history
            ]
            user_text = "\n".join(history) + "\n\n" + user_text
        return user_text


# Satisfy the protocol check at import time.
_: LLMGatewayProtocol = ClaudeCLIGateway()
