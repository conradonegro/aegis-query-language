"""Dump and replay gateways for offline benchmark evaluation.

DumpPromptGateway: saves each (system_prompt, user_prompt) pair to a JSONL
file and returns a refusal so the pipeline doesn't proceed to translation.
Used in pass 1 to capture all prompts.

ReplayGateway: reads pre-computed LLM responses from a JSONL file keyed by
a hash of the user prompt. Used in pass 3 to feed agent-generated SQL back
through the translator/executor/evaluator.
"""

import hashlib
import json
import logging
import os
import time

from app.compiler.models import LLMResult, PromptEnvelope
from app.compiler.ollama import LLMGenerationError

logger = logging.getLogger(__name__)

_DUMP_PATH = os.getenv("LLM_DUMP_PATH", "benchmarks/prompts.jsonl")
_REPLAY_PATH = os.getenv("LLM_REPLAY_PATH", "benchmarks/responses.jsonl")


def _prompt_key(system_prompt: str, user_prompt: str) -> str:
    """Stable hash over BOTH prompts for keying dump/replay entries.

    Schema context (relationships, hints) renders into the system prompt;
    keying on the user prompt alone would serve stale responses after any
    schema-context change.
    """
    combined = system_prompt + "\x00" + user_prompt
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()[:16]


_dump_initialized = False


class DumpPromptGateway:
    """Saves prompts to a JSONL file. Returns a refusal so the pipeline
    records an 'error' for each question (expected — prompts are captured
    for offline processing)."""

    def __init__(self, model: str = "dump") -> None:
        global _dump_initialized  # noqa: PLW0603
        self.model = model
        self._path = _DUMP_PATH
        # Truncate the file once per process, not per instantiation.
        if not _dump_initialized:
            with open(self._path, "w"):
                pass
            _dump_initialized = True

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        key = _prompt_key(prompt.system_instruction, prompt.user_prompt)
        entry = {
            "key": key,
            "system_prompt": prompt.system_instruction,
            "user_prompt": prompt.user_prompt,
        }
        with open(self._path, "a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        # Return a refusal so the benchmark logs an error but keeps going.
        return LLMResult(
            raw_text='{"sql": null, "refused": true, '
            '"reason": "prompt dumped for offline processing"}',
            model_id=self.model,
            latency_ms=0.0,
            prompt_tokens=0,
            completion_tokens=0,
        )


class ReplayGateway:
    """Reads pre-computed LLM responses from a JSONL file."""

    def __init__(self, model: str = "replay") -> None:
        self.model = model
        self._responses: dict[str, str] = {}
        self._load_responses()

    def _load_responses(self) -> None:
        path = _REPLAY_PATH
        if not os.path.exists(path):
            logger.warning("ReplayGateway: %s not found", path)
            return
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                self._responses[entry["key"]] = entry["response"]
        logger.info(
            "ReplayGateway: loaded %d responses from %s",
            len(self._responses),
            path,
        )

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        key = _prompt_key(prompt.system_instruction, prompt.user_prompt)
        if key not in self._responses:
            raise LLMGenerationError(
                f"ReplayGateway: no response for prompt key {key}",
                raw_response="",
            )
        start = time.perf_counter()
        text = self._responses[key]
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
