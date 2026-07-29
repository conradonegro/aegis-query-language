"""Generate SQL responses for dumped prompts using the claude CLI.

Reads benchmarks/prompts.jsonl (from the dump gateway), calls `claude -p`
for each prompt, and writes benchmarks/responses.jsonl for the replay gateway.

This script is a neutral passing layer: only validated model output (a SQL
envelope or a genuine model-authored refusal) is written to responses.jsonl.
Transport failures (CLI timeout/error, unparseable output after retries) are
written to benchmarks/failures.jsonl and retried on the next invocation,
because resumption skips only keys already present in responses.jsonl.

It intentionally uses subscription auth via the logged-in claude CLI and
refuses to run when ANTHROPIC_API_KEY is set (which would silently switch
the CLI to API billing).

Usage:
    uv run python scripts/cli_batch_generate.py [--model MODEL] [--concurrency N]
"""

import argparse
import json
import os
import subprocess
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

PROMPTS_PATH = Path("benchmarks/prompts.jsonl")
RESPONSES_PATH = Path("benchmarks/responses.jsonl")
FAILURES_PATH = Path("benchmarks/failures.jsonl")

MODEL_MAP = {
    "haiku": "claude-haiku-4-5-20251001",
    "sonnet": "sonnet",
    "opus": "opus",
}

# Per-attempt timeouts, escalating. Median call latency is ~17s and the
# slowest observed healthy call ~30s, so a call still running at 45s is
# almost certainly stalled rather than slow: abandon it and retry, which
# usually succeeds immediately. Later attempts get headroom in case the API
# really is in a slow window.
#
# The previous policy (a flat 120s x 3 attempts) meant one stalled question
# could burn six minutes. Across a throttled window that turned an ~18 minute
# run into 2h27m.
ATTEMPT_TIMEOUTS_S = (45.0, 90.0, 150.0)
MAX_ATTEMPTS = len(ATTEMPT_TIMEOUTS_S)

# Kept for callers that want the worst-case ceiling.
CLI_TIMEOUT_S = ATTEMPT_TIMEOUTS_S[-1]

# The CLI call must be pure prompt->answer inference: no tool use.
_DISALLOWED_TOOLS = (
    "Bash,Read,Write,Edit,Glob,Grep,WebFetch,WebSearch,Task,"
    "NotebookEdit,TodoWrite,SlashCommand,Skill"
)

Runner = Callable[[str, str, str, float], str]


class TransportFailure(Exception):
    """Infrastructure-level failure: the model's answer was never obtained.

    Never recorded as a model refusal — callers route these to
    failures.jsonl so the key is retried on the next run.
    """


def require_subscription_auth(environ: Mapping[str, str]) -> None:
    """Refuse to run when ANTHROPIC_API_KEY is set.

    With the key present, `claude -p` bills the API instead of the
    logged-in subscription, defeating the purpose of this script.
    """
    if environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit(
            "Refusing to run: ANTHROPIC_API_KEY is set, so the claude CLI "
            "would bill the API instead of your subscription. Unset it "
            "(`unset ANTHROPIC_API_KEY`) and re-run."
        )


def extract_envelope(text: str) -> dict[str, object] | None:
    """Find the first valid response envelope in model output.

    Accepts an object containing either a string "sql" or "refused": true,
    anywhere in the text (tolerates markdown fences and surrounding prose).
    Returns None when no such object exists.
    """
    decoder = json.JSONDecoder()
    idx = text.find("{")
    while idx != -1:
        try:
            obj, _ = decoder.raw_decode(text[idx:])
        except ValueError:
            idx = text.find("{", idx + 1)
            continue
        if isinstance(obj, dict) and (
            isinstance(obj.get("sql"), str) or obj.get("refused") is True
        ):
            return obj
        idx = text.find("{", idx + 1)
    return None


def _run_cli(
    system_prompt: str,
    user_prompt: str,
    model: str,
    timeout_s: float = CLI_TIMEOUT_S,
) -> str:
    """Invoke `claude -p` once and return the result text.

    Raises TransportFailure for any infrastructure-level problem.
    """
    cmd = [
        "claude",
        "-p",
        "--model",
        model,
        "--system-prompt",
        system_prompt,
        "--disallowed-tools",
        _DISALLOWED_TOOLS,
        "--no-session-persistence",
        "--output-format",
        "json",
    ]
    try:
        proc = subprocess.run(
            cmd,
            input=user_prompt,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        raise TransportFailure(f"CLI timeout after {timeout_s:g}s") from exc

    if proc.returncode != 0:
        # Usage-limit notices and similar often land on stdout with an
        # empty stderr — capture both so the failure reason is visible.
        stderr = " ".join(proc.stderr.split())[:150]
        stdout = " ".join(proc.stdout.split())[:150]
        detail = stderr or stdout or "(no output)"
        raise TransportFailure(f"CLI exit {proc.returncode}: {detail}")

    try:
        cli_envelope = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise TransportFailure("CLI returned non-JSON output") from exc

    if cli_envelope.get("is_error"):
        msg = str(cli_envelope.get("result", "unknown CLI error"))[:200]
        raise TransportFailure(f"CLI error envelope: {msg}")

    result = cli_envelope.get("result", "")
    if not isinstance(result, str):
        raise TransportFailure("CLI result field is not text")
    return result


def generate_one(
    entry: dict[str, str],
    model: str,
    runner: Runner = _run_cli,
    max_attempts: int = MAX_ATTEMPTS,
    backoff_s: float = 2.0,
    on_retry: Callable[[str, str], None] | None = None,
) -> dict[str, str]:
    """Obtain one validated response, retrying transport failures.

    Each attempt gets a longer timeout (ATTEMPT_TIMEOUTS_S): a call that has
    not returned in 45s is far more likely stalled than slow, so abandoning
    and retrying it beats waiting.

    *on_retry* is called with (key, reason) before each retry. Without it a
    throttled window is invisible — only questions that exhaust every attempt
    are ever logged, so hours of stalling can hide behind a single failure.

    Returns a responses.jsonl row. Raises TransportFailure when no
    validated envelope could be obtained within max_attempts.
    """
    last_failure: TransportFailure | None = None
    for attempt in range(max_attempts):
        if attempt:
            if on_retry is not None and last_failure is not None:
                on_retry(entry["key"], str(last_failure))
            time.sleep(backoff_s * attempt)
        timeout_s = ATTEMPT_TIMEOUTS_S[min(attempt, len(ATTEMPT_TIMEOUTS_S) - 1)]
        try:
            text = runner(
                entry["system_prompt"], entry["user_prompt"], model, timeout_s
            )
        except TransportFailure as exc:
            last_failure = exc
            continue
        envelope = extract_envelope(text)
        if envelope is None:
            last_failure = TransportFailure(
                f"no valid envelope in model output: {text[:120]!r}"
            )
            continue
        return {
            "key": entry["key"],
            "response": json.dumps(envelope, ensure_ascii=False),
        }
    assert last_failure is not None
    raise last_failure


def run_batch(
    entries: list[dict[str, str]],
    model: str,
    responses_path: Path,
    failures_path: Path,
    concurrency: int,
    runner: Runner = _run_cli,
    backoff_s: float = 2.0,
) -> tuple[int, int]:
    """Process entries concurrently; route results by outcome.

    Validated envelopes append to responses_path; transport failures
    append to failures_path. Returns (ok_count, failed_count).
    """
    ok = 0
    failed = 0
    retries = 0
    write_lock = Lock()
    start = time.time()

    def note_retry(key: str, reason: str) -> None:
        nonlocal retries
        with write_lock:
            retries += 1
            print(f"  retry key={key}: {reason[:90]}", flush=True)

    with (
        open(responses_path, "a") as out_f,
        open(failures_path, "a") as fail_f,
        ThreadPoolExecutor(max_workers=concurrency) as pool,
    ):
        futures = {
            pool.submit(
                generate_one,
                entry,
                model,
                runner,
                MAX_ATTEMPTS,
                backoff_s,
                note_retry,
            ): entry
            for entry in entries
        }
        for future in as_completed(futures):
            entry = futures[future]
            try:
                row = future.result()
            except TransportFailure as exc:
                failed += 1
                with write_lock:
                    fail_f.write(
                        json.dumps(
                            {"key": entry["key"], "error": str(exc)},
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    fail_f.flush()
                status = "E"
            else:
                ok += 1
                with write_lock:
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    out_f.flush()
                status = "+"
            done = ok + failed
            elapsed = time.time() - start
            rate = done / elapsed * 60 if elapsed > 0 else 0.0
            eta = (
                (len(entries) - done) / (done / elapsed)
                if elapsed > 0 and done
                else 0.0
            )
            print(
                f"[{done}/{len(entries)}] {status} key={entry['key']} "
                f"({rate:.1f}/min, ETA {eta:.0f}s)",
                flush=True,
            )
    if retries:
        # A high retry count against a low failure count means the API was
        # slow, not that the prompts were bad — the distinction that made a
        # ~18 minute run take 2h27m look like a clean run.
        print(
            f"\n{retries} retries across {len(entries)} questions "
            f"({retries / len(entries):.1f} per question) — "
            "high values indicate API throttling, not prompt problems.",
            flush=True,
        )
    return ok, failed


def main() -> None:
    require_subscription_auth(os.environ)

    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="haiku")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    model = MODEL_MAP.get(args.model, args.model)
    print(f"Model: {model}, concurrency: {args.concurrency}")

    with open(PROMPTS_PATH) as f:
        entries = [json.loads(line) for line in f if line.strip()]

    if args.offset:
        entries = entries[args.offset:]
    if args.limit:
        entries = entries[: args.limit]

    # Resume: skip keys that already have a validated response. Keys that
    # previously failed are absent from responses.jsonl, so they retry.
    existing: set[str] = set()
    if RESPONSES_PATH.exists():
        with open(RESPONSES_PATH) as f:
            for line in f:
                if line.strip():
                    existing.add(json.loads(line)["key"])
    remaining = [e for e in entries if e["key"] not in existing]

    print(
        f"Total: {len(entries)}, already done: {len(existing)}, "
        f"remaining: {len(remaining)}"
    )

    if not remaining:
        print("Nothing to do.")
        return

    start = time.time()
    ok, failed = run_batch(
        remaining,
        model=model,
        responses_path=RESPONSES_PATH,
        failures_path=FAILURES_PATH,
        concurrency=args.concurrency,
    )

    print(
        f"\nDone: {ok} responses, {failed} transport failures, "
        f"{time.time() - start:.0f}s elapsed"
    )
    print(f"Responses: {RESPONSES_PATH}")
    if failed:
        print(
            f"Failures logged to {FAILURES_PATH} — re-run this script to "
            "retry them."
        )


if __name__ == "__main__":
    main()
