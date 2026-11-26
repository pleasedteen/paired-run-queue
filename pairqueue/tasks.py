"""Built-in mock workloads for the local pair task queue."""

from __future__ import annotations

import math
import time
from typing import Any, Callable


class TaskCanceled(Exception):
    """Raised inside a mock task when its lease/round is invalidated."""


ProgressCB = Callable[[float, str], None]


def wait_cancellable(seconds: float, cancel_event: Any, step: float = 0.05) -> None:
    """Sleep in small slices so an invalidated assignment stops quickly."""
    seconds = max(0.0, float(seconds))
    deadline = time.monotonic() + seconds
    while True:
        if cancel_event is not None and cancel_event.is_set():
            raise TaskCanceled("assignment canceled")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        interval = min(step, remaining)
        if cancel_event is not None:
            # Event.wait returns True if the event was set during the wait.
            if cancel_event.wait(interval):
                raise TaskCanceled("assignment canceled")
        else:
            time.sleep(interval)


def configured_seconds(
    payload: dict[str, Any],
    side: str,
    round_no: int,
    default: float = 1.0,
) -> float:
    """Choose a duration.

    Useful verification hooks:
    - ``a_seconds`` / ``b_seconds`` set side-specific initial duration;
    - ``retry_seconds`` makes every rerun shorter;
    - ``round_2_seconds`` can target a specific attempt/generation.
    """
    keys: list[Any] = []
    if round_no > 1 and "retry_seconds" in payload:
        return max(0.0, float(payload["retry_seconds"]))
    keys.extend(
        [
            f"round_{round_no}_{side}_seconds",
            f"round_{round_no}_seconds",
            f"{side}_seconds",
            "seconds",
            "duration",
        ]
    )
    for key in keys:
        if key in payload and payload[key] is not None:
            return max(0.0, float(payload[key]))
    return default


def _mock_metrics(seconds: float, side: str, seed_text: str) -> dict[str, float]:
    # Deterministic pseudo-metrics so output looks like a tiny benchmark while
    # remaining independent of optional third-party packages.
    seed = sum(ord(c) for c in seed_text) + (17 if side == "a" else 29)
    base = 120000.0 + (seed % 97) * 137.0
    elapsed = max(seconds, 0.001)
    ops = int(base * elapsed)
    throughput = ops / elapsed
    p50 = 0.8 + (seed % 31) * 0.03
    p99 = p50 * (4.0 + (seed % 17) * 0.11)
    checksum = 0
    for i in range(min(20000, max(1000, int(seconds * 5000)))):
        checksum = (checksum * 1103515245 + 12345 + i + seed) & 0xFFFFFFFF
    return {
        "operations": float(ops),
        "throughput_ops_s": round(throughput, 2),
        "p50_latency_ms": round(p50, 3),
        "p99_latency_ms": round(p99, 3),
        "checksum": float(checksum),
    }


def run_task(
    task_name: str,
    payload: dict[str, Any],
    *,
    side: str,
    worker_id: str,
    execution_id: str,
    round_no: int,
    cancel_event: Any,
    progress: ProgressCB | None = None,
) -> dict[str, Any]:
    """Execute a named built-in mock task and return JSON-serializable result."""
    started = time.monotonic()
    common = {
        "task": task_name,
        "side": side,
        "worker_id": worker_id,
        "execution_id": execution_id,
        "round_no": round_no,
        "mock": True,
    }

    if task_name == "echo":
        return {
            **common,
            "elapsed_seconds": 0.0,
            "echo": payload,
        }

    if task_name in {"sleep", "wait"}:
        seconds = configured_seconds(payload, side, round_no, default=1.0)
        slices = 0
        deadline = time.monotonic() + seconds
        while True:
            if cancel_event is not None and cancel_event.is_set():
                raise TaskCanceled("sleep canceled")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            wait_cancellable(min(0.05, remaining), cancel_event)
            slices += 1
            if progress and slices % 10 == 0:
                progress(min(1.0, slices * 0.05 / max(seconds, 0.001)), "sleeping")
        elapsed = time.monotonic() - started
        return {
            **common,
            "requested_seconds": seconds,
            "elapsed_seconds": round(elapsed, 4),
            "slices": slices,
        }

    if task_name in {"benchmark", "bench", "compare"}:
        seconds = configured_seconds(payload, side, round_no, default=1.0)
        # Do a little deterministic arithmetic periodically, but spend most
        # time in cancellable waits to keep verification fast and stable.
        ticks = max(1, int(math.ceil(seconds / 0.1)))
        checksum = 0
        for tick in range(ticks):
            wait_cancellable(min(0.1, max(0.0, seconds - tick * 0.1)), cancel_event)
            for i in range(1500):
                checksum = (checksum * 1664525 + 1013904223 + i + tick) & 0xFFFFFFFF
            if progress:
                progress((tick + 1) / ticks, "benchmark tick")
        elapsed = time.monotonic() - started
        metrics = _mock_metrics(seconds, side, str(payload.get("workload", "default")))
        metrics["checksum"] = float(checksum)
        return {
            **common,
            "workload": payload.get("workload", "mock-http-json"),
            "requested_seconds": seconds,
            "elapsed_seconds": round(elapsed, 4),
            "metrics": metrics,
        }

    raise ValueError(f"unknown built-in task: {task_name!r}")


KNOWN_TASKS = ("sleep", "wait", "benchmark", "bench", "compare", "echo")
