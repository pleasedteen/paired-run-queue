"""Small JSON/HTTP client and command line interface for pairqueue."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from typing import Any


class QueueAPIError(RuntimeError):
    def __init__(self, status: int, message: str, data: Any = None):
        super().__init__(message)
        self.status = status
        self.data = data if isinstance(data, dict) else {"error": message}


class QueueClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8765", timeout: float = 3.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        req = urllib.request.Request(
            self.base_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(
                req, timeout=timeout if timeout is not None else self.timeout
            ) as resp:
                raw = resp.read()
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                parsed = json.loads(raw.decode("utf-8")) if raw else {}
            except json.JSONDecodeError:
                parsed = {"error": raw.decode("utf-8", "replace")}
            message = str(parsed.get("error") or parsed)
            raise QueueAPIError(exc.code, message, parsed) from exc

    def health(self) -> dict[str, Any]:
        return self.request("GET", "/health", timeout=1.5)

    def submit(self, body: dict[str, Any]) -> dict[str, Any]:
        return self.request("POST", "/api/jobs", body)

    def submit_single(
        self,
        side: str,
        task: str = "sleep",
        *,
        job_id: str | None = None,
        payload: dict[str, Any] | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        merged = dict(payload or {})
        merged.update(fields)
        body: dict[str, Any] = {
            "type": "single",
            "side": side,
            "task": task,
            "payload": merged,
        }
        if job_id:
            body["job_id"] = job_id
        return self.submit(body)

    def submit_pair(
        self,
        task: str = "benchmark",
        *,
        job_id: str | None = None,
        payload: dict[str, Any] | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        merged = dict(payload or {})
        merged.update(fields)
        body: dict[str, Any] = {"type": "pair", "task": task, "payload": merged}
        if job_id:
            body["job_id"] = job_id
        return self.submit(body)

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self.request("GET", f"/api/jobs/{job_id}")

    def get_result(self, job_id: str) -> dict[str, Any]:
        return self.request("GET", f"/api/jobs/{job_id}/result")

    def list_workers(self) -> dict[str, Any]:
        return self.request("GET", "/api/workers")

    def wait_for_status(
        self,
        job_id: str,
        statuses: set[str] | tuple[str, ...] = ("completed",),
        *,
        timeout: float = 30.0,
        interval: float = 0.1,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            last = self.get_job(job_id)
            if last.get("status") in statuses:
                return last
            time.sleep(interval)
        state = last.get("status") if last else "unknown"
        raise TimeoutError(f"job {job_id} did not reach {tuple(statuses)}; last={state}")

    def wait_completed(self, job_id: str, *, timeout: float = 30.0) -> dict[str, Any]:
        job = self.wait_for_status(
            job_id, ("completed", "failed"), timeout=timeout
        )
        if job["status"] == "failed":
            raise RuntimeError(f"job {job_id} failed: {job.get('error')}")
        return job


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Submit and inspect pairqueue mock tasks"
    )
    parser.add_argument(
        "--server",
        default="http://127.0.0.1:8765",
        help="queue base URL (default: http://127.0.0.1:8765)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_single = sub.add_parser("submit-single", help="submit a one-side check")
    p_single.add_argument("--side", required=True, choices=["a", "b"])
    p_single.add_argument("--task", default="sleep", choices=["sleep", "benchmark", "echo"])
    p_single.add_argument("--seconds", type=float, default=None)
    p_single.add_argument("--job-id", default=None)
    p_single.add_argument("--workload", default=None)

    p_pair = sub.add_parser("submit-pair", help="submit a two-runtime paired task")
    p_pair.add_argument("--task", default="benchmark", choices=["sleep", "benchmark", "echo"])
    p_pair.add_argument("--seconds", type=float, default=None)
    p_pair.add_argument("--a-seconds", type=float, default=None)
    p_pair.add_argument("--b-seconds", type=float, default=None)
    p_pair.add_argument("--retry-seconds", type=float, default=None)
    p_pair.add_argument("--job-id", default=None)
    p_pair.add_argument("--workload", default=None)

    p_status = sub.add_parser("status", help="show full job state/attempts/events")
    p_status.add_argument("job_id")

    p_result = sub.add_parser("result", help="fetch result when ready")
    p_result.add_argument("job_id")
    p_result.add_argument("--wait", action="store_true")
    p_result.add_argument("--timeout", type=float, default=30.0)

    sub.add_parser("workers", help="list registered workers")
    sub.add_parser("health", help="check queue health")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    client = QueueClient(args.server)

    if args.command == "health":
        _print_json(client.health())
        return 0
    if args.command == "workers":
        _print_json(client.list_workers())
        return 0
    if args.command == "status":
        _print_json(client.get_job(args.job_id))
        return 0
    if args.command == "result":
        if args.wait:
            job = client.wait_completed(args.job_id, timeout=args.timeout)
            _print_json(
                {
                    "job_id": args.job_id,
                    "status": "completed",
                    "result": job.get("final_result"),
                }
            )
        else:
            try:
                _print_json(client.get_result(args.job_id))
            except QueueAPIError as exc:
                print(json.dumps(exc.data, ensure_ascii=False, indent=2))
                return 2
        return 0

    payload: dict[str, Any] = {}
    for key in ("seconds", "a_seconds", "b_seconds", "retry_seconds", "workload"):
        value = getattr(args, key, None)
        if value is not None:
            payload[key] = value

    if args.command == "submit-single":
        job = client.submit_single(
            args.side, args.task, job_id=args.job_id, payload=payload
        )
        print(job["id"])
        return 0
    if args.command == "submit-pair":
        job = client.submit_pair(args.task, job_id=args.job_id, payload=payload)
        print(job["id"])
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
