"""Independent worker process for one runtime side (a or b).

A worker process runs at most one mock task in its main loop.  A small daemon
thread only sends lease heartbeats; it never executes work.
"""

from __future__ import annotations

import argparse
import os
import signal
import socket
import threading
import time
import urllib.error
from typing import Any

from .client import QueueAPIError, QueueClient
from .tasks import TaskCanceled, run_task


class Worker:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.worker_id = args.worker_id or f"{args.side}-{socket.gethostname()}-{os.getpid()}"
        self.client = QueueClient(args.server, timeout=args.http_timeout)
        self.shutdown_event = threading.Event()
        self.current_cancel: threading.Event | None = None
        self.current_execution: str | None = None

    def log(self, message: str) -> None:
        print(
            f"{time.strftime('%H:%M:%S')} [{self.worker_id}/{self.args.side}] {message}",
            flush=True,
        )

    def request_shutdown(self, signum: int | None = None, frame: Any = None) -> None:  # noqa: ARG002
        self.shutdown_event.set()
        if self.current_cancel is not None:
            self.current_cancel.set()

    def _request_with_retry(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        what: str,
    ) -> Any:
        """Retry terminal writes while the local queue is restarting."""
        last_log = 0.0
        while not self.shutdown_event.is_set():
            try:
                return self.client.request(method, path, body, timeout=2.0)
            except QueueAPIError:
                raise
            except (urllib.error.URLError, OSError, TimeoutError) as exc:
                now = time.monotonic()
                if now - last_log > 2.0:
                    self.log(f"queue unavailable while {what}: {exc}; retrying")
                    last_log = now
                self.shutdown_event.wait(0.2)
        return None

    def claim_once(self) -> dict[str, Any] | None:
        try:
            response = self.client.request(
                "POST",
                "/api/claim",
                {
                    "worker_id": self.worker_id,
                    "side": self.args.side,
                    "pid": os.getpid(),
                },
                timeout=2.0,
            )
            if response is None:
                return None
            return response
        except QueueAPIError as exc:
            self.log(f"claim rejected: {exc}")
            time.sleep(self.args.poll_interval)
            return None
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            self.log(f"queue not reachable for claim: {exc}; retrying")
            self.shutdown_event.wait(min(1.0, max(0.2, self.args.poll_interval * 4)))
            return None

    def _heartbeat(
        self,
        task: dict[str, Any],
        cancel_event: threading.Event,
        run_event: threading.Event,
        interval_box: list[float],
    ) -> None:
        eid = task["execution_id"]
        body = {
            "worker_id": self.worker_id,
            "side": self.args.side,
            "pid": os.getpid(),
            "lease_token": task["lease_token"],
        }
        failures = 0
        while not self.shutdown_event.is_set() and not cancel_event.is_set():
            try:
                resp = self.client.request(
                    "POST",
                    f"/api/executions/{eid}/heartbeat",
                    body,
                    timeout=1.5,
                )
                failures = 0
                if isinstance(resp, dict):
                    if resp.get("heartbeat_interval"):
                        interval_box[0] = float(resp["heartbeat_interval"])
                    if resp.get("phase") == "run":
                        run_event.set()
                    reason = str(resp.get("reason") or "")
                    if reason == "execution_submitted":
                        # Benign race: the result was committed while this
                        # heartbeat was in flight; the main loop is already done.
                        return
                    if resp.get("cancel"):
                        self.log(
                            f"server asked to stop {eid}: {resp.get('reason', 'canceled')}"
                        )
                        cancel_event.set()
                        run_event.set()
                        return
            except QueueAPIError as exc:
                data = exc.data or {}
                if data.get("cancel") or exc.status in (404, 409):
                    self.log(f"lease no longer valid for {eid}: {exc}")
                    cancel_event.set()
                    run_event.set()
                    return
                failures += 1
            except (urllib.error.URLError, OSError, TimeoutError):
                # Queue restart: keep running and retry.  If the lease expires
                # during downtime, the server will answer cancel on return.
                failures += 1
            wait = interval_box[0]
            if failures > 0:
                wait = min(1.0, interval_box[0] * 2)
            if cancel_event.wait(wait):
                return

    def handle(self, task: dict[str, Any]) -> None:
        eid = task["execution_id"]
        job_id = task["job_id"]
        round_no = int(task.get("round_no", 1))
        cancel_event = threading.Event()
        run_event = threading.Event()
        if task.get("phase") == "run":
            run_event.set()
        self.current_cancel = cancel_event
        self.current_execution = eid

        interval = float(
            self.args.heartbeat_interval
            or task.get("heartbeat_interval")
            or max(0.1, float(task.get("lease_seconds", 3.0)) / 10.0)
        )
        interval_box = [interval]
        stop_hb = threading.Event()
        hb_thread = threading.Thread(
            target=self._heartbeat,
            args=(task, cancel_event, run_event, interval_box),
            name=f"hb-{eid[:8]}",
            daemon=True,
        )
        hb_thread.start()

        kind = task.get("type") or ("pair" if task.get("phase") else "single")
        self.log(
            f"claimed {task['task']} job={job_id} exec={eid} "
            f"round={round_no} phase={task.get('phase')}"
        )
        started = time.monotonic()

        try:
            if task.get("phase") == "wait_partner":
                self.log(f"round {round_no} ready signal pending for job {job_id}")
                while not run_event.wait(0.1):
                    if self.shutdown_event.is_set() or cancel_event.is_set():
                        raise TaskCanceled("shut down while waiting for partner")
                if cancel_event.is_set():
                    raise TaskCanceled("canceled while waiting for partner")
                self.log(f"both sides ready; starting job {job_id} round {round_no}")

            try:
                started_resp = self.client.request(
                    "POST",
                    f"/api/executions/{eid}/started",
                    {
                        "worker_id": self.worker_id,
                        "lease_token": task["lease_token"],
                    },
                    timeout=1.5,
                )
                if isinstance(started_resp, dict) and started_resp.get("cancel"):
                    raise TaskCanceled(str(started_resp.get("reason")))
            except QueueAPIError as exc:
                if exc.status == 409:
                    raise TaskCanceled("server rejected started for stale execution") from exc
                # A transient queue outage here is harmless; heartbeats will
                # determine whether the assignment remains valid.
                self.log(f"started notification failed transiently: {exc}")
            except (urllib.error.URLError, OSError, TimeoutError) as exc:
                self.log(f"started notification unavailable: {exc}")

            def progress(fraction: float, message: str) -> None:
                if self.args.verbose:
                    self.log(f"progress {eid[:12]} {fraction:.0%} {message}")

            result = run_task(
                task["task"],
                task.get("payload") or {},
                side=self.args.side,
                worker_id=self.worker_id,
                execution_id=eid,
                round_no=round_no,
                cancel_event=cancel_event,
                progress=progress,
            )
            if cancel_event.is_set():
                raise TaskCanceled("cancel flag set at completion boundary")

            try:
                response = self._request_with_retry(
                    "POST",
                    f"/api/executions/{eid}/result",
                    {
                        "worker_id": self.worker_id,
                        "lease_token": task["lease_token"],
                        "result": result,
                    },
                    what="submitting result",
                )
            except QueueAPIError as exc:
                if exc.status in (404, 409):
                    raise TaskCanceled(
                        f"result rejected because assignment was superseded: {exc}"
                    ) from exc
                raise
            if response is None and self.shutdown_event.is_set():
                raise TaskCanceled("shutdown while submitting result")
            elapsed = time.monotonic() - started
            if isinstance(response, dict) and response.get("job_completed"):
                self.log(
                    f"submitted final result for {job_id} round {round_no} in {elapsed:.2f}s; "
                    "job complete"
                )
            else:
                self.log(
                    f"submitted side result for {job_id} round {round_no} in {elapsed:.2f}s; "
                    "free for other work"
                )

        except TaskCanceled as exc:
            elapsed = time.monotonic() - started
            self.log(f"stopped old assignment {eid} after {elapsed:.2f}s: {exc}")
            try:
                self._request_with_retry(
                    "POST",
                    f"/api/executions/{eid}/stopped",
                    {
                        "worker_id": self.worker_id,
                        "lease_token": task["lease_token"],
                        "reason": str(exc)[:200],
                        "stage": "canceled",
                    },
                    what="acknowledging stop",
                )
            except QueueAPIError as ack_exc:
                self.log(f"stop ack rejected (safe): {ack_exc}")
        except Exception as exc:
            self.log(f"task {eid} failed: {exc!r}")
            try:
                self.client.request(
                    "POST",
                    f"/api/executions/{eid}/fail",
                    {
                        "worker_id": self.worker_id,
                        "lease_token": task["lease_token"],
                        "error": {"type": type(exc).__name__, "message": str(exc)},
                    },
                    timeout=2.0,
                )
            except Exception as report_exc:  # noqa: BLE001
                self.log(f"could not report failure: {report_exc!r}")
        finally:
            stop_hb.set()
            hb_thread.join(timeout=1.0)
            self.current_cancel = None
            self.current_execution = None

    def run(self) -> int:
        signal.signal(signal.SIGTERM, self.request_shutdown)
        signal.signal(signal.SIGINT, self.request_shutdown)
        self.log(
            f"started; server={self.args.server} pid={os.getpid()} "
            f"poll={self.args.poll_interval}s"
        )
        while not self.shutdown_event.is_set():
            task = self.claim_once()
            if task is None:
                self.shutdown_event.wait(self.args.poll_interval)
                continue
            try:
                self.handle(task)
            except Exception as exc:  # noqa: BLE001 - worker must stay alive
                self.log(f"unexpected handler error: {exc!r}")
                self.current_cancel = None
                self.current_execution = None
                self.shutdown_event.wait(0.2)
        self.log("graceful shutdown")
        return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="pairqueue worker process")
    parser.add_argument("--server", default="http://127.0.0.1:8765")
    parser.add_argument("--side", required=True, choices=["a", "b"])
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--poll-interval", type=float, default=0.15)
    parser.add_argument("--heartbeat-interval", type=float, default=None)
    parser.add_argument("--http-timeout", type=float, default=3.0)
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return Worker(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
