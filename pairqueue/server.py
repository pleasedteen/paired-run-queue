"""SQLite-backed local HTTP task queue.

The server is intentionally dependency-free.  It owns all SQLite access; worker
processes and CLI clients talk to it over JSON/HTTP.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from .tasks import KNOWN_TASKS

SIDES = ("a", "b")
ACTIVE_EXECUTION_STATES = ("leased", "running")


def now_ts() -> float:
    return time.time()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


class QueueStore:
    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            parent = os.path.dirname(os.path.abspath(path))
            os.makedirs(parent, exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()
        self.conn.commit()

    def _create_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT UNIQUE NOT NULL,
                type TEXT NOT NULL CHECK(type IN ('single', 'pair')),
                side TEXT,
                task TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                final_result_json TEXT,
                error_json TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                completed_at REAL
            );

            CREATE TABLE IF NOT EXISTS workers (
                worker_id TEXT PRIMARY KEY,
                side TEXT NOT NULL,
                pid INTEGER,
                last_seen REAL NOT NULL,
                current_execution_id TEXT
            );

            CREATE TABLE IF NOT EXISTS rounds (
                job_id TEXT NOT NULL,
                round_no INTEGER NOT NULL,
                state TEXT NOT NULL,
                created_at REAL NOT NULL,
                ready_at REAL,
                completed_at REAL,
                invalidated_at REAL,
                reason TEXT,
                PRIMARY KEY(job_id, round_no)
            );

            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                round_no INTEGER NOT NULL,
                side TEXT NOT NULL,
                worker_id TEXT,
                lease_token TEXT,
                state TEXT NOT NULL,
                phase TEXT NOT NULL,
                task TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                result_json TEXT,
                error_json TEXT,
                lease_expires_at REAL,
                heartbeat_count INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL,
                claimed_at REAL,
                ready_at REAL,
                started_at REAL,
                last_heartbeat_at REAL,
                submitted_at REAL,
                invalidated_at REAL,
                stop_requested_at REAL,
                cancel_reason TEXT,
                stopped_at REAL,
                stop_ack TEXT,
                UNIQUE(job_id, round_no, side)
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                job_id TEXT,
                execution_id TEXT,
                round_no INTEGER,
                kind TEXT NOT NULL,
                message TEXT,
                data_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_status_seq ON jobs(status, seq);
            CREATE INDEX IF NOT EXISTS idx_executions_job ON executions(job_id, round_no);
            CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id, id);
            """
        )

    def close(self) -> None:
        with self.lock:
            self.conn.close()


def add_event(
    conn: sqlite3.Connection,
    kind: str,
    *,
    ts: float | None = None,
    job_id: str | None = None,
    execution_id: str | None = None,
    round_no: int | None = None,
    message: str = "",
    data: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """INSERT INTO events(ts, job_id, execution_id, round_no, kind, message, data_json)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            ts if ts is not None else now_ts(),
            job_id,
            execution_id,
            round_no,
            kind,
            message,
            json.dumps(data or {}, ensure_ascii=False, sort_keys=True),
        ),
    )


def parse_json(text: str | None, default: Any = None) -> Any:
    if text is None or text == "":
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


def execution_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    data["payload"] = parse_json(data.pop("payload_json", "{}"), {})
    data["result"] = parse_json(data.pop("result_json", None), None)
    data["error"] = parse_json(data.pop("error_json", None), None)
    return data


def job_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    data.pop("seq", None)
    data["payload"] = parse_json(data.pop("payload_json", "{}"), {})
    data["final_result"] = parse_json(data.pop("final_result_json", None), None)
    data["error"] = parse_json(data.pop("error_json", None), None)
    return data


def worker_to_dict(row: sqlite3.Row, at: float, ttl: float) -> dict[str, Any]:
    data = dict(row)
    data["online"] = at - float(row["last_seen"]) <= ttl
    data["age_seconds"] = round(at - float(row["last_seen"]), 3)
    return data


def compare_pair_results(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    ea = float(a.get("elapsed_seconds") or 0.0)
    eb = float(b.get("elapsed_seconds") or 0.0)
    comparison: dict[str, Any] = {
        "mock": True,
        "elapsed_seconds": {"a": ea, "b": eb},
    }
    if ea > 0:
        comparison["b_over_a_elapsed"] = round(eb / ea, 4)
    if eb > 0:
        comparison["a_over_b_elapsed"] = round(ea / eb, 4)
    ma = a.get("metrics") if isinstance(a.get("metrics"), dict) else {}
    mb = b.get("metrics") if isinstance(b.get("metrics"), dict) else {}
    ta = float(ma.get("throughput_ops_s") or 0.0)
    tb = float(mb.get("throughput_ops_s") or 0.0)
    if ta > 0 and tb > 0:
        comparison["throughput_a_over_b"] = round(ta / tb, 4)
        comparison["throughput_b_over_a"] = round(tb / ta, 4)
    return comparison


def invalidate_round_locked(
    conn: sqlite3.Connection,
    job_id: str,
    round_no: int,
    at: float,
    reason: str,
) -> bool:
    round_row = conn.execute(
        "SELECT * FROM rounds WHERE job_id=? AND round_no=?",
        (job_id, round_no),
    ).fetchone()
    if round_row is None or round_row["state"] in ("succeeded", "invalidated"):
        return False

    conn.execute(
        """UPDATE rounds
           SET state='invalidated', invalidated_at=?, reason=?
           WHERE job_id=? AND round_no=?""",
        (at, reason, job_id, round_no),
    )

    rows = conn.execute(
        "SELECT * FROM executions WHERE job_id=? AND round_no=?",
        (job_id, round_no),
    ).fetchall()
    for row in rows:
        state = row["state"]
        if state == "submitted":
            conn.execute(
                """UPDATE executions
                   SET state='superseded', invalidated_at=COALESCE(invalidated_at, ?)
                   WHERE id=?""",
                (at, row["id"]),
            )
            add_event(
                conn,
                "result_superseded",
                ts=at,
                job_id=job_id,
                execution_id=row["id"],
                round_no=round_no,
                message="early result discarded because partner did not finish lease",
                data={"reason": reason},
            )
        elif state in ACTIVE_EXECUTION_STATES:
            conn.execute(
                """UPDATE executions
                   SET stop_requested_at=COALESCE(stop_requested_at, ?),
                       cancel_reason=COALESCE(cancel_reason, ?)
                   WHERE id=?""",
                (at, reason, row["id"]),
            )
            add_event(
                conn,
                "stop_requested",
                ts=at,
                job_id=job_id,
                execution_id=row["id"],
                round_no=round_no,
                message=reason,
            )
        elif state == "slot":
            conn.execute(
                """UPDATE executions
                   SET state='canceled', invalidated_at=?, stopped_at=?
                   WHERE id=?""",
                (at, at, row["id"]),
            )

    conn.execute(
        "UPDATE jobs SET status='queued', updated_at=? WHERE id=? AND status='active'",
        (at, job_id),
    )
    add_event(
        conn,
        "round_invalidated",
        ts=at,
        job_id=job_id,
        round_no=round_no,
        message=reason,
    )
    return True


def sweep_locked(
    conn: sqlite3.Connection,
    at: float,
    worker_ttl: float,
) -> list[str]:
    """Expire leases and reap forming rounds whose missing side is offline."""
    expired_ids: list[str] = []
    rows = conn.execute(
        """SELECT * FROM executions
           WHERE state IN ('leased', 'running')
             AND lease_expires_at IS NOT NULL
             AND lease_expires_at < ?""",
        (at,),
    ).fetchall()

    for row in rows:
        eid = row["id"]
        expired_ids.append(eid)
        reason = "lease_expired_before_result"
        conn.execute(
            """UPDATE executions
               SET state='expired',
                   invalidated_at=COALESCE(invalidated_at, ?),
                   stop_requested_at=COALESCE(stop_requested_at, ?),
                   cancel_reason=COALESCE(cancel_reason, ?)
               WHERE id=?""",
            (at, at, reason, eid),
        )
        add_event(
            conn,
            "lease_expired",
            ts=at,
            job_id=row["job_id"],
            execution_id=eid,
            round_no=row["round_no"],
            message=reason,
        )
        job = conn.execute("SELECT * FROM jobs WHERE id=?", (row["job_id"],)).fetchone()
        if job is None or job["status"] in ("completed", "failed"):
            continue
        if job["type"] == "single":
            conn.execute(
                "UPDATE jobs SET status='queued', updated_at=? WHERE id=? AND status='active'",
                (at, job["id"]),
            )
        else:
            invalidate_round_locked(
                conn, job["id"], row["round_no"], at, reason
            )

    # A formed pair cannot wait forever for a side with no live worker.  If the
    # unclaimed slot's side has not been seen recently, invalidate this attempt.
    forming = conn.execute(
        "SELECT * FROM rounds WHERE state='forming'"
    ).fetchall()
    for round_row in forming:
        slot = conn.execute(
            """SELECT * FROM executions
               WHERE job_id=? AND round_no=? AND state='slot'
               LIMIT 1""",
            (round_row["job_id"], round_row["round_no"]),
        ).fetchone()
        if slot is None:
            continue
        online = conn.execute(
            "SELECT 1 FROM workers WHERE side=? AND last_seen >= ? LIMIT 1",
            (slot["side"], at - worker_ttl),
        ).fetchone()
        if online is None:
            invalidate_round_locked(
                conn,
                round_row["job_id"],
                round_row["round_no"],
                at,
                "partner_side_offline_before_ready",
            )

    return expired_ids


class PairQueueServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        handler_cls: type[BaseHTTPRequestHandler],
        store: QueueStore,
        lease_seconds: float,
        worker_ttl: float,
        sweep_interval: float,
    ):
        super().__init__(address, handler_cls)
        self.store = store
        self.lock = store.lock
        self.lease_seconds = lease_seconds
        self.worker_ttl = worker_ttl
        self.sweep_interval = sweep_interval
        self.sweeper_stop = threading.Event()
        self.sweeper_thread = threading.Thread(
            target=self._sweeper_main, name="pairqueue-sweeper", daemon=True
        )
        self.sweeper_thread.start()

    def _sweeper_main(self) -> None:
        while not self.sweeper_stop.wait(self.sweep_interval):
            try:
                with self.lock:
                    sweep_locked(
                        self.store.conn, now_ts(), self.worker_ttl
                    )
                    self.store.conn.commit()
            except Exception as exc:  # pragma: no cover - defensive daemon loop
                print(f"[pairqueue] sweeper error: {exc!r}", file=sys.stderr, flush=True)

    def shutdown_all(self) -> None:
        self.sweeper_stop.set()
        try:
            self.shutdown()
        finally:
            self.server_close()
            self.store.close()


class Handler(BaseHTTPRequestHandler):
    server: PairQueueServer

    def log_message(self, fmt: str, *args: Any) -> None:
        # The verifier redirects stderr to a log; normal operation stays quiet.
        if os.environ.get("PAIRQUEUE_HTTP_DEBUG"):
            super().log_message(fmt, *args)

    def _json(self, status: int, obj: Any) -> None:
        if status == 204:
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body = json.dumps(obj, ensure_ascii=False, indent=None).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _bad_request(self, message: str) -> None:
        self._json(400, {"error": message})

    def _read_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid JSON body: {exc}") from exc
        if not isinstance(value, dict):
            raise ValueError("JSON body must be an object")
        return value

    def do_GET(self) -> None:  # noqa: N802 - stdlib API
        parsed = urlparse(self.path)
        parts = [p for p in parsed.path.split("/") if p]
        try:
            if parsed.path == "/health":
                with self.server.lock:
                    job_count = self.server.store.conn.execute(
                        "SELECT COUNT(*) AS c FROM jobs"
                    ).fetchone()["c"]
                self._json(
                    200,
                    {
                        "ok": True,
                        "service": "pairqueue",
                        "time": now_ts(),
                        "job_count": job_count,
                        "lease_seconds": self.server.lease_seconds,
                    },
                )
                return

            if parts == ["api", "jobs"]:
                self._list_jobs()
                return
            if len(parts) == 3 and parts[:2] == ["api", "jobs"]:
                self._get_job(parts[2])
                return
            if len(parts) == 4 and parts[:2] == ["api", "jobs"] and parts[3] == "result":
                self._get_result(parts[2])
                return
            if parts == ["api", "workers"]:
                self._list_workers()
                return
            self._json(404, {"error": "not found"})
        except Exception as exc:  # pragma: no cover
            self._json(500, {"error": repr(exc)})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        parts = [p for p in parsed.path.split("/") if p]
        try:
            body = self._read_body()
            if parts == ["api", "jobs"]:
                self._create_job(body)
            elif parts == ["api", "claim"]:
                self._claim(body)
            elif len(parts) == 4 and parts[:2] == ["api", "executions"]:
                action = parts[3]
                if action == "heartbeat":
                    self._heartbeat(parts[2], body)
                elif action == "started":
                    self._started(parts[2], body)
                elif action == "result":
                    self._submit_result(parts[2], body)
                elif action == "stopped":
                    self._stopped(parts[2], body)
                elif action == "fail":
                    self._fail(parts[2], body)
                else:
                    self._json(404, {"error": "unknown execution action"})
            else:
                self._json(404, {"error": "not found"})
        except ValueError as exc:
            self._bad_request(str(exc))
        except Exception as exc:  # pragma: no cover
            self._json(500, {"error": repr(exc)})

    def _list_jobs(self) -> None:
        with self.server.lock:
            rows = self.server.store.conn.execute(
                "SELECT * FROM jobs ORDER BY seq DESC LIMIT 200"
            ).fetchall()
            self._json(200, {"jobs": [job_to_dict(r) for r in rows]})

    def _job_detail_locked(self, job_id: str) -> dict[str, Any] | None:
        conn = self.server.store.conn
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            return None
        job = job_to_dict(row)
        rounds = conn.execute(
            "SELECT * FROM rounds WHERE job_id=? ORDER BY round_no", (job_id,)
        ).fetchall()
        executions = conn.execute(
            "SELECT * FROM executions WHERE job_id=? ORDER BY round_no, side",
            (job_id,),
        ).fetchall()
        events = conn.execute(
            "SELECT * FROM events WHERE job_id=? ORDER BY id", (job_id,)
        ).fetchall()
        job["rounds"] = [dict(r) for r in rounds]
        detailed_executions = []
        for row in executions:
            execution = execution_to_dict(row)
            # Do not leak one half of a pair through the status API before the
            # whole paired result is available.  Metadata/state remains visible.
            if job["status"] != "completed" and execution.get("result") is not None:
                execution["result_stored"] = True
                execution["result"] = None
            detailed_executions.append(execution)
        job["executions"] = detailed_executions
        job["events"] = [dict(r) for r in events]
        return job

    def _get_job(self, job_id: str) -> None:
        with self.server.lock:
            detail = self._job_detail_locked(job_id)
        if detail is None:
            self._json(404, {"error": "job not found"})
        else:
            self._json(200, detail)

    def _get_result(self, job_id: str) -> None:
        with self.server.lock:
            row = self.server.store.conn.execute(
                "SELECT * FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if row is None:
                self._json(404, {"error": "job not found"})
                return
            job = job_to_dict(row)
            if job["status"] == "completed":
                self._json(
                    200,
                    {
                        "job_id": job_id,
                        "status": "completed",
                        "result": job["final_result"],
                        "completed_at": job["completed_at"],
                    },
                )
            elif job["status"] == "failed":
                self._json(
                    422,
                    {"job_id": job_id, "status": "failed", "error": job["error"]},
                )
            else:
                self._json(
                    409,
                    {
                        "job_id": job_id,
                        "status": job["status"],
                        "error": "result is not ready",
                    },
                )

    def _list_workers(self) -> None:
        at = now_ts()
        with self.server.lock:
            rows = self.server.store.conn.execute(
                "SELECT * FROM workers ORDER BY side, worker_id"
            ).fetchall()
            self._json(
                200,
                {
                    "workers": [
                        worker_to_dict(r, at, self.server.worker_ttl) for r in rows
                    ]
                },
            )

    def _create_job(self, body: dict[str, Any]) -> None:
        job_type = body.get("type")
        if job_type not in ("single", "pair"):
            raise ValueError("type must be 'single' or 'pair'")
        side = body.get("side")
        if job_type == "single":
            if side not in SIDES:
                raise ValueError("single jobs require side='a' or side='b'")
        else:
            side = None
        task = body.get("task", "sleep")
        if task not in KNOWN_TASKS:
            raise ValueError(f"unsupported task {task!r}; known={list(KNOWN_TASKS)}")
        payload = body.get("payload")
        if payload is None:
            # Convenience for callers: arbitrary extra fields become payload.
            reserved = {"type", "side", "task", "payload"}
            payload = {k: v for k, v in body.items() if k not in reserved}
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")

        job_id = body.get("job_id") or new_id("job")
        at = now_ts()
        with self.server.lock:
            conn = self.server.store.conn
            exists = conn.execute(
                "SELECT 1 FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if exists:
                detail = self._job_detail_locked(job_id)
                self._json(200, detail)
                return
            conn.execute(
                """INSERT INTO jobs(id, type, side, task, payload_json, status,
                                    created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)""",
                (
                    job_id,
                    job_type,
                    side,
                    task,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    at,
                    at,
                ),
            )
            add_event(
                conn,
                "job_submitted",
                ts=at,
                job_id=job_id,
                message=f"{job_type} submitted",
                data={"side": side, "task": task},
            )
            conn.commit()
            detail = self._job_detail_locked(job_id)
        self._json(201, detail)

    def _upsert_worker_locked(
        self,
        worker_id: str,
        side: str | None,
        pid: int | None = None,
        current_execution_id: str | None = None,
    ) -> None:
        if not worker_id:
            raise ValueError("worker_id is required")
        conn = self.server.store.conn
        at = now_ts()
        row = conn.execute(
            "SELECT * FROM workers WHERE worker_id=?", (worker_id,)
        ).fetchone()
        if row is None:
            if side not in SIDES:
                raise ValueError("side must be 'a' or 'b' for a new worker")
            conn.execute(
                """INSERT INTO workers(worker_id, side, pid, last_seen, current_execution_id)
                   VALUES (?, ?, ?, ?, ?)""",
                (worker_id, side, pid or os.getpid(), at, current_execution_id),
            )
        else:
            conn.execute(
                """UPDATE workers
                   SET last_seen=?,
                       pid=COALESCE(?, pid),
                       current_execution_id=?
                   WHERE worker_id=?""",
                (at, pid, current_execution_id, worker_id),
            )

    def _claim_response_locked(
        self,
        execution: sqlite3.Row,
        token: str,
        phase: str,
    ) -> dict[str, Any]:
        return {
            "execution_id": execution["id"],
            "job_id": execution["job_id"],
            "round_no": execution["round_no"],
            "side": execution["side"],
            "task": execution["task"],
            "payload": parse_json(execution["payload_json"], {}),
            "lease_token": token,
            "lease_seconds": self.server.lease_seconds,
            "heartbeat_interval": max(0.1, min(1.0, self.server.lease_seconds / 10.0)),
            "phase": phase,
            "server_time": now_ts(),
        }

    def _claim(self, body: dict[str, Any]) -> None:
        worker_id = str(body.get("worker_id") or "").strip()
        side = str(body.get("side") or "").strip()
        if not worker_id:
            raise ValueError("worker_id is required")
        if side not in SIDES:
            raise ValueError("side must be 'a' or 'b'")
        pid = body.get("pid")
        at = now_ts()
        conn = self.server.store.conn

        with self.server.lock:
            self._upsert_worker_locked(worker_id, side, pid)
            sweep_locked(conn, at, self.server.worker_ttl)

            # First priority: join an already-formed round's unclaimed slot.
            slot = conn.execute(
                """SELECT e.* FROM executions e
                   JOIN rounds r ON r.job_id=e.job_id AND r.round_no=e.round_no
                   WHERE e.side=? AND e.state='slot' AND r.state='forming'
                   ORDER BY e.created_at ASC, e.id ASC
                   LIMIT 1""",
                (side,),
            ).fetchone()
            if slot is not None:
                token = new_id("lease")
                expires = at + self.server.lease_seconds
                cur = conn.execute(
                    """UPDATE executions
                       SET state='leased', worker_id=?, lease_token=?,
                           lease_expires_at=?, claimed_at=?, last_heartbeat_at=?,
                           phase='run'
                       WHERE id=? AND state='slot'""",
                    (worker_id, token, expires, at, at, slot["id"]),
                )
                if cur.rowcount == 1:
                    conn.execute(
                        """UPDATE rounds SET state='running', ready_at=?
                           WHERE job_id=? AND round_no=? AND state='forming'""",
                        (at, slot["job_id"], slot["round_no"]),
                    )
                    conn.execute(
                        """UPDATE executions SET phase='run', ready_at=?
                           WHERE job_id=? AND round_no=?""",
                        (at, slot["job_id"], slot["round_no"]),
                    )
                    self._upsert_worker_locked(
                        worker_id, side, pid, slot["id"]
                    )
                    add_event(
                        conn,
                        "pair_ready",
                        ts=at,
                        job_id=slot["job_id"],
                        execution_id=slot["id"],
                        round_no=slot["round_no"],
                        message="both sides claimed; workload may start",
                        data={"worker_id": worker_id},
                    )
                    conn.commit()
                    updated = conn.execute(
                        "SELECT * FROM executions WHERE id=?", (slot["id"],)
                    ).fetchone()
                    self._json(200, self._claim_response_locked(updated, token, "run"))
                    return

            # Strict-ish fair order: oldest queued job this side can work.
            candidates = conn.execute(
                """SELECT * FROM jobs
                   WHERE status='queued'
                     AND ((type='single' AND side=?) OR type='pair')
                   ORDER BY seq ASC
                   LIMIT 100""",
                (side,),
            ).fetchall()

            for job in candidates:
                job_id = job["id"]
                if job["type"] == "pair":
                    open_round = conn.execute(
                        """SELECT 1 FROM rounds
                           WHERE job_id=? AND state IN ('forming', 'running')
                           LIMIT 1""",
                        (job_id,),
                    ).fetchone()
                    if open_round is not None:
                        continue

                    other_side = "b" if side == "a" else "a"
                    online = conn.execute(
                        "SELECT 1 FROM workers WHERE side=? AND last_seen >= ? LIMIT 1",
                        (other_side, at - self.server.worker_ttl),
                    ).fetchone()
                    if online is None:
                        # One runtime class offline: do not let this worker wait
                        # at a pair barrier; look for a side-specific job.
                        continue

                    last_round = conn.execute(
                        "SELECT MAX(round_no) AS m FROM rounds WHERE job_id=?",
                        (job_id,),
                    ).fetchone()["m"]
                    round_no = int(last_round or 0) + 1
                    self_eid = new_id("exec")
                    other_eid = new_id("exec")
                    token = new_id("lease")
                    expires = at + self.server.lease_seconds
                    payload = job["payload_json"]

                    conn.execute(
                        """INSERT INTO rounds(job_id, round_no, state, created_at)
                           VALUES (?, ?, 'forming', ?)""",
                        (job_id, round_no, at),
                    )
                    conn.execute(
                        """INSERT INTO executions(
                               id, job_id, round_no, side, worker_id, lease_token,
                               state, phase, task, payload_json, lease_expires_at,
                               created_at, claimed_at, last_heartbeat_at)
                           VALUES (?, ?, ?, ?, ?, ?, 'leased', 'wait_partner', ?, ?, ?, ?, ?, ?)""",
                        (
                            self_eid, job_id, round_no, side, worker_id, token,
                            job["task"], payload, expires, at, at, at,
                        ),
                    )
                    conn.execute(
                        """INSERT INTO executions(
                               id, job_id, round_no, side, worker_id, lease_token,
                               state, phase, task, payload_json, created_at)
                           VALUES (?, ?, ?, ?, NULL, NULL, 'slot', 'wait_partner', ?, ?, ?)""",
                        (
                            other_eid, job_id, round_no, other_side,
                            job["task"], payload, at,
                        ),
                    )
                    conn.execute(
                        "UPDATE jobs SET status='active', updated_at=? WHERE id=?",
                        (at, job_id),
                    )
                    self._upsert_worker_locked(worker_id, side, pid, self_eid)
                    add_event(
                        conn,
                        "pair_round_formed",
                        ts=at,
                        job_id=job_id,
                        execution_id=self_eid,
                        round_no=round_no,
                        message="first side claimed; waiting for partner",
                        data={"side": side, "worker_id": worker_id},
                    )
                    conn.commit()
                    row = conn.execute(
                        "SELECT * FROM executions WHERE id=?", (self_eid,)
                    ).fetchone()
                    self._json(
                        200,
                        self._claim_response_locked(row, token, "wait_partner"),
                    )
                    return

                # Single-side job.  round_no is reused as the attempt generation.
                last_attempt = conn.execute(
                    "SELECT MAX(round_no) AS m FROM executions WHERE job_id=?",
                    (job_id,),
                ).fetchone()["m"]
                attempt = int(last_attempt or 0) + 1
                eid = new_id("exec")
                token = new_id("lease")
                expires = at + self.server.lease_seconds
                conn.execute(
                    """INSERT INTO executions(
                           id, job_id, round_no, side, worker_id, lease_token,
                           state, phase, task, payload_json, lease_expires_at,
                           created_at, claimed_at, last_heartbeat_at)
                       VALUES (?, ?, ?, ?, ?, ?, 'leased', 'run', ?, ?, ?, ?, ?, ?)""",
                    (
                        eid, job_id, attempt, job["side"], worker_id, token,
                        job["task"], job["payload_json"], expires, at, at, at,
                    ),
                )
                conn.execute(
                    "UPDATE jobs SET status='active', updated_at=? WHERE id=?",
                    (at, job_id),
                )
                self._upsert_worker_locked(worker_id, side, pid, eid)
                add_event(
                    conn,
                    "execution_claimed",
                    ts=at,
                    job_id=job_id,
                    execution_id=eid,
                    round_no=attempt,
                    data={"worker_id": worker_id, "side": side},
                )
                conn.commit()
                row = conn.execute(
                    "SELECT * FROM executions WHERE id=?", (eid,)
                ).fetchone()
                self._json(200, self._claim_response_locked(row, token, "run"))
                return

            # Keep worker presence fresh even when there is no work.
            self._upsert_worker_locked(worker_id, side, pid, None)
            conn.commit()
        self._json(204, {})

    def _auth_execution(
        self, eid: str, body: dict[str, Any]
    ) -> tuple[sqlite3.Row | None, dict[str, Any] | None]:
        conn = self.server.store.conn
        row = conn.execute(
            "SELECT * FROM executions WHERE id=?", (eid,)
        ).fetchone()
        if row is None:
            self._json(404, {"error": "execution not found"})
            return None, None
        worker_id = str(body.get("worker_id") or "")
        token = str(body.get("lease_token") or body.get("token") or "")
        if row["worker_id"] != worker_id or row["lease_token"] != token:
            self._json(
                409,
                {
                    "ok": False,
                    "cancel": True,
                    "error": "invalid or stale lease token",
                    "state": row["state"],
                },
            )
            return None, None
        return row, None

    def _heartbeat(self, eid: str, body: dict[str, Any]) -> None:
        worker_id = str(body.get("worker_id") or "")
        with self.server.lock:
            conn = self.server.store.conn
            at = now_ts()
            if worker_id:
                self._upsert_worker_locked(
                    worker_id, body.get("side"), body.get("pid"), eid
                )
            sweep_locked(conn, at, self.server.worker_ttl)
            row, error = self._auth_execution(eid, body)
            if error is not None or row is None:
                conn.commit()
                return

            if row["state"] in ("submitted", "superseded", "canceled", "expired", "failed"):
                conn.commit()
                self._json(
                    200,
                    {
                        "ok": False,
                        "cancel": True,
                        "reason": f"execution_{row['state']}",
                        "state": row["state"],
                        "server_time": at,
                    },
                )
                return

            if row["stop_requested_at"] is not None:
                conn.commit()
                self._json(
                    200,
                    {
                        "ok": True,
                        "cancel": True,
                        "reason": row["cancel_reason"] or "round_invalidated",
                        "server_time": at,
                    },
                )
                return

            if row["lease_expires_at"] and float(row["lease_expires_at"]) < at:
                sweep_locked(conn, at, self.server.worker_ttl)
                conn.commit()
                self._json(
                    200,
                    {
                        "ok": False,
                        "cancel": True,
                        "reason": "lease_expired",
                        "server_time": at,
                    },
                )
                return

            expires = at + self.server.lease_seconds
            conn.execute(
                """UPDATE executions
                   SET lease_expires_at=?, last_heartbeat_at=?,
                       heartbeat_count=heartbeat_count+1
                   WHERE id=?""",
                (expires, at, eid),
            )

            phase = row["phase"]
            cancel = False
            reason = None
            if row["job_id"]:
                round_row = conn.execute(
                    "SELECT * FROM rounds WHERE job_id=? AND round_no=?",
                    (row["job_id"], row["round_no"]),
                ).fetchone()
                if round_row is not None:
                    if round_row["state"] == "forming":
                        phase = "wait_partner"
                    elif round_row["state"] == "invalidated":
                        cancel = True
                        reason = round_row["reason"] or "round_invalidated"
                    elif round_row["state"] == "running":
                        phase = "run"
            conn.commit()
            self._json(
                200,
                {
                    "ok": not cancel,
                    "cancel": cancel,
                    "reason": reason,
                    "phase": phase,
                    "lease_expires_at": expires,
                    "heartbeat_interval": max(
                        0.1, min(1.0, self.server.lease_seconds / 10.0)
                    ),
                    "server_time": at,
                },
            )

    def _started(self, eid: str, body: dict[str, Any]) -> None:
        with self.server.lock:
            conn = self.server.store.conn
            at = now_ts()
            sweep_locked(conn, at, self.server.worker_ttl)
            row, _ = self._auth_execution(eid, body)
            if row is None:
                conn.commit()
                return
            if row["state"] not in ("leased", "running") or row["stop_requested_at"]:
                conn.commit()
                self._json(
                    409,
                    {
                        "ok": False,
                        "cancel": True,
                        "reason": row["cancel_reason"] or row["state"],
                    },
                )
                return
            conn.execute(
                """UPDATE executions
                   SET state='running', started_at=COALESCE(started_at, ?),
                       last_heartbeat_at=?
                   WHERE id=?""",
                (at, at, eid),
            )
            conn.commit()
            self._json(200, {"ok": True, "cancel": False, "started_at": at})

    def _stopped(self, eid: str, body: dict[str, Any]) -> None:
        with self.server.lock:
            conn = self.server.store.conn
            at = now_ts()
            row, _ = self._auth_execution(eid, body)
            if row is None:
                conn.commit()
                return
            reason = str(body.get("reason") or body.get("stage") or "canceled")[:200]
            conn.execute(
                """UPDATE executions
                   SET stopped_at=COALESCE(stopped_at, ?),
                       stop_ack=?,
                       state=CASE WHEN state IN ('leased','running')
                                  THEN 'canceled' ELSE state END
                   WHERE id=?""",
                (at, reason, eid),
            )
            worker_id = body.get("worker_id")
            if worker_id:
                conn.execute(
                    "UPDATE workers SET current_execution_id=NULL WHERE worker_id=?",
                    (worker_id,),
                )
            add_event(
                conn,
                "worker_stopped_old_round",
                ts=at,
                job_id=row["job_id"],
                execution_id=eid,
                round_no=row["round_no"],
                message=reason,
            )
            conn.commit()
            self._json(200, {"ok": True, "stopped_at": at})

    def _fail(self, eid: str, body: dict[str, Any]) -> None:
        with self.server.lock:
            conn = self.server.store.conn
            at = now_ts()
            row, _ = self._auth_execution(eid, body)
            if row is None:
                conn.commit()
                return
            error = body.get("error") or {"message": "task failed"}
            conn.execute(
                """UPDATE executions
                   SET state='failed', error_json=?, submitted_at=?
                   WHERE id=?""",
                (json.dumps(error, ensure_ascii=False), at, eid),
            )
            job = conn.execute(
                "SELECT * FROM jobs WHERE id=?", (row["job_id"],)
            ).fetchone()
            if job is not None:
                if job["type"] == "single":
                    conn.execute(
                        """UPDATE jobs SET status='failed', error_json=?,
                                          updated_at=?, completed_at=? WHERE id=?""",
                        (json.dumps(error, ensure_ascii=False), at, at, job["id"]),
                    )
                else:
                    invalidate_round_locked(
                        conn, job["id"], row["round_no"], at, "execution_failed"
                    )
                    conn.execute(
                        """UPDATE jobs SET status='failed', error_json=?,
                                          updated_at=?, completed_at=? WHERE id=?""",
                        (json.dumps(error, ensure_ascii=False), at, at, job["id"]),
                    )
            conn.commit()
            self._json(200, {"ok": True})

    def _submit_result(self, eid: str, body: dict[str, Any]) -> None:
        with self.server.lock:
            conn = self.server.store.conn
            at = now_ts()
            sweep_locked(conn, at, self.server.worker_ttl)
            row, _ = self._auth_execution(eid, body)
            if row is None:
                conn.commit()
                return

            # Retry of a response lost after commit.
            if row["state"] == "submitted":
                job = conn.execute(
                    "SELECT status FROM jobs WHERE id=?", (row["job_id"],)
                ).fetchone()
                conn.commit()
                self._json(
                    200,
                    {
                        "accepted": True,
                        "duplicate": True,
                        "job_completed": bool(job and job["status"] == "completed"),
                    },
                )
                return

            if row["state"] not in ("leased", "running") or row["stop_requested_at"]:
                conn.commit()
                self._json(
                    409,
                    {
                        "accepted": False,
                        "cancel": True,
                        "reason": row["cancel_reason"]
                        or f"execution_{row['state']}",
                    },
                )
                return

            result = body.get("result")
            if not isinstance(result, dict):
                raise ValueError("result must be a JSON object")
            conn.execute(
                """UPDATE executions
                   SET state='submitted', result_json=?, submitted_at=?
                   WHERE id=?""",
                (json.dumps(result, ensure_ascii=False), at, eid),
            )
            add_event(
                conn,
                "result_submitted",
                ts=at,
                job_id=row["job_id"],
                execution_id=eid,
                round_no=row["round_no"],
                data={"worker_id": row["worker_id"], "side": row["side"]},
            )

            job = conn.execute(
                "SELECT * FROM jobs WHERE id=?", (row["job_id"],)
            ).fetchone()
            job_completed = False

            if job["type"] == "single":
                conn.execute(
                    """UPDATE jobs
                       SET status='completed', final_result_json=?,
                           updated_at=?, completed_at=?
                       WHERE id=?""",
                    (json.dumps(result, ensure_ascii=False), at, at, job["id"]),
                )
                job_completed = True
            else:
                sibling = conn.execute(
                    """SELECT * FROM executions
                       WHERE job_id=? AND round_no=? AND side != ?
                       ORDER BY created_at DESC LIMIT 1""",
                    (row["job_id"], row["round_no"], row["side"]),
                ).fetchone()
                if sibling is not None and sibling["state"] == "submitted":
                    a_row = conn.execute(
                        """SELECT * FROM executions
                           WHERE job_id=? AND round_no=? AND side='a'""",
                        (row["job_id"], row["round_no"]),
                    ).fetchone()
                    b_row = conn.execute(
                        """SELECT * FROM executions
                           WHERE job_id=? AND round_no=? AND side='b'""",
                        (row["job_id"], row["round_no"]),
                    ).fetchone()
                    a_result = parse_json(a_row["result_json"], {}) if a_row else {}
                    b_result = parse_json(b_row["result_json"], {}) if b_row else {}
                    final_result = {
                        "round_no": row["round_no"],
                        "completed_at": at,
                        "a": a_result,
                        "b": b_result,
                        "comparison": compare_pair_results(a_result, b_result),
                    }
                    conn.execute(
                        """UPDATE rounds
                           SET state='succeeded', completed_at=?
                           WHERE job_id=? AND round_no=?""",
                        (at, row["job_id"], row["round_no"]),
                    )
                    conn.execute(
                        """UPDATE jobs
                           SET status='completed', final_result_json=?,
                               updated_at=?, completed_at=?
                           WHERE id=?""",
                        (
                            json.dumps(final_result, ensure_ascii=False),
                            at, at, job["id"],
                        ),
                    )
                    job_completed = True
                    add_event(
                        conn,
                        "pair_completed",
                        ts=at,
                        job_id=job["id"],
                        round_no=row["round_no"],
                        message="both results available; caller may fetch pair result",
                    )
                else:
                    add_event(
                        conn,
                        "pair_half_ready",
                        ts=at,
                        job_id=job["id"],
                        execution_id=eid,
                        round_no=row["round_no"],
                        message="first result stored; worker is free for other jobs",
                    )

            if row["worker_id"]:
                conn.execute(
                    "UPDATE workers SET current_execution_id=NULL WHERE worker_id=?",
                    (row["worker_id"],),
                )
            conn.commit()
            self._json(200, {"accepted": True, "job_completed": job_completed})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local SQLite-backed pair task queue")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", default="pairqueue.db")
    parser.add_argument("--lease-seconds", type=float, default=3.0)
    parser.add_argument(
        "--worker-ttl",
        type=float,
        default=None,
        help="seconds without claim/heartbeat before a worker side is considered offline",
    )
    parser.add_argument("--sweep-interval", type=float, default=0.1)
    parser.add_argument("--port-file", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    worker_ttl = (
        float(args.worker_ttl)
        if args.worker_ttl is not None
        else max(1.0, float(args.lease_seconds))
    )
    store = QueueStore(args.db)
    httpd = PairQueueServer(
        (args.host, args.port),
        Handler,
        store,
        lease_seconds=float(args.lease_seconds),
        worker_ttl=worker_ttl,
        sweep_interval=float(args.sweep_interval),
    )
    actual_port = httpd.server_address[1]
    if args.port_file:
        with open(args.port_file, "w", encoding="utf-8") as f:
            f.write(str(actual_port))
    print(
        f"pairqueue listening on http://{args.host}:{actual_port} "
        f"db={os.path.abspath(args.db)} lease={args.lease_seconds}s",
        flush=True,
    )

    import signal

    def _term_handler(signum: int, frame: Any) -> None:  # noqa: ARG001
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _term_handler)
    try:
        httpd.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        print("\npairqueue stopping", flush=True)
    finally:
        httpd.shutdown_all()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
