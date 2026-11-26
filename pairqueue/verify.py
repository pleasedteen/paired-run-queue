#!/usr/bin/env python3
"""One-command local verification for pairqueue.

Starts one SQLite/HTTP queue and independent worker OS processes, then checks:
1. one online side handles its own single jobs;
2. paired jobs wait for both sides and only then start;
3. continuous single jobs do not starve a queued pair;
4. slow tasks survive leases via heartbeats and the fast side takes other work;
5. SIGKILL takeover invalidates the round and stops the old partner;
6. an already submitted half-result is superseded when its partner later expires;
7. queue restart preserves results and lets in-flight/queued work finish.

A successful run is intended to take roughly 20-35 seconds on a normal laptop.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pairqueue.client import QueueAPIError, QueueClient  # noqa: E402

LEASE_SECONDS = 3.0
HEARTBEAT_EXPECTED = LEASE_SECONDS / 10.0
TOTAL_BUDGET_SECONDS = 80.0


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class VerificationFailure(RuntimeError):
    pass


class Harness:
    def __init__(self) -> None:
        self.port = find_free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.client = QueueClient(self.base_url, timeout=2.0)
        self.tmp = Path(tempfile.mkdtemp(prefix="pairqueue-verify-"))
        self.db = self.tmp / "queue.db"
        self.server_log_path = self.tmp / "server.log"
        self.server_proc: subprocess.Popen[bytes] | None = None
        self.server_log: Any = None
        self.workers: dict[str, dict[str, Any]] = {}
        self.worker_logs: dict[str, Any] = {}
        self.started_at = time.monotonic()
        self.check_count = 0

    def step(self, title: str) -> None:
        elapsed = time.monotonic() - self.started_at
        print(f"\n=== [{elapsed:5.1f}s] {title} ===", flush=True)

    def check(self, condition: bool, message: str) -> None:
        if not condition:
            raise VerificationFailure(message)
        self.check_count += 1
        print(f"  ✓ {message}", flush=True)

    def info(self, message: str) -> None:
        print(f"  · {message}", flush=True)

    def start_server(self) -> None:
        self.server_log = open(self.server_log_path, "wb")
        cmd = [
            sys.executable,
            "-u",
            "-m",
            "pairqueue.server",
            "--host",
            "127.0.0.1",
            "--port",
            str(self.port),
            "--db",
            str(self.db),
            "--lease-seconds",
            str(LEASE_SECONDS),
            "--sweep-interval",
            "0.1",
        ]
        self.server_proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdout=self.server_log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 10.0
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            if self.server_proc.poll() is not None:
                raise VerificationFailure(
                    f"queue exited early with code {self.server_proc.returncode}"
                )
            try:
                health = self.client.health()
                self.check(health.get("ok") is True, "queue /health responds")
                return
            except (urllib.error.URLError, OSError, TimeoutError) as exc:
                last_error = exc
                time.sleep(0.1)
        raise VerificationFailure(f"queue did not become healthy: {last_error!r}")

    def restart_server(self) -> None:
        self.step("队列重启：终止并重新启动 SQLite/HTTP 队列进程")
        assert self.server_proc is not None
        # Use SIGKILL for the old queue too: committed SQLite state must recover
        # without relying on graceful shutdown hooks.
        self.server_proc.kill()
        self.server_proc.wait(timeout=5.0)
        if self.server_log:
            self.server_log.close()
        self.start_server()
        self.info(f"队列已用同一 SQLite 文件重启：{self.db}")

    def start_worker(self, name: str, side: str) -> subprocess.Popen[bytes]:
        log_path = self.tmp / f"worker-{name}.log"
        log_handle = open(log_path, "wb")
        self.worker_logs[name] = log_handle
        cmd = [
            sys.executable,
            "-u",
            "-m",
            "pairqueue.worker",
            "--server",
            self.base_url,
            "--side",
            side,
            "--worker-id",
            name,
            "--poll-interval",
            "0.08",
        ]
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
        self.workers[name] = {"side": side, "proc": proc, "log": log_path}
        return proc

    def kill_worker(self, name: str) -> None:
        proc = self.workers[name]["proc"]
        proc.kill()
        proc.wait(timeout=5.0)
        self.info(f"已 SIGKILL 工作进程 {name} (pid={proc.pid})")

    def _stop_process(
        self, label: str, proc: subprocess.Popen[bytes], timeout: float = 3.0
    ) -> None:
        if proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.info(f"{label} 未在 {timeout:.1f}s 内退出，发送 SIGKILL")
            proc.kill()
            proc.wait(timeout=5.0)

    def cleanup(self) -> None:
        for name, item in list(self.workers.items()):
            try:
                self._stop_process(f"worker-{name}", item["proc"], timeout=2.0)
            except Exception:
                pass
        if self.server_proc is not None:
            try:
                self._stop_process("queue", self.server_proc, timeout=3.0)
            except Exception:
                pass
        for handle in list(self.worker_logs.values()) + (
            [self.server_log] if self.server_log else []
        ):
            try:
                handle.close()
            except Exception:
                pass

    def wait_workers_online(self, names: set[str], timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                rows = self.client.list_workers().get("workers", [])
                online = {w["worker_id"] for w in rows if w.get("online")}
                if names.issubset(online):
                    return
            except Exception:
                pass
            time.sleep(0.1)
        raise VerificationFailure(f"workers did not come online: {sorted(names)}")

    def wait_job_predicate(
        self,
        job_id: str,
        predicate: Callable[[dict[str, Any]], bool],
        *,
        timeout: float,
        interval: float = 0.04,
        description: str = "predicate",
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            last = self.client.get_job(job_id)
            if last.get("status") == "failed":
                raise VerificationFailure(
                    f"任务 {job_id} 失败: {json.dumps(last.get('error'), ensure_ascii=False)}"
                )
            if predicate(last):
                return last
            time.sleep(interval)
        status = last.get("status") if last else "missing"
        raise VerificationFailure(
            f"等待任务 {job_id} 的 {description} 超时；status={status}"
        )

    @staticmethod
    def find_execution_by_side(
        job: dict[str, Any], side: str, round_no: int | None = None
    ) -> dict[str, Any] | None:
        matches = [
            e
            for e in job.get("executions", [])
            if e["side"] == side and (round_no is None or e["round_no"] == round_no)
        ]
        if not matches:
            return None
        return sorted(matches, key=lambda e: e["round_no"])[-1]

    @staticmethod
    def execution_by_side(job: dict[str, Any], side: str, round_no: int | None = None) -> dict[str, Any]:
        matches = [
            e
            for e in job.get("executions", [])
            if e["side"] == side and (round_no is None or e["round_no"] == round_no)
        ]
        if not matches:
            raise VerificationFailure(f"缺少 side={side} round={round_no} 执行记录")
        return sorted(matches, key=lambda e: e["round_no"])[-1]

    @staticmethod
    def round_by_no(job: dict[str, Any], round_no: int) -> dict[str, Any]:
        for r in job.get("rounds", []):
            if r["round_no"] == round_no:
                return r
        raise VerificationFailure(f"缺少 round {round_no}")

    def tail_logs(self) -> None:
        print("\n--- 诊断日志尾部 ---", flush=True)
        paths = [self.server_log_path] + [
            item["log"] for item in self.workers.values()
        ]
        for path in paths:
            print(f"\n### {path}", flush=True)
            try:
                lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
                for line in lines[-40:]:
                    print(line, flush=True)
            except Exception as exc:
                print(f"(无法读取: {exc!r})", flush=True)

    def verify_one_side_and_gate(self) -> str:
        self.step("1/6 单边进程 + 配对放行")
        self.start_worker("a1", "a")
        self.wait_workers_online({"a1"})

        single_id = "verify-one-side-single"
        self.client.submit_single(
            "a",
            "sleep",
            job_id=single_id,
            payload={"seconds": 0.3},
        )
        job = self.client.wait_completed(single_id, timeout=6.0)
        exe = job["executions"][0]
        self.check(exe["worker_id"] == "a1", "只有 A 在线时，A 单边任务仍被处理")

        waiting_pair = "verify-wait-for-b"
        self.client.submit_pair(
            "benchmark",
            job_id=waiting_pair,
            payload={"seconds": 0.5, "workload": "gate"},
        )
        time.sleep(0.8)
        pending = self.client.get_job(waiting_pair)
        self.check(
            pending["status"] == "queued" and not pending.get("executions"),
            "B 侧离线时配对任务不提前开跑，也不占住 A 工作进程",
        )

        self.start_worker("b1", "b")
        self.wait_workers_online({"a1", "b1"})
        job = self.client.wait_completed(waiting_pair, timeout=8.0)
        round1 = self.round_by_no(job, 1)
        a = self.execution_by_side(job, "a", 1)
        b = self.execution_by_side(job, "b", 1)
        self.check(round1["state"] == "succeeded", "两侧领取同一轮后配对轮成功")
        self.check(
            bool(round1["ready_at"])
            and a["started_at"] >= round1["ready_at"] - 0.01
            and b["started_at"] >= round1["ready_at"] - 0.01,
            "两侧都就绪后才记录 started_at（配对闸门有效）",
        )
        start_gap = abs(float(a["started_at"]) - float(b["started_at"]))
        self.check(
            start_gap < LEASE_SECONDS,
            f"两侧实际开跑时间差 {start_gap:.2f}s < 租期 {LEASE_SECONDS:.0f}s",
        )
        self.check(
            job["final_result"] and {"a", "b", "comparison"} <= set(job["final_result"]),
            "调用方可以取得 A/B 两份结果和比较字段",
        )
        return single_id

    def verify_starvation(self) -> None:
        self.step("2/6 持续单边流量下配对不被饿死")
        stop_feed = threading.Event()
        submitted_ids: list[str] = []
        errors: list[Exception] = []
        counter = 0
        lock = threading.Lock()

        def feed() -> None:
            nonlocal counter
            local = QueueClient(self.base_url, timeout=2.0)
            while not stop_feed.wait(0.12):
                with lock:
                    i = counter
                    counter += 1
                side = "a" if i % 2 == 0 else "b"
                jid = f"verify-feed-{i:03d}"
                try:
                    local.submit_single(
                        side,
                        "sleep",
                        job_id=jid,
                        payload={"seconds": 0.22},
                    )
                    submitted_ids.append(jid)
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)

        feeder = threading.Thread(target=feed, name="single-feeder", daemon=True)
        feeder.start()
        time.sleep(0.35)

        pair_id = "verify-starved-pair"
        pair_started = time.monotonic()
        self.client.submit_pair(
            "sleep",
            job_id=pair_id,
            payload={"seconds": 0.8},
        )
        pair = self.client.wait_completed(pair_id, timeout=12.0)
        pair_elapsed = time.monotonic() - pair_started
        stop_feed.set()
        feeder.join(timeout=2.0)

        self.check(not errors, f"持续提交 {len(submitted_ids)} 个单边任务无提交错误")
        self.check(
            pair["status"] == "completed" and pair_elapsed < 8.0,
            f"持续单边流量中排队配对在 {pair_elapsed:.2f}s 内完成（不会无限等待）",
        )
        self.info(f"停止注入后等待 {len(submitted_ids)} 个已提交单边任务排空")
        deadline = time.monotonic() + 25.0
        for jid in submitted_ids:
            remaining = max(0.1, deadline - time.monotonic())
            self.client.wait_completed(jid, timeout=remaining)
        self.check(True, "注入的单边任务全部排空，未影响后续验证")

    def verify_slow_and_early_result(self) -> None:
        self.step("3/6 慢进程续租 + 先交结果一侧接别的活")
        pair_id = "verify-slow-pair"
        self.client.submit_pair(
            "sleep",
            job_id=pair_id,
            payload={"a_seconds": 0.35, "b_seconds": 4.2},
        )

        job = self.wait_job_predicate(
            pair_id,
            lambda j: len(j.get("executions", [])) == 2
            and all(e["state"] == "running" for e in j["executions"]),
            timeout=5.0,
            description="both pair executions running",
        )
        a0 = self.execution_by_side(job, "a", 1)
        b0 = self.execution_by_side(job, "b", 1)

        job = self.wait_job_predicate(
            pair_id,
            lambda j: self.execution_by_side(j, "a", 1)["state"] == "submitted",
            timeout=3.0,
            description="fast A result submitted",
        )
        a_done = self.execution_by_side(job, "a", 1)
        self.check(a_done["worker_id"] == "a1", "A 侧第一份配对结果由 a1 提交")
        self.check(
            a_done.get("result") is None and a_done.get("result_stored") is True,
            "配对未交齐时状态接口只显示结果已暂存，不提前给出半份结果",
        )

        try:
            self.client.get_result(pair_id)
            ready_early = True
        except QueueAPIError as exc:
            ready_early = exc.status == 200
            self.check(
                exc.status == 409,
                "两侧未交齐时配对结果接口返回未就绪（409）",
            )
        else:
            self.check(False, "两侧未交齐时不应能取得配对结果")
        if ready_early:
            raise VerificationFailure("pair result unexpectedly ready early")

        single_id = "verify-early-other-work"
        submit_wall = time.time()
        self.client.submit_single(
            "a",
            "sleep",
            job_id=single_id,
            payload={"seconds": 0.55},
        )
        single = self.client.wait_completed(single_id, timeout=4.0)
        single_exe = single["executions"][0]
        self.check(
            single_exe["worker_id"] == "a1",
            "同一个 a1 进程交完配对结果后立即领取并完成了别的单边活",
        )
        self.check(
            float(single["completed_at"]) >= submit_wall
            and float(single["completed_at"]) - submit_wall < 2.0,
            "插入的单边任务在慢搭档完成前就已结束",
        )

        during = self.client.get_job(pair_id)
        b_during = self.execution_by_side(during, "b", 1)
        self.check(
            during["status"] == "active" and b_during["state"] == "running",
            "此时慢 B 仍在运行，配对整体尚未完成",
        )

        pair = self.client.wait_completed(pair_id, timeout=10.0)
        b_done = self.execution_by_side(pair, "b", 1)
        self.check(
            pair["final_result"]["round_no"] == 1
            and b_done["state"] == "submitted"
            and int(b_done["heartbeat_count"]) >= 5,
            f"慢 B 执行 {b_done['result']['elapsed_seconds']:.1f}s 超过租期，"
            f"靠 {b_done['heartbeat_count']} 次续租完成且没有作废重跑",
        )
        self.check(
            float(single["completed_at"]) < float(pair["completed_at"]),
            "早交一侧的额外任务完成时间早于整轮配对完成时间",
        )

    def verify_kill_takeover(self) -> None:
        self.step("4/6 SIGKILL 强杀与旧轮次取消接手")
        pair_id = "verify-kill-pair"
        self.client.submit_pair(
            "sleep",
            job_id=pair_id,
            payload={"a_seconds": 20.0, "b_seconds": 20.0, "retry_seconds": 0.7},
        )
        job = self.wait_job_predicate(
            pair_id,
            lambda j: len(j.get("executions", [])) == 2
            and all(e["state"] == "running" for e in j["executions"]),
            timeout=5.0,
            description="long pair round running",
        )
        old_a = self.execution_by_side(job, "a", 1)
        old_b = self.execution_by_side(job, "b", 1)
        self.check(old_a["worker_id"] == "a1", "强杀目标是正在跑 A 的独立进程 a1")
        self.check(old_b["worker_id"] == "b1", "旧轮次搭档是独立进程 b1")

        kill_wall = time.time()
        self.kill_worker("a1")
        self.start_worker("a2", "a")

        canceled_job = self.wait_job_predicate(
            pair_id,
            lambda j: (
                self.execution_by_side(j, "b", 1).get("stopped_at") is not None
            ),
            timeout=LEASE_SECONDS + 5.0,
            description="old B partner acknowledges stop",
        )
        kill_to_stop_wall = time.time() - kill_wall
        old_a_after = self.execution_by_side(canceled_job, "a", 1)
        old_b_after = self.execution_by_side(canceled_job, "b", 1)
        round1 = self.round_by_no(canceled_job, 1)

        self.check(old_a_after["state"] == "expired", "被杀 A 的租期到期，执行记录为 expired")
        self.check(round1["state"] == "invalidated", "未交结果的 A 租期到期后整轮作废")
        self.check(
            old_b_after["state"] == "canceled"
            and bool(old_b_after["stopped_at"]),
            "仍在跑旧轮次的 B 收到取消并真正停下来确认",
        )
        invalidated_at = float(round1["invalidated_at"])
        stopped_at = float(old_b_after["stopped_at"])
        cancel_latency = stopped_at - invalidated_at
        self.check(
            0 <= cancel_latency <= LEASE_SECONDS + 0.25,
            f"轮次作废后 B 在 {cancel_latency:.2f}s 内停止（≤ 一个租期 {LEASE_SECONDS:.0f}s）",
        )
        self.info(
            f"从 SIGKILL 到旧 B 停止共 {kill_to_stop_wall:.2f}s "
            f"（租期 {LEASE_SECONDS:.0f}s + 检测/心跳）"
        )

        pair = self.client.wait_completed(pair_id, timeout=12.0)
        round2 = self.round_by_no(pair, 2)
        new_a = self.execution_by_side(pair, "a", 2)
        new_b = self.execution_by_side(pair, "b", 2)
        self.check(round2["state"] == "succeeded", "新轮次被重新领取并成功")
        self.check(new_a["worker_id"] == "a2", "新 A 工作进程 a2 接手重跑")
        self.check(new_b["worker_id"] == "b1", "健康 B 进程 b1 重新领活")
        self.check(
            float(new_b["claimed_at"]) >= stopped_at - 0.05,
            "B 先停下旧轮次、腾出执行槽，再领取新轮次",
        )
        self.check(
            pair["final_result"]["round_no"] == 2,
            "调用方拿到的是两侧重跑后的第 2 轮配对结果",
        )

    def verify_early_result_expiry(self) -> None:
        self.step("5/6 已交的半份结果遇搭档过期后作废并重跑")
        pair_id = "verify-early-result-expiry"
        self.client.submit_pair(
            "sleep",
            job_id=pair_id,
            payload={"a_seconds": 0.25, "b_seconds": 20.0, "retry_seconds": 0.6},
        )
        def early_states_ready(job: dict[str, Any]) -> bool:
            a = self.find_execution_by_side(job, "a", 1)
            b = self.find_execution_by_side(job, "b", 1)
            return bool(
                a is not None
                and b is not None
                and a["state"] == "submitted"
                and b["state"] == "running"
            )

        job = self.wait_job_predicate(
            pair_id,
            early_states_ready,
            timeout=5.0,
            description="A early result stored and B still running",
        )
        old_a = self.execution_by_side(job, "a", 1)
        old_b = self.execution_by_side(job, "b", 1)
        self.check(old_a["worker_id"] == "a2", "当前早交结果的是接手进程 a2")
        self.check(old_b["worker_id"] == "b1", "未交结果的慢侧是 b1")

        self.kill_worker("b1")
        self.start_worker("b2", "b")

        pair = self.client.wait_completed(pair_id, timeout=LEASE_SECONDS + 9.0)
        old_a_after = self.execution_by_side(pair, "a", 1)
        old_b_after = self.execution_by_side(pair, "b", 1)
        new_a = self.execution_by_side(pair, "a", 2)
        new_b = self.execution_by_side(pair, "b", 2)
        self.check(
            old_a_after["state"] == "superseded",
            "B 租期到期后，A 早已提交的旧结果被标记为 superseded，不能组成配对结果",
        )
        self.check(old_b_after["state"] == "expired", "B 旧执行记录为 expired")
        self.check(
            new_a["worker_id"] == "a2" and new_b["worker_id"] == "b2",
            "A2 与新 B2 完成同一新轮次，两侧都重跑",
        )
        self.check(
            pair["final_result"]["round_no"] == 2,
            "调用方最终只拿到新轮第 2 轮的完整配对结果",
        )

    def verify_restart(self, old_completed_job_id: str) -> None:
        self.step("6/6 队列重启：结果可查，未完成任务继续")
        long_a = "verify-restart-long-a"
        long_b = "verify-restart-long-b"
        queued_a = "verify-restart-queued-a"

        self.client.submit_single("a", "sleep", job_id=long_a, payload={"seconds": 4.0})
        self.client.submit_single("b", "sleep", job_id=long_b, payload={"seconds": 4.0})
        self.wait_job_predicate(
            long_a,
            lambda j: j["executions"] and j["executions"][-1]["state"] == "running",
            timeout=4.0,
            description="restart A task running",
        )
        self.wait_job_predicate(
            long_b,
            lambda j: j["executions"] and j["executions"][-1]["state"] == "running",
            timeout=4.0,
            description="restart B task running",
        )
        self.client.submit_single("a", "sleep", job_id=queued_a, payload={"seconds": 0.3})
        time.sleep(0.2)
        queued = self.client.get_job(queued_a)
        self.check(queued["status"] == "queued", "重启前已持久化一个排队中的 A 单边任务")

        self.restart_server()

        old = self.client.get_result(old_completed_job_id)
        self.check(
            old["status"] == "completed" and old["result"] is not None,
            "队列重启后旧的已完成配对结果仍可查询",
        )

        job_a = self.client.wait_completed(long_a, timeout=12.0)
        job_b = self.client.wait_completed(long_b, timeout=12.0)
        job_q = self.client.wait_completed(queued_a, timeout=12.0)
        self.check(
            job_a["status"] == "completed" and job_b["status"] == "completed",
            "重启前正在执行的 A/B 任务在队列恢复后通过续租/重试完成",
        )
        self.check(
            job_q["status"] == "completed",
            "SQLite 中排队的未完成任务在重启后继续被领取处理",
        )

    def run(self) -> int:
        print("PairQueue 本地一键验证", flush=True)
        print(f"工作目录: {self.tmp}", flush=True)
        print(f"队列地址: {self.base_url}", flush=True)
        print(f"租期: {LEASE_SECONDS:.1f}s；总时间预算: {TOTAL_BUDGET_SECONDS:.0f}s", flush=True)
        try:
            self.start_server()
            old_completed = self.verify_one_side_and_gate()
            self.verify_starvation()
            self.verify_slow_and_early_result()
            self.verify_kill_takeover()
            self.verify_early_result_expiry()
            self.verify_restart(old_completed)

            elapsed = time.monotonic() - self.started_at
            self.check(
                elapsed <= TOTAL_BUDGET_SECONDS,
                f"全部验证耗时 {elapsed:.1f}s，不超过 {TOTAL_BUDGET_SECONDS:.0f}s",
            )
            print("\n" + "=" * 72, flush=True)
            print(
                f"✅ 通过：{self.check_count} 项断言；配对放行/公平调度/慢续租/"
                f"强杀接手/队列重启均正常；总耗时 {elapsed:.1f}s。",
                flush=True,
            )
            print("=" * 72, flush=True)
            return 0
        except Exception as exc:
            elapsed = time.monotonic() - self.started_at
            print("\n" + "!" * 72, flush=True)
            print(f"❌ 验证失败（{elapsed:.1f}s）: {exc}", flush=True)
            print("!" * 72, flush=True)
            self.tail_logs()
            return 1
        finally:
            self.cleanup()


def main() -> int:
    harness = Harness()
    return harness.run()


if __name__ == "__main__":
    raise SystemExit(main())
