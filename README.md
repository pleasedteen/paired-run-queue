# PairQueue：双运行时配对任务本地队列

一个仅依赖 Python 标准库的本地任务队列。队列状态持久化到 SQLite；A/B 两侧各自启动独立工作进程。每个工作进程主循环一次只执行一个任务，后台线程只负责租期心跳。

## 0. 一条命令完成本地验证

```bash
cd /workspace
./verify.sh
```

等价命令：

```bash
cd /workspace
python3 -m pairqueue.verify
```

该命令会自动：

1. 找空闲端口，启动一个 SQLite/HTTP 队列；
2. 启动独立的 `a1`、`b1` 工作进程，强杀 `a1` 后再启动 `a2`；
3. 验证：
   - 只有一侧在线时，该侧单边任务照常完成；
   - 配对任务必须等两侧领取同一轮后才开始；
   - 持续注入单边任务时，排队配对不会被饿死；
   - 慢任务超过租期但靠心跳续租，仍能成功；
   - 先交结果的同一工作进程可以立刻领取别的单边活；
   - `SIGKILL` 强杀后，租期到期、整轮作废，旧搭档在一个租期内停止并由新进程接手重跑；
   - 若一侧已提前交结果后另一侧租期才到期，旧的半份结果会标记为作废，两侧仍重跑；
   - 重启队列进程后，旧结果仍可查询，执行中和排队中的任务继续完成；
4. 打印中文可读断言和耗时；正常机器约 **24 秒**，脚本要求总耗时不超过 **80 秒**；
5. 自动清理它启动的所有进程。日志和 SQLite 文件位于输出中的 `/tmp/pairqueue-verify-*` 目录，失败时会自动打印日志尾部。

环境要求：Python 3.10+（开发验证使用 Python 3.11），无需 `pip install`。

---

## 1. 启动队列

```bash
cd /workspace
python3 -m pairqueue.server \
  --host 127.0.0.1 \
  --port 8765 \
  --db ./pairqueue.db \
  --lease-seconds 3
```

队列提供 JSON/HTTP API，并把任务、轮次、执行租期、结果和事件写入 SQLite。

## 2. 启动两侧工作进程

分别打开终端：

```bash
cd /workspace
python3 -m pairqueue.worker --server http://127.0.0.1:8765 --side a --worker-id a1
```

```bash
cd /workspace
python3 -m pairqueue.worker --server http://127.0.0.1:8765 --side b --worker-id b1
```

也可以为同一侧启动额外进程，注意 `--worker-id` 必须唯一：

```bash
python3 -m pairqueue.worker --server http://127.0.0.1:8765 --side a --worker-id a2
```

每个工作进程同时只执行一份任务。任务执行期间，后台心跳线程按租期的约十分之一间隔续租；收到队列的取消指令后，内置模拟任务会在 50ms 粒度的等待点停下。

## 3. 调用方提交任务、查状态、取结果

### 单边检查

```bash
cd /workspace

JOB_ID=$(python3 -m pairqueue.client \
  --server http://127.0.0.1:8765 \
  submit-single \
  --side a \
  --task benchmark \
  --seconds 1)

echo "job id: $JOB_ID"
python3 -m pairqueue.client status "$JOB_ID"
python3 -m pairqueue.client result "$JOB_ID" --wait --timeout 30
```

`--side` 可以是 `a` 或 `b`，表示只让指定运行时一侧执行。

### 配对对照任务

```bash
PAIR_ID=$(python3 -m pairqueue.client \
  --server http://127.0.0.1:8765 \
  submit-pair \
  --task benchmark \
  --workload json-api \
  --a-seconds 1.0 \
  --b-seconds 1.4)

python3 -m pairqueue.client status "$PAIR_ID"
python3 -m pairqueue.client result "$PAIR_ID" --wait
```

在同一轮的 A/B 两份结果都交齐前，`result` 接口返回 `409`。交齐后结果形如：

```json
{
  "round_no": 1,
  "a": {"elapsed_seconds": 1.01, "metrics": {"throughput_ops_s": 123456.0}},
  "b": {"elapsed_seconds": 1.41, "metrics": {"throughput_ops_s": 122900.0}},
  "comparison": {
    "elapsed_seconds": {"a": 1.01, "b": 1.41},
    "b_over_a_elapsed": 1.396
  }
}
```

### 其他常用命令

```bash
python3 -m pairqueue.client health
python3 -m pairqueue.client workers
python3 -m pairqueue.client status JOB_ID
python3 -m pairqueue.client result JOB_ID --wait
```

查看全部参数：

```bash
python3 -m pairqueue.server --help
python3 -m pairqueue.worker --help
python3 -m pairqueue.client --help
```

### Python 代码调用

```python
from pairqueue.client import QueueClient

queue = QueueClient("http://127.0.0.1:8765")
job = queue.submit_pair("benchmark", payload={"a_seconds": 1, "b_seconds": 1.5})
finished = queue.wait_completed(job["id"], timeout=30)
print(finished["final_result"])
```

## 4. 内置模拟任务

| 任务名 | 作用 |
| --- | --- |
| `sleep` / `wait` | 可取消地等待指定秒数，适合模拟慢进程和验证租期。 |
| `benchmark` / `bench` / `compare` | 可取消的模拟性能任务，返回耗时、ops、吞吐、p50/p99 和确定性 checksum。 |
| `echo` | 立即原样返回 payload，适合连通性检查。 |

常用 payload 字段：

- `seconds`：两侧共同耗时；
- `a_seconds` / `b_seconds`：分别指定 A/B 耗时；
- `retry_seconds`：配对轮次作废后的重跑耗时，验证脚本用它让接手轮快速完成；
- `workload`：模拟负载名称，会出现在结果中。

示例：

```bash
# A 快、B 慢；B 耗时大于租期，用来观察续租
python3 -m pairqueue.client submit-pair \
  --task sleep \
  --a-seconds 0.3 \
  --b-seconds 8
```

## 5. 持久化与队列重启

所有已提交任务、执行租期、结果和事件都在 `--db` 指定的 SQLite 文件中。队列重启时仍使用同一个文件即可；一键验证甚至会 `SIGKILL` 旧队列进程后再启动，以确认已提交事务和恢复流程可靠。

```bash
# 第一次启动
python3 -m pairqueue.server --db ./pairqueue.db --port 8765

# Ctrl+C 后重新执行同一条命令
python3 -m pairqueue.server --db ./pairqueue.db --port 8765
```

工作进程在队列不可达时不会立刻退出，而是重试心跳/提交。队列恢复后：

- 租期仍有效：原执行继续并提交结果；
- 停机超过租期：SQLite 恢复的过期扫描会把单边任务重新排队，或把配对旧轮作废并创建新轮，由工作进程至少一次地重新处理；
- 已完成并提交的结果始终可查询。

## 6. 关键调度和故障语义

- **配对闸门**：服务器创建同一 `round_no` 的 A/B 两条执行记录。第一个领取者进入 `wait_partner`；另一侧也领取后，服务器才把轮次置为 `running` 并通知双方开始。
- **公平防饿死**：候选任务按提交顺序选择。若最早的可执行任务是配对且另一侧近期在线，空闲一侧会先进入配对闸门，而不是让更新的单边任务不断插队；已经在跑的有限单边任务结束后即会加入。若另一侧完全离线，则跳过配对、继续处理本侧单边任务。
- **租期与续租**：每个执行有随机 `lease_token` 和 `lease_expires_at`。健康工作进程持续续租；过期扫描每 `--sweep-interval` 秒运行一次。
- **配对作废**：任一侧未交结果且租期到期，该轮变为 `invalidated`。已提前提交的另一侧结果标记为 `superseded`；仍在运行的搭档会在下次心跳收到取消，并在一个租期内停止、确认、重新领取新轮。
- **进程模型**：只有队列进程写 SQLite；多个工作进程只通过本地 HTTP 协调，因此强杀工作进程不会破坏持久状态。

## 7. 目录结构

```text
pairqueue/
  __init__.py
  server.py    # SQLite 存储、HTTP/JSON API、调度、租期扫描
  worker.py    # 独立工作进程、心跳续租、取消和结果提交
  client.py    # Python client 与命令行调用工具
  tasks.py     # 内置可取消模拟任务
  verify.py    # 一键 80 秒内本地验证
verify.sh      # ./verify.sh 入口
README.md
```
