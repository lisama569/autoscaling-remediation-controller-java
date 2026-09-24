# Autoscaling & Remediation Controller（进程内可复现版）

一套完全运行在**单个本地进程**中的自动伸缩与自愈编排能力：用内存状态模拟工作负载、
实例与调度事件，不依赖任何外部服务、真实凭据或不可访问资源。给定相同的事件序列，
系统的最终状态、决策结论与事件日志**逐字节一致**（确定性、可复现）。

## 1. 构建与验证

```bash
# JDK 21+，离线/联网均可
mvn test                       # 运行全部 14 个确定性测试（核心验证方式）
mvn spring-boot:run            # 启动本地演示 HTTP 服务（默认 8080）
```

测试全部基于 `ManualClock`（逻辑时钟）与脚本化的“基础设施”，无随机数、无线程时序依赖。
测试会通过标准输出打印**关键输入、状态迁移与判定依据**（也可调用 `GET /api/demo/events` 查看）。

### 本地演示接口（均为进程内状态）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/demo/workloads/{id}?replicas=2` | 注册工作负载并创建初始实例（重复注册幂等） |
| POST | `/api/demo/workloads/{id}/policy` | 写入策略（JSON 体为 ScalePolicy 字段） |
| POST | `/api/demo/workloads/{id}/load?version=&ratio=&observedAt=` | 上报负载观测 |
| POST | `/api/demo/workloads/{id}/health/{instanceId}?version=&healthy=` | 上报健康观测（不健康自动触发修复） |
| POST | `/api/demo/workloads/{id}/reconcile` | 计算目标副本并收敛实例集合 |
| POST | `/api/demo/workloads/{id}/remediate/{instanceId}` | 显式请求修复 |
| POST | `/api/demo/tick` | 推进所有修复任务（PENDING→RUNNING→终态） |
| GET  | `/api/demo/workloads/{id}` | 不可变状态快照 |
| GET  | `/api/demo/events` | 关键事件日志 |

> 注意：HTTP 演示使用墙钟（`SystemClock`），适合手工体验；**可复现验证请使用测试中的
> `ManualClock` 与脚本化修复**（见 `src/test/.../support/TestHarness.java`）。

## 2. 策略语义（ScalePolicy）

| 字段 | 语义 |
| --- | --- |
| `version` | 策略版本，**必须严格单调递增**；回退/重复版本返回 `POLICY_VERSION_CONFLICT`，不覆盖状态 |
| `minReplicas` / `maxReplicas` | 副本地板/天花板，要求 `1 <= min <= max` |
| `scaleUpThreshold` | `loadRatio > 该值` 记一次扩容意向 |
| `scaleDownThreshold` | `loadRatio <= 该值` 记一次缩容意向；要求 `down <= up`，二者之间为**滞后带** |
| `stabilizationRounds` | 同一方向连续满足 N 轮才允许伸缩（防抖） |
| `cooldownMillis` | 同一工作负载两次伸缩之间的冷却时间 |
| `remediationEnabled` | 为 false 时一切修复请求返回 `REMEDIATION_DISABLED` |
| `remediationConcurrency` | 单工作负载同时 RUNNING 的修复任务上限 |
| `failureBudget` / `failureBudgetWindowMillis` | 滚动窗口内允许的失败（含超时）次数；耗尽返回 `FAILURE_BUDGET_EXHAUSTED`，窗口滑过后自动恢复 |
| `remediationCooldownMillis` | 同一实例两次修复之间的冷却时间 |
| `remediationTimeoutMillis` | 单次修复超时；超时任务以 `TIMED_OUT` 终止（区别于 `FAILED`） |

### 伸缩判定（完全确定性）

1. 无策略 → `MISSING_POLICY`；无负载观测 → `MISSING_LOAD_OBSERVATION`；观测过期 → `STALE_LOAD_OBSERVATION`。
2. `ratio` 落在滞后带 `(down, up]` 内：两个方向计数清零，`NO_CHANGE`。
3. 方向计数达到 `stabilizationRounds` 才产生伸缩意图。
4. 冷却未结束：返回 **`IN_COOLDOWN`（独立于 REJECTED）**，已积累的方向计数保留；冷却结束后的下一次同方向观测即可动作。
5. 每次决策**最多调整 1 个副本**；到达边界给出 `NO_CHANGE` 并说明原因。
6. **同一条观测至多触发一次伸缩**（`lastActedObservationVersion`），重复事件/重复调度不会重复扩容。
7. 策略更新后 `desiredReplicas` 立即被钳制进新的 `[min,max]` 区间，实例集合随之收敛。

非法策略（版本、阈值、上下界、各时间/容量字段）一律返回 `INVALID_POLICY` 且带字段级说明，
不抛模糊异常。所有拒绝原因见枚举 `RejectionReason`，互不重叠、可区分。

## 3. 状态迁移与不变量

### 实例
`PROVISIONING`（模拟实现立即 READY）→ `READY` ⇄ `UNHEALTHY` → `REPAIRING` → 成功回 `READY`/
失败回 `UNHEALTHY`；`TERMINATED` 为终态。

- 缩容回收顺序：优先回收**无活动修复的 UNHEALTHY** 实例，其次按创建顺序回收 READY 实例；
  活动修复中的实例不会被回收（避免与修复竞争），此时缩容会记录 `scale-down BLOCKED`。
- 健康抖动：实例 UNHEALTHY 时若没有活动任务，新的 healthy 观测使其自然恢复 READY，不产生修复任务。

### 修复任务
`PENDING`（已受理、等待并发槽）→ `RUNNING`（启动时即确定执行计划）→
`SUCCEEDED` / `FAILED` / `TIMED_OUT`（三者互斥的终态）。

- 第 1 次修复使用 `REPAIR`（原地恢复）；第 2 次及以后使用 `REPLACE`（终止旧实例并创建 `id-repl`）。
- **无自动重试**：终态就是终态，再次修复必须由显式请求/新的不健康观测重新建任务，
  因此恢复后不会残留重复任务，也不会无限重试。
- 一个实例同一时刻至多一个活动任务：重复请求返回结果 `DUPLICATE`（原因 `DUPLICATE_REMEDIATION`）。
- `FAILED` 与 `TIMED_OUT` 都计入失败预算；预算按**滚动窗口**记账，窗口滑过自动恢复。

### 全局不变量

1. 策略、负载观测、每实例健康观测的版本只允许严格递增；回退/重复被拒绝且不覆盖状态。
2. `minReplicas <= desiredReplicas <= maxReplicas` 恒成立。
3. 任一时刻活动修复任务数 ≤ `remediationConcurrency`。
4. 一个实例至多一个活动修复任务；修复不做后台自动重试。
5. 终态任务集合中不存在 PENDING/RUNNING 残留（由测试断言）。

## 4. 并发模型

- `WorkloadRegistry` 使用 `ConcurrentHashMap` 保存工作负载。
- `Controller` 为每个 `workloadId` 维护一把锁（striped lock）：同一工作负载的策略写入、
  观测写入、伸缩评估、修复请求与 `tick` 推进**全部串行化**；不同工作负载之间完全并行。
- 因此在任意线程交错下：不会超额扩容（副本天花板 + 单观测单次动作 + 每轮 ±1）、
  不会漏扩容（每次合法意向都被 streak 与冷却规则确定处理）、不会重复修复（活动任务去重）。
- 测试 `ObservationAndConcurrencyTest` 使用 32 线程栅栏并发验证上述性质。

## 5. 失败预算规则

- 记账单位是工作负载；任何修复 `FAILED` 或 `TIMED_OUT` 记一次失败时间戳。
- 每次判定前先滚动清理窗口外记录；窗口内失败数 ≥ `failureBudget` 时，
  新请求（含健康事件自动触发）一律以 `FAILURE_BUDGET_EXHAUSTED` 终止。
- 拒绝原因判定顺序（前者优先）：未注册 → 无策略 → 策略禁止 → 实例不存在/已终止 →
  活动任务重复 → 实例已健康 → **失败预算耗尽** → 实例冷却中 → 并发槽已满。
- 预算恢复后，若实例冷却也已结束，新的修复请求可被受理（测试覆盖“耗尽→窗口恢复→成功”）。

## 6. 代码结构

```
time/           Clock / SystemClock(墙钟) / ManualClock(测试与仿真)
model/          ScalePolicy、观测、实例、修复任务、全部枚举与结论记录
store/          WorkloadRegistry、WorkloadAggregate(聚合根)、ObservationStore(版本治理)
autoscale/      PolicyValidator、Autoscaler(滞后带/稳定次数/冷却/单观测单次)
infra/          SimulatedInfrastructure(内存实例 + 脚本化修复结果)
remediation/    RemediationService(请求校验)、RemediationExecutor(推进/超时/预算)
orchestration/  Controller(分锁门面)、EventLog(确定性日志)、MainConfig
web/            DemoController(本地 HTTP 演示)
tests/          4 个测试类共 14 个用例 + support/TestHarness
```

## 7. 可复现性说明

- 逻辑时钟：所有判定时间由调用方显式传入，生产用 `SystemClock`，测试用 `ManualClock`。
- 修复行为脚本化：`SimulatedInfrastructure.scriptRepair(instanceId, success, timedOut, durationMillis)`
  在任务启动时消费脚本，未配置脚本时默认成功。
- 任务 id 含 UUID（仅标识用），归一化快照/日志比较会抹除 UUID，因此行为结论完全可比；
  见 `EndToEndReproducibilityTest`：同一完整序列执行两次，最终状态与全部日志严格相等。
