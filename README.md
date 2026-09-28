# Autoscaling & Remediation Controller（进程内可复现版）

一套完全运行在**单个本地进程**中的自动伸缩与自愈编排能力：用内存状态模拟工作负载、
实例与调度事件，不依赖任何外部服务、真实凭据或不可访问资源。给定相同的事件序列，
系统的最终状态、决策结论与事件日志**逐字节一致**（确定性、可复现）。

## 1. 构建与验证

```bash
# JDK 21+，离线/联网均可
mvn test                       # 运行全部 25 个确定性测试（核心验证方式）
mvn spring-boot:run            # 启动本地演示 HTTP 服务（默认 8080）
```

测试全部基于 `ManualClock`（逻辑时钟）与脚本化的“基础设施”，无随机数、无线程时序依赖。
测试会通过标准输出打印**关键输入、状态迁移与判定依据**（也可调用 `GET /api/demo/events` 查看）。

### 本地演示接口（均为进程内状态）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/demo/workloads/{id}?replicas=2` | 注册工作负载并从全局容量总盘占初始额度（重复注册幂等；额度不足返回 409） |
| POST | `/api/demo/workloads/{id}/unregister` | 工作负载退出，退还全部共享额度并驱动等待者 |
| POST | `/api/demo/workloads/{id}/policy` | 写入策略（JSON 体为 ScalePolicy 字段） |
| POST | `/api/demo/workloads/{id}/load?version=&ratio=&observedAt=` | 上报负载观测 |
| POST | `/api/demo/workloads/{id}/health/{instanceId}?version=&healthy=` | 上报健康观测（不健康自动触发修复） |
| POST | `/api/demo/workloads/{id}/reconcile` | 计算目标副本并收敛实例集合（扩容受共享容量闸门约束） |
| POST | `/api/demo/workloads/{id}/remediate/{instanceId}` | 显式请求修复 |
| POST | `/api/demo/tick` | 推进所有修复任务（PENDING→RUNNING→终态），随后做一次容量补额调度 |
| POST | `/api/demo/capacity/schedule` | 显式驱动一次 FIFO 容量补额调度，返回本轮补额次数 |
| GET  | `/api/demo/capacity` | 全局容量账本快照（总盘/已用/可用/各负载占用/FIFO 等待队列） |
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

## 3. 多工作负载共享容量协同（CapacityPool）

多个工作负载的**所有存活实例都从同一个全局总盘子** `arc.total-capacity`（默认 20）里占额度。
账本由独立组件 `capacity/CapacityPool` 维护（单一内部锁，线程安全），与每工作负载的串行锁
按**固定加锁顺序**（先工作负载锁、后容量锁）配合，任何线程交错下都满足：

> **不变量：任意时刻，全部工作负载的存活实例之和 ≤ totalCapacity；账本占用数恒等于存活实例数。**

### 3.1 额度不足是一个独立、可区分的结论

- 伸缩评估时，只有当滞后带/稳定次数/冷却/观测版本等既有条件都通过、确实要扩容的那一刻，
  才向容量账本申请 **1 份**额度。
- 拿不到额度（盘子满，或 FIFO 队列前方有其他等待者）时返回
  **`ScaleOutcome.CAPACITY_PENDING` + `RejectionReason.INSUFFICIENT_GLOBAL_CAPACITY`**，
  与下列情况明确区分：策略非法 `INVALID_POLICY`、缺策略 `MISSING_POLICY`、观测缺失
  `MISSING_LOAD_OBSERVATION`、观测过期 `STALE_LOAD_OBSERVATION`、冷却中 `IN_COOLDOWN`。
- 容量等待期间**不提交任何伸缩状态**：`desiredReplicas` 不变、不刷新冷却时间、
  不消费该条观测（`lastActedObservationVersion` 不动）、已积累的方向 streak 保留。
  因此额度一到，下一次评估用同一条观测即可补扩，不漏扩、不重复扩。
- **注册**是必须立即落地的一次性动作：初始副本要一次性占足额度。盘子不足或前方有等待者时
  抛 `CapacityUnavailableException`（HTTP 演示返回 409，携带 requested/available/total），
  并回滚注册，不留“注册成功但没有实例”的半成品。

### 3.2 分配规则：严格 FIFO + 每份轮流（确定、可解释、无饿死）

- 等待队列是 **FIFO**（`LinkedHashSet`：同一工作负载重复登记合并为一个条目，保持首次排位）。
- 每份申请只有在“自己位于队首（或队列为空）**且**有空闲额度”时才被满足；满足后立即出队。
  还想再拿下一份的工作负载会在**下一次申请时重新排到队尾**——于是多个工作负载**轮流拿**，
  先到先得、谁也不会被永久插队饿死。
- 未被满足的申请登记到队尾（已登记则原地等待），**不占额度、不超发**。
- 新工作负载的初始注册同样不允许越过既有等待者（FIFO 一致）。

### 3.3 回收与再分配：释放后自动收敛

- **缩容/实例回收**：每终止一个实例立即向总盘退还 1 份额度。
- **工作负载退出**（`unregister` / `POST .../unregister`）：一次性退还其全部额度、
  移出等待队列与注册表。
- **修复（REPAIR/REPLACE）不改变额度账**：原地修复实例不变；替换是“旧实例销毁、同额新实例”，
  总数不变，无需退/占。
- 任何释放事件（缩容、退出、策略变化、`tick` 心跳）之后，`Controller` 都会在**所有工作负载锁之外**
  跑一次确定性的 `schedulePendingAllocations()`：按 FIFO 快照逐个让等待工作负载重新评估一次；
  队首且有空闲额度的立即补扩并落地，未就绪的（冷却中、观测过期、已不需要……）跳过保留排队。
  一轮里发生过补额就再来一轮（形成轮流），一整轮无人补额或队列清空即停止——**必然终止、不空转**。
  也可调用 `POST /api/demo/capacity/schedule` 显式驱动。

### 3.4 与既有规则如何配合

| 既有规则 | 与共享容量的关系 |
| --- | --- |
| 阈值滞后带 / 稳定次数 | 容量闸门只在方向意图成立、达到稳定次数后才介入；滞后带内 NO_CHANGE 不碰额度 |
| 冷却 | **冷却判定先于容量闸门**：冷却中返回 `IN_COOLDOWN`，不占额、不入队；冷却结束后才尝试占额（测试覆盖这一先后） |
| 单观测单次动作 | 容量等待不消费观测；补额成功才算“acted”，重复事件不会重复扩 |
| 每轮 ±1 | 每次只申请/退还 1 份，因此 FIFO 的每一份都能清晰归属到某个工作负载 |
| 策略版本单调 | 策略更新只钳制 `[min,max]`：上调 min 产生的欠账逐份走公平申请，不足则排队；回退版本仍被拒 |
| 修复并发/预算/超时 | 修复不占额外实例额度；缩容仍优先回收无活动修复的 UNHEALTHY 实例，活动修复中的实例不被回收（额度暂缓退还，记 `scale-down BLOCKED`） |
| 工作负载串行锁 | 复合操作固定“工作负载锁 → 容量锁”顺序；容量补额调度在工作负载锁之外执行，避免锁环死锁 |

### 3.5 确定性

- 账本（占用表、等待队列）使用保序结构；`tick` 按 workloadId 字典序遍历，
  补额调度只依赖 FIFO 队列与逻辑时钟，不依赖线程调度顺序。
- 同一条输入序列（含占满、等待、释放、轮流补额）重复执行：谁先拿到、谁后拿到、
  各自最终副本数、FIFO 队列与账本**逐次一致**（`SharedCapacityConcurrencyTest` 覆盖）。

## 4. 状态迁移与不变量

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
6. **共享容量**：任意线程交错下，所有工作负载存活实例之和 ≤ `totalCapacity`；
   `CapacityPool` 账本占用数恒等于存活实例总数；无超发、无重复占用（并发栅栏测试断言）。
7. **无饿死/会收敛**：每个等待条目要么被满足、要么因不再需要而被清理；
   FIFO + 每份轮流 + 释放事件后调度保证等待者最终都能被处理。

## 5. 并发模型

- `WorkloadRegistry` 使用 `ConcurrentHashMap` 保存工作负载。
- `Controller` 为每个 `workloadId` 维护一把锁（striped lock）：同一工作负载的策略写入、
  观测写入、伸缩评估、修复请求与 `tick` 推进**全部串行化**；不同工作负载之间完全并行。
- 共享容量账本 `CapacityPool` 自带单一内部锁；涉及“工作负载状态 + 容量账本”的复合操作
  一律按 **先工作负载锁、后容量锁** 的固定顺序加锁，容量补额调度则在工作负载锁之外进行，
  因此不存在锁环死锁。
- 因此在任意线程交错下：不会超额扩容（全局总盘 + FIFO 公平闸门 + 副本天花板 + 单观测单次
  + 每轮 ±1）、不会漏扩容（被容量挡住不消费观测，释放后由调度补扩）、
  不会重复修复（活动任务去重）；重复/乱序/回退的观测与策略依旧被版本规则拒绝，不会算错账。
- 测试 `ObservationAndConcurrencyTest` 使用 32 线程栅栏验证单工作负载性质；
  `SharedCapacityConcurrencyTest` 使用 8 工作负载栅栏并发抢最后一份额度验证全局不超发。

## 6. 失败预算规则

- 记账单位是工作负载；任何修复 `FAILED` 或 `TIMED_OUT` 记一次失败时间戳。
- 每次判定前先滚动清理窗口外记录；窗口内失败数 ≥ `failureBudget` 时，
  新请求（含健康事件自动触发）一律以 `FAILURE_BUDGET_EXHAUSTED` 终止。
- 拒绝原因判定顺序（前者优先）：未注册 → 无策略 → 策略禁止 → 实例不存在/已终止 →
  活动任务重复 → 实例已健康 → **失败预算耗尽** → 实例冷却中 → 并发槽已满。
- 预算恢复后，若实例冷却也已结束，新的修复请求可被受理（测试覆盖“耗尽→窗口恢复→成功”）。

## 7. 代码结构

```
time/           Clock / SystemClock(墙钟) / ManualClock(测试与仿真)
model/          ScalePolicy、观测、实例、修复任务、全部枚举与结论记录（含 CapacitySnapshot）
capacity/       CapacityPool(全局额度账本 + FIFO 等待队列)、CapacityAcquireStatus、
                CapacityUnavailableException
store/          WorkloadRegistry、WorkloadAggregate(聚合根)、ObservationStore(版本治理)
autoscale/      PolicyValidator、Autoscaler(滞后带/稳定次数/冷却/单观测单次/容量闸门)
infra/          SimulatedInfrastructure(内存实例 + 脚本化修复结果)
remediation/    RemediationService(请求校验)、RemediationExecutor(推进/超时/预算)
orchestration/  Controller(分锁门面 + FIFO 容量补额调度)、EventLog(确定性日志)、MainConfig
web/            DemoController(本地 HTTP 演示)
tests/          8 个测试类共 25 个用例 + support/TestHarness
```

容量相关测试：

| 测试类 | 覆盖内容 |
| --- | --- |
| `CapacityPoolTest` | 初始占用/超额拒绝、FIFO 轮流分配、释放与退出清账、10 工作负载并发申请不超发 |
| `SharedCapacityOrchestrationTest` | 一个占满另一个等待、释放后经调度补上、初始注册额度不足、退出释放并唤醒等待者 |
| `SharedCapacityConcurrencyTest` | 8 工作负载并发抢最后一份额度（恰好一个拿到、总量恒等于总盘）、同序列重放逐次一致 |
| `SharedCapacityCooperationTest` | 冷却优先于容量闸门、冷却结束后转容量等待、严格 FIFO、队首退出唤醒下一个 |

## 8. 可复现性说明

- 逻辑时钟：所有判定时间由调用方显式传入，生产用 `SystemClock`，测试用 `ManualClock`。
- 修复行为脚本化：`SimulatedInfrastructure.scriptRepair(instanceId, success, timedOut, durationMillis)`
  在任务启动时消费脚本，未配置脚本时默认成功。
- 任务 id 含 UUID（仅标识用），归一化快照/日志比较会抹除 UUID，因此行为结论完全可比；
  见 `EndToEndReproducibilityTest`：同一完整序列执行两次，最终状态与全部日志严格相等。
