# Autoscaling & Remediation Controller（进程内可复现版）

一套完全运行在**单个本地进程**中的自动伸缩与自愈编排能力：用内存状态模拟工作负载、
实例与调度事件，不依赖任何外部服务、真实凭据或不可访问资源。给定相同的事件序列，
系统的最终状态、决策结论与事件日志**逐字节一致**（确定性、可复现）。

在单工作负载的伸缩/自愈之上，本版本新增**多工作负载共享实例容量的协同编排**：
所有工作负载的实例都从同一个全局容量总盘占额度，扩容在额度不足时进入
**确定顺序的 FIFO 等待队列**，额度回收后按规则补分配并最终收敛。


## 1. 构建与验证

```bash
# JDK 21+，离线/联网均可
mvn test                       # 运行全部 34 个确定性测试（核心验证方式）
mvn spring-boot:run            # 启动本地演示 HTTP 服务（默认 8080）
```

测试全部基于 `ManualClock`（逻辑时钟）与脚本化的“基础设施”，无随机数、无线程时序依赖。
测试会通过标准输出打印**关键输入、状态迁移与判定依据**（也可调用 `GET /api/demo/events` 查看）。

关键配置（`src/main/resources/application.properties`）：

- `arc.total-capacity`：所有工作负载共享的全局实例容量上限（默认 100）。
- `arc.max-observation-age-millis`：负载观测超过该年龄视为过期（默认 30000）。


### 本地演示接口（均为进程内状态）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/demo/workloads/{id}?replicas=2` | 注册工作负载并创建初始实例（占用共享容量；不足以 `SHARED_CAPACITY_EXHAUSTED` 拒绝；重复注册幂等） |
| POST | `/api/demo/workloads/{id}/deregister` | 工作负载整体退出：终止实例、归还全部额度、移出等待队列并触发补分配 |
| POST | `/api/demo/workloads/{id}/policy` | 写入策略（JSON 体为 ScalePolicy 字段）；下调上限等引发缩容时，释放的额度在本次操作内 FIFO 补分配 |
| POST | `/api/demo/workloads/{id}/load?version=&ratio=&observedAt=` | 上报负载观测 |
| POST | `/api/demo/workloads/{id}/health/{instanceId}?version=&healthy=` | 上报健康观测（不健康自动触发修复） |
| POST | `/api/demo/workloads/{id}/reconcile` | 计算目标副本并收敛实例集合（缩容释放额度后自动 FIFO 补分配） |
| POST | `/api/demo/workloads/{id}/remediate/{instanceId}` | 显式请求修复 |
| POST | `/api/demo/tick` | 推进所有修复任务（PENDING→RUNNING→终态；按 workloadId 有序） |
| POST | `/api/demo/capacity/reconcile` | 显式触发一次等待队列 FIFO 补分配 |
| GET  | `/api/demo/capacity` | 共享容量快照：`totalCapacity / usedCapacity / waitingOrder` |
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

## 3. 多工作负载共享容量（SharedCapacityPool）

所有工作负载的实例都从**同一个全局总盘** `arc.total-capacity`（默认 100，测试可注入任意值）
里占额度。容量池只记一笔硬账：`used = Σ 各工作负载已授予额度`，
已授予额度最终都落地为实例，因此 **`Σ 活动实例数 ≤ totalCapacity` 在任意线程交错下恒成立**。

### 3.1 占用额度的时机

| 场景 | 容量行为 |
| --- | --- |
| 注册工作负载（初始副本） | 一次性预留全部初始额度；**不足则一份不占、整体拒绝注册**（不排队、无残留），原因 `SHARED_CAPACITY_EXHAUSTED`，额度释放后可重新注册 |
| 观测触发的扩容 | 稳定次数 + 冷却 + 去重全部通过后，向容量池申请 **1 份**；拿到才创建实例 |
| 策略抬高 `minReplicas` | 对增量逐份预留，不足部分排队等待补分配 |
| 修复 REPAIR / REPLACE | **不占新额度**：原地修复不增减实例；替换是「旧实例终止 + 同 id 系新实例」，净额为 0 |

### 3.2 额度不足的明确结论

扩容意向本身合法（有策略、观测新鲜且超阈值、已过稳定窗口与冷却），仅仅是总盘没额度时，
评估返回独立的结果类别 **`ScaleOutcome.PENDING_CAPACITY`**，并带拒绝原因
**`RejectionReason.SHARED_CAPACITY_EXHAUSTED`** 与队列位置。它与以下情况互不混淆：

- `REJECTED / MISSING_POLICY`：没有策略；
- `REJECTED / MISSING_LOAD_OBSERVATION`、`STALE_LOAD_OBSERVATION`：观测缺失或过期；
- `IN_COOLDOWN`：在冷却窗口内（输入合法但时间未到）。

等待期间：`desiredReplicas` 记为「已满足 + 未满足需求」，但**不占额度、不建实例**；
不占用伸缩冷却（额度一到即可落地），也不会因重复调度重复扩容。

### 3.3 分配规则：FIFO + 队首优先 + 轮转补满（确定、可解释、无饥饿）

- 额度不足的申请进入一条 **FIFO 等待队列**（同一工作负载至多占一个位置；重复申请保持原位，
  不会因重复评估被挪到队尾）。
- **只要队首在等待，空闲额度只留给队首**，后来者即使有空余也不能插队——
  一次性预留（策略抬 `minReplicas`）与单份申请（观测扩容）都遵守此规则，
  因此任何工作负载都不会被无限期饿死。
- **补分配（drain）只授予队首，且每次 1 份**：授予后若该工作负载**仍有未满足需求**
  （例如一次性欠两份、三份），它会带着剩余需求**回到队尾**，形成轮转公平；
  drain 在同一次释放操作内持续轮转，**直到每个等待者都补到它的目标（`desiredReplicas`
  钳进策略 `[min,max]` 的值）、或者总盘确实再无空闲为止**——只有后一种情况它才继续留在
  队首等待下一次释放。这修复了「欠多份只补 1 份、剩余需求既不补也不排队」的漏分配：
  等待者的 `desiredReplicas` 始终记录完整目标，授予 1 份后剩余份额继续保留在需求里。
- **盘子空但队首仍有需求时，本轮立即停止并保留队首位置**（不会把它误判为已满足而踢出队列）；
  队首若已无需求（意向撤销、被新策略钳低），则移出队列后继续服务下一个等待者，
  僵尸队首不会永久挡住后来者。
- 一次 drain 有硬上限（`2 × totalCapacity` 个迭代：每次要么真实授予 1 份、
  要么移除一个永不复活的等待者、要么终止），杜绝活锁。
- **触发时机：任何释放额度的事件都在该次操作内（锁外）自动 drain**——缩容回收、
  工作负载退出，以及**改策略腾出额度**（下调 `maxReplicas` 引发缩容）；
  也可用 `POST /api/demo/capacity/reconcile` 显式触发（手动补分配同样能把排队需求补到收敛）。
- 等待者的扩容意向消失（观测进入滞后带/转为缩容/策略把目标压到已授予额度/工作负载退出）时
  **立即让出队列**，不做僵尸等待。

### 3.4 额度回收与收敛

- **缩容**：每回收一个实例立即归还 1 份，**归还后在同一次 reconcile 内**触发 FIFO 补分配。
  活动修复中的实例不被回收（沿用既有 `scale-down BLOCKED` 规则），其额度在真正终止后才归还。
- **工作负载退出**（`deregister`）：终止全部活动实例、归还全部已授予额度、移出等待队列，
  **同一次操作内**让等待者补上。
- **改策略腾出额度**（如下调 `maxReplicas` / `minReplicas`）：策略写入先把
  `desiredReplicas` 钳进新区间并终止多余实例、归还额度，**释放工作负载锁后立即 drain**——
  排队等待的负载在**这一次策略写入内**就拿到刚释放的额度并收敛，不需要任何不相干的
  后续操作把它「带出来」。
- 已授予额度与实例数最终收敛到一致：等待者在当次释放事件中逐份轮转补足，
  最终各自 `活动实例数 == 已授予额度 == 目标`，队列为空；
  全局底线 `Σ 活动实例数 ≤ totalCapacity` 恒成立——授予只在「队首身份 + 确有空闲」同时
  成立时发生，两个并发 drain 也不会超发（抢不到的一方立即停在 UNAVAILABLE，队首继续等）。

### 3.5 与冷却、阈值滞后、修复规则的配合

- **顺序**：先做既有的策略/观测/稳定/冷却/去重判定，全部通过后才进入容量判定。
  容量不足绝不「提前」消耗一次伸缩（不刷新 streak 成已动作、不占冷却）。
- **冷却**：等待额度的时间不计入也不阻塞冷却；额度落地后才按正常时间线继续。
- **滞后带 / 稳定次数**：等待期间若观测回落，扩容意向作废并退出队列（见 3.3）。
- **修复**：修复与容量正交（修复不抢额度）；多工作负载并发修复仍按各工作负载的
  并发槽、失败预算与实例冷却规则执行，`tick` 按 `workloadId` 有序推进以保证可复现。

### 3.6 确定性

等待队列是有序数据结构：补分配严格按队首授予、每份授予后才决定「回队尾轮转」还是「离队收敛」，
无需求的队首按固定规则移除；`tick` 按工作负载 id 排序；**改策略释放额度也在同一操作内完成
「释放 → drain」，不引入额外事件时序**。
因此**同一条输入序列在全新环境重跑，谁先拿到、谁后拿到、各自最终收敛到多少完全一致**，
不依赖线程抢跑（见 `SharedCapacityOrchestrationTest#sameInputSequenceYieldsSameAllocationAcrossRuns`
与 `SharedCapacityBackfillTest#backfillSequenceIsDeterministicAcrossRuns`）。
并发抢最后一份额度的测试只断言不变量（总数不超限、无重复/卡住任务、无等待者永久滞留），
不依赖特定交错顺序。


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
6. **`Σ 各工作负载活动实例数 ≤ arc.total-capacity` 在任意并发交错下恒成立**；
   已授予额度只在缩容/退出时归还，重复或乱序事件不会重复占用或超发额度。

## 5. 并发模型

- `WorkloadRegistry` 使用 `ConcurrentHashMap` 保存工作负载。
- `Controller` 为每个 `workloadId` 维护一把锁（striped lock）：同一工作负载的策略写入、
  观测写入、伸缩评估、修复请求与 `tick` 推进**全部串行化**；不同工作负载之间完全并行。
- `SharedCapacityPool` 内部用一把全局锁原子记账。锁顺序固定为
  **「工作负载锁 → 容量锁」**（不存在反向获取），因此无锁顺序环、无死锁；
  缩容回收、工作负载退出、**策略写入**触发的 FIFO 补分配都在工作负载锁释放之后逐把获取，
  drain 全程不嵌套任何工作负载锁。
- drain 每轮只在池锁内做一次「队首身份 + 是否有空闲」的原子判定再授予：两个并发释放各自
  触发的 drain 也不会把同一份额度授给两方，抢不到的一方立即停止本轮（`UNAVAILABLE`），
  队首位置原样保留、等待下一次释放。
- 因此在任意线程交错下：不会超额扩容（全局总盘 + 副本天花板 + 单观测单次动作 + 每轮 ±1）、
  不会漏扩容（每次合法意向都被 streak/冷却/容量规则确定处理，额度不足则排队并在回收后补上）、
  不会重复修复（活动任务去重）。
- 乱序/回退/重复的观测与策略仍由版本治理拒绝、不覆盖状态，因此不会把容量账算错。
- 测试 `ObservationAndConcurrencyTest`（32 线程栅栏）与 `SharedCapacityOrchestrationTest`
  （多工作负载并发抢最后一份额度、并发修复+伸缩混合收敛）验证上述性质。

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
model/          ScalePolicy、观测、实例、修复任务、全部枚举与结论记录
                (新增 RegistrationResult；ScaleOutcome 增 PENDING_CAPACITY；
                 RejectionReason 增 SHARED_CAPACITY_EXHAUSTED)
store/          WorkloadRegistry(含注销)、WorkloadAggregate(聚合根, pendingCapacity 标志)、
                ObservationStore(版本治理)
capacity/       SharedCapacityPool(全局额度账本 + FIFO 等待队列；grantQueuedUnit 队首授予+轮转)、
                CapacityGate(扩容申请网关)、CapacityReservation / CapacitySnapshot(结论与快照)
autoscale/      PolicyValidator、Autoscaler(滞后带/稳定次数/冷却/单观测单次 + 共享容量判定)
infra/          SimulatedInfrastructure(内存实例 + 脚本化修复结果)
remediation/    RemediationService(请求校验)、RemediationExecutor(推进/超时/预算)
orchestration/  Controller(分锁门面 + 注册占额/缩容归还/FIFO 补分配/退出)、EventLog、MainConfig
web/            DemoController(本地 HTTP 演示，含 /capacity、/capacity/reconcile、deregister)
tests/          7 个测试类共 34 个用例 + support/TestHarness
```

容量相关测试：

- `SharedCapacityPoolTest`（6 例）：账本占用/超发、FIFO 队首不被插队、释放与退出归还、
  确定性授予顺序、**重复申请不改变队列位置 / drain 授予后轮转回队尾 / 非队首不能靠一次性预留插队**。
- `SharedCapacityOrchestrationTest`（9 例）：占满→等待→后来拿到、缩容/退出后 FIFO 重分配并收敛、
  超额注册被干净拒绝、多工作负载并发抢最后一份额度不超发、等待意向消失让出队列、
  活动修复阻塞缩容时额度不提前释放、并发修复+伸缩混合无残留/卡住任务、同一输入序列重跑的确定性、
  **多等待者轮转（欠多份每份回队尾）+ 僵尸队首（需求被策略钳掉）当场移出不挡路**。
- `SharedCapacityBackfillTest`（5 例，本次回归）：
  - **等待者一次性欠多份：一次释放后在同一操作内轮转补到目标并收敛**（欠 2 份/释放 2 份）；
  - **部分释放只补 1 份时剩余需求继续排队，后续释放（含手动 `/capacity/reconcile`）继续补直到收敛**；
  - **改策略下调 max 腾出的额度在同一次策略写入内当场授予等待者**；
  - 多等待者轮转每负载每份轮转、盘子空时队首保留位置继续等；
  - 同一条补分配输入序列重跑，授予先后/份数/队列顺序逐次一致。

## 8. 本地验证

```bash
mvn test                                  # 全部 34 个确定性用例（核心验证）
mvn -Dtest=SharedCapacityPoolTest test    # 只跑容量账本/FIFO 单测
mvn -Dtest=SharedCapacityBackfillTest test
                                          # 只跑「欠多份一次补到位/部分释放继续排队/改策略当场补」回归
mvn -Dtest=SharedCapacityOrchestrationTest test  # 只跑多工作负载协同用例（含轮转/僵尸队首）
mvn -Dtest=AutoscaleDeterminismTest,ObservationAndConcurrencyTest,EndToEndReproducibilityTest,RemediationTest test
                                          # 只跑原有单工作负载用例，确认不回归
mvn spring-boot:run                       # HTTP 手工体验（默认 arc.total-capacity=100）
```

### 8.1 复现「欠多份只补一份」与「改策略腾额度不补」（修复前会失败）

```bash
# 场景一：b 策略抬 min=3 但盘子满（a=2,b=1,total=3，b 欠 2 份）；
# a 退出释放 2 份后，b 必须在同一次操作内补到 3、队列清空。
mvn -Dtest='SharedCapacityBackfillTest#waiterOwingMultipleUnitsIsFullyBackfilledOnSingleRelease' test
# 场景二：a 下调 maxReplicas 缩出 1 份时，排队的 b 必须在同一次 policy 写入内拿到。
mvn -Dtest='SharedCapacityBackfillTest#capacityFreedByPolicyTighteningIsGrantedToWaiterInSameOperation' test
```

HTTP 快速体验共享容量（可在 `application.properties` 调小 `arc.total-capacity`）：

```bash
curl -s localhost:8080/api/demo/capacity
curl -s -X POST "localhost:8080/api/demo/workloads/a?replicas=60"
curl -s -X POST "localhost:8080/api/demo/workloads/b?replicas=60"   # 超过剩余 -> SHARED_CAPACITY_EXHAUSTED
curl -s -X POST localhost:8080/api/demo/workloads/a/deregister      # 释放并触发补分配
```

## 9. 可复现性说明

- 逻辑时钟：所有判定时间由调用方显式传入，生产用 `SystemClock`，测试用 `ManualClock`。
- 修复行为脚本化：`SimulatedInfrastructure.scriptRepair(instanceId, success, timedOut, durationMillis)`
  在任务启动时消费脚本，未配置脚本时默认成功。
- 任务 id 含 UUID（仅标识用），归一化快照/日志比较会抹除 UUID，因此行为结论完全可比；
  见 `EndToEndReproducibilityTest`：同一完整序列执行两次，最终状态与全部日志严格相等。
