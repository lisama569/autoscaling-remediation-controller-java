# 自动伸缩与自愈编排控制器（进程内确定性模拟）

本服务在**单个本地进程**内模拟工作负载（Workload）、实例（Instance）、负载/健康观测与
修复任务，提供一套可解释、可复现的自动伸缩（Autoscaling）与自愈（Remediation）编排能力。

- 不依赖任何外部服务、真实凭据或不可访问资源；不启动任何网络目标。
- 时间来自可显式推进的 `VirtualClock`，相同输入序列必然产生相同状态迁移与最终状态。
- 所有判定（接受 / 拒绝 / 终态）都有**可区分的原因码**，并写入进程内决策日志与 SLF4J 日志。

## 1. 构建与本地验证

环境：JDK 21、Maven（仓库自带 Spring Boot 3.4.x 依赖）。

```bash
# 全量测试（并发编排、重复/乱序观测、健康抖动、预算耗尽与恢复、可复现性）
mvn test

# 一键确定性演示（不启动 Web，直接打印完整收敛过程与判定依据）
mvn spring-boot:run -Dspring-boot.run.arguments="--controller.demo=true --spring.main.web-application-type=none"

# 启动本地 HTTP 服务（默认 8080）
mvn spring-boot:run
```

测试日志会打印关键输入（负载值、来源、序号、实例连续不健康计数）、每次状态迁移
（副本数变化、任务入队/晋升/终态）以及判定原因，例如：

```
decision | accepted=false kind=SCALE reason=SCALE_IN_COOLDOWN detail='... cooldown remaining 900ms' ...
decision | accepted=false kind=REPAIR reason=REPAIR_ATTEMPTS_EXHAUSTED detail='repair failed 1 time(s); giving up'
```

## 2. 模块布局

| 包/类 | 职责 |
| --- | --- |
| `clock.Clock` / `clock.VirtualClock` | 时间抽象；虚拟时钟只能单调向前，供测试与模拟器显式推进 |
| `domain.ScalingPolicy` | 不可变策略值对象，内含完整的非法项校验（不静默修正） |
| `domain.Workload` | 工作负载聚合：策略、实例、分来源观测、冷却时间点、单调状态版本、内部锁 |
| `domain.Instance` / `InstanceStatus` | 模拟实例与生命周期状态 |
| `domain.LoadObservation` | 负载观测（sourceId + 单调 sequence + observedAt） |
| `domain.RepairTask` / `RepairOutcome` | 修复任务与脚本化结果（PENDING/REPAIRED/REPLACED/FAILED/TIMEOUT） |
| `core.PolicyValidator` | 策略合法性校验 |
| `core.WorkloadRegistry` | 注册、版本化策略更新、观测接收、健康上报、按工作负载串行化访问 |
| `core.Autoscaler` | 阈值滞后 + 冷却 + 边界/观测校验的伸缩编排（含纯函数目标副本计算） |
| `core.RemediationManager` | 修复队列、并发上限、失败预算滑动窗口、冷却、超时与重试终态 |
| `core.SimulationEngine` | 确定性 tick：推进虚拟时间并驱动一轮修复循环 |
| `core.DecisionResult` / `RejectReason` / `DecisionLog` | 统一判定结果、可区分原因码、进程内决策日志 |
| `core.RepairBehavior` | 可注入的修复脚本接口（默认恒成功；测试用它制造失败/超时/替换） |
| `api.Controller` | 本地 REST 接口 |
| `demo.DemoRunner` | `--controller.demo=true` 时运行的确定性端到端演示 |

## 3. 策略语义（ScalingPolicy）

| 字段 | 语义 |
| --- | --- |
| `version` | 策略版本；只接受**严格递增**更新，相同/回退版本返回 `POLICY_VERSION_STALE` |
| `minReplicas` / `maxReplicas` | 副本上下界（含），要求 `max >= min` |
| `scaleUpThreshold` / `scaleDownThreshold` | 每副本平均负载的扩/缩容阈值，要求 `0 <= down < up`，形成**阈值滞后带** |
| `scaleUpCooldownMillis` / `scaleDownCooldownMillis` | 同方向两次伸缩的最小间隔；期内请求返回 `SCALE_IN_COOLDOWN` |
| `remediationEnabled` | 是否允许自愈；关闭时任何修复请求返回 `REMEDIATION_DISABLED` |
| `maxConcurrentRepairs` | 单工作负载并发修复上限；超出后任务入队等待 |
| `failureBudget` / `failureBudgetWindowMillis` | 滑动窗口内允许的修复失败次数；耗尽后新请求返回 `REM_FAILURE_BUDGET_EXHAUSTED`，窗口滑过自动恢复 |
| `repairTimeoutMillis` | 单次修复截止时间；到时未成功以 `REPAIR_TIMEOUT` 终态结束 |
| `repairCooldownMillis` | 同一实例两次修复的最小间隔，计时从**上一个任务终态**开始 |
| `unhealthyThreshold` | 连续不健康帧门槛，用于吸收健康抖动；中间出现健康帧则计数清零 |
| `observationMaxAgeMillis` | 最新负载观测最大允许年龄；超龄返回 `OBSERVATION_STALE`（0 表示不检查） |

### 3.1 目标副本数（确定性纯函数）

```
avg = load / max(currentReplicas, 1)
avg > scaleUpThreshold   -> desiredRaw = ceil(load / scaleUpThreshold)
avg < scaleDownThreshold -> desiredRaw = max(1, floor(load / scaleDownThreshold))
滞后带内（down <= avg <= up）        -> 保持 currentReplicas 不变
最终 target = clamp(desiredRaw, minReplicas, maxReplicas)
```

方向与边界拒绝依据**未裁剪的 `desiredRaw`** 判定：已在上限但仍需更多容量时给出
`SCALE_AT_MAX_REPLICAS`，已在下限且仍需缩容时给出 `SCALE_AT_MIN_REPLICAS`，
绝不把“到顶”模糊成“无需伸缩”。扩容一步直接到 target（不超额、不漏步），
缩容逐个确定性地终止插入顺序最后的 `HEALTHY` 实例（绝不终止修复中的实例）。

### 3.2 观测规则

- 观测按 `sourceId` 各自维护单调 `sequence`：相等序号 -> `OBSERVATION_DUPLICATE`，
  序号回退 -> `OBSERVATION_STALE`，二者都**不覆盖**既有观测与状态。
- 从未收到观测、或最新观测显式不携带负载值（`load=null`）-> `OBSERVATION_MISSING`，
  编排器不会猜测副本数。
- “最新观测”的选取是确定的：`observedAt` 最大；同时间戳按 `sourceId` 字典序最大。

## 4. 状态迁移与不变量

实例状态机：

```
HEALTHY ──连续不健康达门槛──> UNHEALTHY ──任务启动──> REPAIRING
  ^                            │  │                     │
  └────────修复成功────────────┘  │                     ├─成功─> HEALTHY
                                 │                     ├─替换─> REPLACED（同时新建 HEALTHY 实例）
                                 │                     └─超时/预算耗尽/尝试耗尽─> REMEDIATION_FAILED
健康实例缩容 ───────────────────────────────────────────────────────────────> TERMINATED
```

核心不变量：

1. **单调状态版本**：每次接受观测、策略更新、伸缩、健康/实例状态变化都令工作负载
   `stateVersion` 加一；拒绝不推进状态。
2. **副本数一致**：扩容创建的新实例数与 `currentReplicas` 增量严格相等；
   `currentReplicas` 只随成功的伸缩迁移变化。
3. **修复幂等**：同一实例任意时刻至多存在一个“排队中或执行中”的任务；
   重复触发返回 `REM_ALREADY_QUEUED` / `REM_ALREADY_IN_PROGRESS`，不会产生第二个任务。
4. **终态不重试、恢复无残留**：任务以 `REPAIR_TIMEOUT` / `REPAIR_FAILED` /
   `REPAIR_ATTEMPTS_EXHAUSTED` / `REM_FAILURE_BUDGET_EXHAUSTED` 结束后立即移出执行集合；
   排队期间实例恢复/消失则任务作废（`REPAIR_INSTANCE_GONE`，不计失败、不重试）；
   任何路径都不会留下无限重试的任务。
5. **失败预算终态可区分**：预算只统计终态失败（FAILED 尝试耗尽、超时、执行中预算耗尽），
   滑动窗口外自动剔除；预算耗尽时新请求被拒绝且**不入队**。

## 5. 并发规则

- 注册表用一把短临界区锁保护工作负载 Map；每个 `Workload` 有自己的 `monitor`。
- 每次编排（伸缩评估、修复触发/执行）都在对应工作负载 monitor 内完成
  “读输入 → 判定 → 提交迁移”全过程：同一工作负载严格串行，不同工作负载完全并行。
- 因此在任意线程交错下：并发的扩容触发只有一次生效（其余得到冷却/上限等确定性拒绝），
  并发的修复触发只会启动一个任务；不存在超额扩容、漏扩容或重复修复。
- 修复队列按 FIFO（taskId 递增）晋升；一个 tick 内：先处理本拍开始时已在执行的任务
  （按 taskId 排序），再循环“晋升队首 → 立即执行”，直到队列无法推进
  （并发上限/预算/实例已恢复）。工作负载按 id 字典序处理，全程无随机源。

## 6. 可区分的拒绝/终态原因（RejectReason）

`WORKLOAD_NOT_FOUND`、`WORKLOAD_ALREADY_EXISTS`、`POLICY_INVALID`、`POLICY_VERSION_STALE`、
`OBSERVATION_MISSING`、`OBSERVATION_STALE`、`OBSERVATION_DUPLICATE`、
`SCALE_IN_COOLDOWN`、`SCALE_AT_MIN_REPLICAS`、`SCALE_AT_MAX_REPLICAS`、`SCALE_IN_PROGRESS`、
`REMEDIATION_DISABLED`、`REM_ALREADY_QUEUED`、`REM_ALREADY_IN_PROGRESS`、
`REM_NO_UNHEALTHY_INSTANCE`、`REM_IN_REMEDIATION_COOLDOWN`、`REM_QUEUED`、
`REM_CONCURRENCY_LIMITED`、`REM_FAILURE_BUDGET_EXHAUSTED`、
`REPAIR_TIMEOUT`、`REPAIR_FAILED`、`REPAIR_ATTEMPTS_EXHAUSTED`、`REPAIR_INSTANCE_GONE`、
`NO_SCALE_NEEDED`、`NO_QUEUED_TASK`。

被拒绝的编排操作通过 HTTP 返回 `422 Unprocessable Entity`，响应体仍包含完整原因与细节。

## 7. HTTP 接口（本地）

| 方法与路径 | 说明 |
| --- | --- |
| `POST /api/workloads` | 注册工作负载（body：workloadId、policy、initialReplicas） |
| `POST /api/workloads/{id}/policy` | 版本化策略更新 |
| `POST /api/workloads/{id}/observations` | 上报负载观测（sourceId、sequence、load；load 可省略/为 null） |
| `POST /api/workloads/{id}/scale` | 触发一次伸缩评估 |
| `POST /api/workloads/{id}/health` | 上报实例健康帧（instanceId、healthy） |
| `POST /api/workloads/{id}/remediations/scan` | 扫描全部达门槛的不健康实例并生成修复决策 |
| `POST /api/workloads/{id}/remediations` | 对指定实例触发修复 |
| `POST /api/tick` | 推进虚拟时钟（deltaMillis，可省为 0）并执行一轮修复循环 |
| `GET /api/workloads/{id}` | 当前状态快照（副本、实例状态、观测、队列/预算） |
| `GET /api/workloads` | 已注册工作负载 id 列表 |
| `GET /api/decisions` | 全量决策日志 |

示例：

```bash
curl -s -X POST localhost:8080/api/workloads -H 'Content-Type: application/json' -d '{
  "workloadId":"svc","initialReplicas":2,
  "policy":{"version":1,"minReplicas":2,"maxReplicas":4,
    "scaleUpThreshold":1.0,"scaleDownThreshold":0.4,
    "scaleUpCooldownMillis":1000,"scaleDownCooldownMillis":1000,
    "remediationEnabled":true,"maxConcurrentRepairs":1,
    "failureBudget":2,"failureBudgetWindowMillis":10000,
    "repairTimeoutMillis":2000,"repairCooldownMillis":500,
    "unhealthyThreshold":2,"observationMaxAgeMillis":0}}'
curl -s -X POST localhost:8080/api/workloads/svc/observations \
  -H 'Content-Type: application/json' -d '{"sourceId":"s","sequence":1,"load":6.0}'
curl -s -X POST localhost:8080/api/workloads/svc/scale
```

## 8. 测试覆盖

- `AutoscalerSemanticsTest`：非法策略多错误项、观测缺失/超龄、阈值滞后带、
  冷却与上下限拒绝、策略版本回退、纯函数可复现。
- `OrchestrationConcurrencyTest`：16 线程并发伸缩恰有一次生效、12 线程并发修复只启动一个
  任务、重复/乱序观测不覆盖、固定输入序列（含乱序观测）两次执行最终状态指纹完全一致。
- `RemediationScenarioTest`：单帧抖动吸收与计数清零、并发上限排队与按序晋升、
  失败预算耗尽拒绝且不入队、滑动窗口恢复、超时终态、尝试次数耗尽、策略禁止、
  修复冷却、恢复后无重复任务残留。

确定性来源：虚拟时钟、固定阈值公式、确定的选取顺序（工作负载 id、taskId、实例插入顺序、
sourceId 字典序）、无随机数与无真实并发副作用（并发只通过锁串行化）。
