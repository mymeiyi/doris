# MetaService RPC 自适应限流设计与实现方案

## 1. 背景

`MetaServiceProxy` 的 `executeWithMetrics` 方法已实现基于固定配置值的 RPC 限流。在此基础上，需要增加自适应限流能力：当 meta-service 出现过载信号时自动降低 QPS 阈值，过载消除后逐步恢复至配置值。

过载信号来源于两种场景：

1. **超时信号**：发往 meta-service 的 RPC 持续返回 gRPC `DEADLINE_EXCEEDED`，表明服务端处理能力不足。
2. **服务端背压信号**：meta-service 返回的 response 中 `MetaServiceCode` 为 `MAX_QPS_LIMIT`（code 6001），表明服务端主动拒绝请求。

## 2. 整体架构

### 2.1 信号流

```
MetaServiceProxy.executeWithMetrics()
  │
  ├─ 成功响应 → MetaServiceCodeExtractor.isMaxQpsLimit(response)
  │              ├─ true  → Signal.BACKPRESSURE
  │              └─ false → Signal.SUCCESS
  │
  └─ RpcException → RpcException.isTimeout()
                     ├─ true  → Signal.TIMEOUT
                     └─ false → Signal.SUCCESS
                                    │
                                    ▼
                   MetaServiceAdaptiveThrottle.recordSignal(signal)
                                    │
                                    ▼
                          状态机状态转换 + factor 调整
                                    │
                                    ▼
                     FactorChangeListener 回调通知
                                    │
                                    ▼
                   MetaServiceRateLimiter.setAdaptiveFactor(factor)
                                    │
                                    ▼
                   各 MethodRateLimiter.applyAdaptiveFactor(factor)
                                    │
                                    ▼
                   RateLimiter.setRate(max(1.0, configuredQps * factor))
```

### 2.2 状态机

```
                    ┌──────────────────────────────────────────────────┐
                    │                                                  │
                    ▼                                                  │
                 NORMAL ──[过载触发]──► FAST_DECREASE                  │
                                          │                            │
                                          │ [过载消除]                  │
                                          ▼                            │
                                       COOLDOWN                       │
                                          │                            │
                              ┌───────────┼───────────┐               │
                              │           │           │               │
                         [过载触发]  [冷却到期]    [等待中]            │
                              │           │           │               │
                              ▼           ▼           │               │
                        FAST_DECREASE  SLOW_RECOVERY  │               │
                                          │           │               │
                              ┌───────────┤           │               │
                              │           │           │               │
                         [过载触发]  [factor=1.0]     │               │
                              │           │           │               │
                              ▼           └───────────┼───────────────┘
                        FAST_DECREASE                 │
                                                      │
                                               (保持COOLDOWN)
```

| 状态 | 含义 | 行为 |
|------|------|------|
| `NORMAL` | 系统正常，factor = 1.0 | 检测到过载 → 进入 `FAST_DECREASE` 并降低 factor |
| `FAST_DECREASE` | 持续过载，快速降低 QPS | 每次检测到过载执行 `factor = factor * decreaseMultiplier`；过载消除 → 进入 `COOLDOWN` |
| `COOLDOWN` | 等待系统稳定 | 等待 `cooldown_ms` 毫秒；期间再次过载 → 回到 `FAST_DECREASE`；冷却到期 → 进入 `SLOW_RECOVERY` |
| `SLOW_RECOVERY` | 缓慢恢复 QPS | 每隔 `recovery_interval_ms` 执行 `factor += recovery_step`；factor 达到 1.0 → 回到 `NORMAL`；期间过载 → 回到 `FAST_DECREASE` |

### 2.3 过载判定条件

三个条件同时满足时判定为过载：

```
windowTotal >= min_window_requests        // 窗口内请求数达到最低阈值
  && windowBad >= bad_trigger_count       // 坏信号（超时+背压）达到最低次数
  && windowBad / windowTotal >= bad_rate_trigger  // 坏信号比率达到触发阈值
```

## 3. 模块设计

### 3.1 RpcException 改造

**文件**: `fe/fe-core/src/main/java/org/apache/doris/rpc/RpcException.java`

增加 `FailureType` 枚举区分失败类型：

```java
public enum FailureType {
    TIMEOUT,   // gRPC DEADLINE_EXCEEDED
    OTHER      // UNAVAILABLE, UNKNOWN 等
}
```

- 新增构造函数 `RpcException(String host, String message, Exception e, FailureType failureType)`
- 新增方法 `isTimeout()` 和 `getFailureType()`
- 原有构造函数默认使用 `FailureType.OTHER`，保持向后兼容

### 3.2 MetaServiceCodeExtractor

**文件**: `fe/fe-core/src/main/java/org/apache/doris/cloud/rpc/MetaServiceCodeExtractor.java`（新建）

从 protobuf response 对象中提取 `MetaServiceCode`，采用 `MethodHandle` 缓存实现高性能反射访问。

核心方法：

| 方法 | 说明 |
|------|------|
| `tryGetCode(Object response)` | 返回 `OptionalInt`，提取失败返回 empty（fail-open） |
| `isMaxQpsLimit(Object response)` | 判断 code 是否为 `MAX_QPS_LIMIT`（6001） |
| `clearCache()` | 清除缓存（测试用） |

实现原理：
- 所有 cloud.proto response 消息共享 `optional MetaServiceResponseStatus status = 1` 字段
- 通过 `MethodHandles.publicLookup()` 构建 `hasStatus()` → `getStatus()` → `getCode()` 调用链
- 以 `ConcurrentHashMap<Class<?>, MethodHandle[]>` 缓存，每个 response 类型仅解析一次

### 3.3 MetaServiceAdaptiveThrottle

**文件**: `fe/fe-core/src/main/java/org/apache/doris/cloud/rpc/MetaServiceAdaptiveThrottle.java`（新建）

自适应限流核心控制器，单例模式。

#### 3.3.1 核心数据结构

```java
// 状态机
private volatile State state = State.NORMAL;
private volatile double factor = 1.0;

// 滑动窗口计数器
private final LongAdder windowTotal;    // 窗口内总请求数
private final LongAdder windowBad;      // 窗口内坏信号数（超时+背压）
private volatile long windowStartMs;    // 当前窗口起始时间

// 状态时间戳
private volatile long cooldownStartMs;  // 进入 COOLDOWN 的时间
private volatile long lastRecoveryMs;   // 上次恢复的时间

// 回调
private final AtomicReference<FactorChangeListener> listenerRef;
```

#### 3.3.2 主要方法

| 方法 | 说明 |
|------|------|
| `recordSignal(Signal)` | 主入口，记录信号并驱动状态机转换 |
| `isOverloaded()` | 根据窗口统计判断是否过载 |
| `decreaseFactor()` | 执行 `factor = max(minFactor, factor * decreaseMultiplier)` |
| `setFactor(double)` | 更新 factor 并通知 listener、更新 metrics |
| `transitionTo(State, long)` | 状态转换，重置窗口计数器，更新 metrics |

#### 3.3.3 FactorChangeListener

```java
public interface FactorChangeListener {
    void onFactorChanged(double newFactor);
}
```

当 factor 变化超过 0.001 时触发回调，通知 `MetaServiceRateLimiter` 调整实际 QPS。

### 3.4 MetaServiceRateLimiter 扩展

**文件**: `fe/fe-core/src/main/java/org/apache/doris/cloud/rpc/MetaServiceRateLimiter.java`

在已有限流器基础上增加自适应能力：

- `MethodRateLimiter` 增加 `configuredQps` 字段，记录配置的原始 QPS
- `MethodRateLimiter.applyAdaptiveFactor(double factor)`：执行 `rateLimiter.setRate(max(1.0, configuredQps * factor))`
- `MetaServiceRateLimiter.setAdaptiveFactor(double factor)`：遍历所有 `MethodRateLimiter` 并应用 factor
- 构造函数中，当 `meta_service_rpc_adaptive_throttle_enabled = true` 时注册为 `FactorChangeListener`

### 3.5 MetaServiceProxy 信号采集

**文件**: `fe/fe-core/src/main/java/org/apache/doris/cloud/rpc/MetaServiceProxy.java`

在 `executeWithMetrics` 方法中采集信号：

```java
// 成功路径
Response response = w.executeRequest(methodName, function);
if (Config.meta_service_rpc_adaptive_throttle_enabled) {
    Signal signal = MetaServiceCodeExtractor.isMaxQpsLimit(response)
            ? Signal.BACKPRESSURE : Signal.SUCCESS;
    MetaServiceAdaptiveThrottle.getInstance().recordSignal(signal);
}

// 失败路径
catch (RpcException e) {
    if (Config.meta_service_rpc_adaptive_throttle_enabled) {
        Signal signal = e.isTimeout() ? Signal.TIMEOUT : Signal.SUCCESS;
        MetaServiceAdaptiveThrottle.getInstance().recordSignal(signal);
    }
    throw e;
}
```

在 `MetaServiceClientWrapper.executeRequest` 中，当 gRPC 返回 `DEADLINE_EXCEEDED` 时构造带 `FailureType.TIMEOUT` 的 `RpcException`：

```java
case DEADLINE_EXCEEDED:
    isTimeout = true;
    shouldRetry = tried <= Config.meta_service_rpc_timeout_retry_times;
    break;
// ...
RpcException.FailureType failureType = isTimeout
        ? RpcException.FailureType.TIMEOUT : RpcException.FailureType.OTHER;
throw new RpcException("", sre.getMessage(), sre, failureType);
```

### 3.6 CloudMetrics 监控指标

**文件**: `fe/fe-core/src/main/java/org/apache/doris/metric/CloudMetrics.java`

新增 4 个指标：

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `meta_service_rpc_adaptive_throttle_factor` | `GaugeMetricImpl<Double>` | 当前 factor 值（0.1–1.0），默认 1.0 |
| `meta_service_rpc_adaptive_throttle_state` | `GaugeMetricImpl<String>` | 当前状态机状态，默认 "NORMAL" |
| `meta_service_rpc_adaptive_throttle_timeout_signals` | `LongCounterMetric` | 累计超时信号数 |
| `meta_service_rpc_adaptive_throttle_backpressure_signals` | `LongCounterMetric` | 累计背压信号数 |

## 4. 配置参数

所有参数均支持运行时动态修改（`mutable = true`），位于 `fe-common/.../Config.java`。

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `meta_service_rpc_adaptive_throttle_enabled` | boolean | false | 是否启用自适应限流 |
| `meta_service_rpc_adaptive_throttle_min_factor` | double | 0.1 | factor 下限，QPS 最低为 configuredQps × 0.1 |
| `meta_service_rpc_adaptive_throttle_decrease_multiplier` | double | 0.7 | 快速下降乘数，每次过载 factor × 0.7 |
| `meta_service_rpc_adaptive_throttle_cooldown_ms` | long | 30000 | 冷却等待时间（ms） |
| `meta_service_rpc_adaptive_throttle_recovery_interval_ms` | long | 5000 | 恢复间隔（ms） |
| `meta_service_rpc_adaptive_throttle_recovery_step` | double | 0.05 | 每次恢复步长 |
| `meta_service_rpc_adaptive_throttle_window_seconds` | int | 10 | 滑动窗口时长（秒） |
| `meta_service_rpc_adaptive_throttle_min_window_requests` | int | 20 | 触发过载判定的最小请求数 |
| `meta_service_rpc_adaptive_throttle_bad_trigger_count` | int | 3 | 触发过载判定的最小坏信号数 |
| `meta_service_rpc_adaptive_throttle_bad_rate_trigger` | double | 0.05 | 触发过载判定的最小坏信号比率 |

### 4.1 调参示例

**场景 A：激进降级、快速恢复**（对延迟敏感的在线查询场景）

```
meta_service_rpc_adaptive_throttle_decrease_multiplier = 0.5
meta_service_rpc_adaptive_throttle_cooldown_ms = 10000
meta_service_rpc_adaptive_throttle_recovery_interval_ms = 2000
meta_service_rpc_adaptive_throttle_recovery_step = 0.1
```

**场景 B：保守降级、缓慢恢复**（批量导入场景，避免 QPS 抖动）

```
meta_service_rpc_adaptive_throttle_decrease_multiplier = 0.8
meta_service_rpc_adaptive_throttle_cooldown_ms = 60000
meta_service_rpc_adaptive_throttle_recovery_interval_ms = 10000
meta_service_rpc_adaptive_throttle_recovery_step = 0.02
```

### 4.2 降级过程示例

假设 `configuredQps = 1000`，使用默认参数：

```
时间  状态            factor  effectiveQps  事件
T+0   NORMAL          1.0     1000          检测到过载
T+0   FAST_DECREASE   0.7     700           factor × 0.7
T+1   FAST_DECREASE   0.49    490           继续过载，factor × 0.7
T+2   FAST_DECREASE   0.343   343           继续过载，factor × 0.7
T+3   COOLDOWN        0.343   343           过载消除，进入冷却
T+33  SLOW_RECOVERY   0.343   343           冷却30秒到期
T+38  SLOW_RECOVERY   0.393   393           factor + 0.05
T+43  SLOW_RECOVERY   0.443   443           factor + 0.05
...
T+98  SLOW_RECOVERY   0.993   993           factor + 0.05
T+103 NORMAL          1.0     1000          恢复至 1.0
```

## 5. 变更文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `fe/fe-core/.../rpc/RpcException.java` | 修改 | 增加 `FailureType` 枚举和 `isTimeout()` |
| `fe/fe-common/.../common/Config.java` | 修改 | 增加 10 个自适应限流配置项 |
| `fe/fe-core/.../cloud/rpc/MetaServiceCodeExtractor.java` | 新建 | Protobuf response code 提取器 |
| `fe/fe-core/.../cloud/rpc/MetaServiceAdaptiveThrottle.java` | 新建 | 自适应限流状态机控制器 |
| `fe/fe-core/.../cloud/rpc/MetaServiceRateLimiter.java` | 修改 | 增加 `applyAdaptiveFactor` 和 listener 注册 |
| `fe/fe-core/.../cloud/rpc/MetaServiceProxy.java` | 修改 | 信号采集和 `FailureType` 传递 |
| `fe/fe-core/.../metric/CloudMetrics.java` | 修改 | 增加 4 个自适应限流监控指标 |

## 6. 测试用例

### 6.1 MetaServiceAdaptiveThrottleTest

**文件**: `fe/fe-core/src/test/java/org/apache/doris/cloud/rpc/MetaServiceAdaptiveThrottleTest.java`

| 测试方法 | 验证内容 |
|----------|----------|
| `testInitialState` | 初始状态为 NORMAL，factor = 1.0 |
| `testDisabledDoesNothing` | 关闭功能时信号不影响状态 |
| `testSuccessKeepsNormal` | 纯成功信号保持 NORMAL |
| `testTimeoutTriggersDecrease` | 超时信号触发 FAST_DECREASE |
| `testBackpressureTriggersDecrease` | 背压信号触发 FAST_DECREASE |
| `testFactorDoesNotGoBelowMin` | factor 不低于 min_factor |
| `testFastDecreaseToCooldownTransition` | 过载消除后转入 COOLDOWN |
| `testCooldownToSlowRecoveryTransition` | 冷却到期后转入 SLOW_RECOVERY |
| `testSlowRecoveryIncreasesFactorGradually` | 恢复阶段 factor 逐步增加 |
| `testSlowRecoveryBackToNormal` | factor 达到 1.0 回到 NORMAL |
| `testCooldownBadSignalGoesBackToFastDecrease` | COOLDOWN 中再次过载回到 FAST_DECREASE |
| `testSlowRecoveryBadSignalGoesBackToFastDecrease` | SLOW_RECOVERY 中再次过载回到 FAST_DECREASE |
| `testFactorChangeListenerCalled` | listener 回调被调用 |
| `testBelowMinWindowRequestsDoesNotTrigger` | 请求数不足时不触发过载 |
| `testWindowResets` | 窗口到期后计数器重置 |

### 6.2 MetaServiceCodeExtractorTest

**文件**: `fe/fe-core/src/test/java/org/apache/doris/cloud/rpc/MetaServiceCodeExtractorTest.java`

| 测试方法 | 验证内容 |
|----------|----------|
| `testTryGetCodeWithOkResponse` | 正确提取 OK code |
| `testTryGetCodeWithMaxQpsLimit` | 正确提取 MAX_QPS_LIMIT (6001) |
| `testIsMaxQpsLimitReturnsTrue` | `isMaxQpsLimit` 对 6001 返回 true |
| `testIsMaxQpsLimitReturnsFalseForOk` | `isMaxQpsLimit` 对 OK 返回 false |
| `testTryGetCodeWithNullResponse` | null 输入返回 empty |
| `testTryGetCodeWithNonProtobufObject` | 非 protobuf 对象返回 empty（fail-open） |
| `testTryGetCodeWithNoStatusSet` | 未设置 status 返回 empty |
| `testCacheIsPopulatedOnSecondCall` | 第二次调用命中缓存 |
| `testDifferentResponseTypes` | 不同 response 类型均可正确提取 |
| `testClearCacheAllowsRepopulation` | 清除缓存后可重新填充 |

## 7. 设计决策

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 信号采集位置 | `executeWithMetrics` 方法 | 所有 RPC 调用的统一入口，覆盖完整 |
| Code 提取方式 | MethodHandle 缓存反射 | 避免硬编码每种 response 类型，自动适配新增的 proto message |
| 过载判定 | 三重条件（最小请求数 + 最小坏信号数 + 坏信号率） | 防止低流量时的误判 |
| 窗口实现 | 固定窗口（非滑动窗口） | 实现简单且足够准确，状态转换时自动重置窗口 |
| factor 下限 | 可配置，默认 0.1 | 避免 QPS 降为 0 导致服务完全不可用 |
| Fail-open 策略 | Code 提取失败不阻断请求 | 自适应限流是增强功能，不应成为新的故障点 |
| 默认关闭 | `enabled = false` | 安全上线，需要显式开启 |
| 非超时异常处理 | 记录为 SUCCESS | UNAVAILABLE/UNKNOWN 等错误表示连接问题而非服务端过载，不应触发降级 |
