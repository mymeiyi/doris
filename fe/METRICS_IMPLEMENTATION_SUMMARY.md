# Meta Service RPC Metrics Implementation Summary

## Overview
Added comprehensive metric tracking for all Meta Service RPC methods in cloud mode, including:
- Total RPC call counter (per method)
- Failed RPC call counter (per method)
- Cumulative latency counter (per method)
- Per-minute call rate gauge (per method)

## Files Modified

### 1. CloudMetrics.java
**Location:** `/fe-core/src/main/java/org/apache/doris/metric/CloudMetrics.java`

**Changes:**
- Added 4 new metric fields:
  - `META_SERVICE_RPC_ALL_COUNTER` - Total RPC calls per method
  - `META_SERVICE_RPC_FAILED_COUNTER` - Failed RPC calls per method
  - `META_SERVICE_RPC_LATENCY` - Cumulative latency in milliseconds per method
  - `META_SERVICE_RPC_PER_MINUTE` - Calls per minute gauge per method

- Initialized metrics in `init()` method using `AutoMappedMetric` pattern with "method" label

### 2. MetaServiceProxy.java
**Location:** `/fe-core/src/main/java/org/apache/doris/cloud/rpc/MetaServiceProxy.java`

**Changes:**
- Modified `executeRequest()` to accept `String methodName` parameter
- Added metric tracking in `executeRequest()`:
  - Records start time at method entry
  - On success: increments total counter and latency
  - On failure: increments failed counter and latency
  - Handles both `StatusRuntimeException` and general `Exception`

- Updated all 50+ RPC method calls to pass method name:
  - `beginTxn`, `commitTxn`, `abortTxn`, `precommitTxn`
  - `createTablets`, `updateTablet`, `getTabletStats`
  - `prepareIndex`, `commitIndex`, `dropIndex`
  - `preparePartition`, `commitPartition`, `dropPartition`
  - `getVersion`, `getCluster`, `createStage`, `getStage`, `dropStage`
  - `getIam`, `beginCopy`, `finishCopy`, `getCopyJob`
  - And many more...

- Added metric tracking for async method `getVisibleVersionAsync()`:
  - Uses callback to track success/failure after completion
  - Records latency in both success and failure paths

### 3. MetricCalculator.java
**Location:** `/fe-core/src/main/java/org/apache/doris/metric/MetricCalculator.java`

**Changes:**
- Added `metaServiceRpcLastCounter` map to track last counter values per method
- Added `initMetaServiceMetrics()` method to initialize tracking on first run
- Added `updateMetaServiceMetrics(long interval)` method to calculate per-minute rates:
  - Calculates RPM as: `(currentCount - lastCount) * 60.0 / interval`
  - Updates gauge metrics via `MetricRepo.updateMetaServiceRpcPerMinute()`
- Integrated into `update()` method lifecycle

### 4. MetricRepo.java
**Location:** `/fe-core/src/main/java/org/apache/doris/metric/MetricRepo.java`

**Changes:**
- Added `updateMetaServiceRpcPerMinute(String methodName, double value, List<MetricLabel> labels)` helper method
- Updates the `META_SERVICE_RPC_PER_MINUTE` gauge with calculated rate
- Follows same pattern as cluster-level metrics

## Metric Details

### Metric Names (Prometheus format)
1. **meta_service_rpc_total{method="<methodName>"}** - Counter
   - Total number of RPC calls per method
   - Unit: REQUESTS

2. **meta_service_rpc_failed{method="<methodName>"}** - Counter
   - Total number of failed RPC calls per method
   - Unit: REQUESTS

3. **meta_service_rpc_latency_ms{method="<methodName>"}** - Counter
   - Cumulative latency in milliseconds per method
   - Unit: MILLISECONDS
   - Can calculate average latency: latency_ms / total

4. **meta_service_rpc_per_minute{method="<methodName>"}** - Gauge
   - Calls per minute (calculated every 15 seconds)
   - Unit: NOUNIT
   - Formula: (current_count - last_count) * 60 / interval_seconds

## Usage Examples

### View all Meta Service RPC metrics
```
curl http://fe_host:fe_http_port/metrics | grep meta_service_rpc
```

### Sample output
```
meta_service_rpc_total{method="beginTxn"} 1500
meta_service_rpc_failed{method="beginTxn"} 10
meta_service_rpc_latency_ms{method="beginTxn"} 45000
meta_service_rpc_per_minute{method="beginTxn"} 12.5

meta_service_rpc_total{method="commitTxn"} 1480
meta_service_rpc_failed{method="commitTxn"} 5
meta_service_rpc_latency_ms{method="commitTxn"} 38000
meta_service_rpc_per_minute{method="commitTxn"} 12.0
```

### Calculate derived metrics
- **Success rate**: `(total - failed) / total * 100%`
- **Error rate**: `failed / total * 100%`
- **Average latency**: `latency_ms / total`
- **Calls per second**: `per_minute / 60`

## Benefits

1. **Observability**: Track performance and reliability of each Meta Service RPC method separately
2. **Debugging**: Identify which specific RPC methods are slow or failing
3. **Capacity Planning**: Monitor call rates per method to understand usage patterns
4. **Alerting**: Set up alerts on failure rates or high latency for critical methods
5. **Performance Analysis**: Compare latency across different RPC methods

## Testing Recommendations

1. Verify metrics appear after Meta Service RPC calls in cloud mode
2. Confirm failure metrics increment on RPC errors
3. Check per-minute rates are calculated correctly (every 15 seconds)
4. Validate metrics are labeled with correct method names
5. Test async method `getVisibleVersionAsync` metrics in callbacks

