# Extension Development Guide

Extensions provide ecosystem integrations and extensibility beyond core Doris. Two distinct areas exist:

1. **Ecosystem extensions** (`extension/`): Third-party tool integrations (dbt, DataX, Logstash, Kettle)
2. **Core extensions** (`fe/be-java-extensions/`): Built-in Java extensions for UDFs and external catalogs

## Directory Structure

```
extension/
├── dbt-doris/           # dbt adapter for data transformation (Python)
├── DataX/doriswriter/   # Alibaba DataX plugin for data loading
├── logstash/            # Logstash output plugin (Ruby)
├── kettle/              # Kettle ETL plugin (Java)
├── mysql_to_doris/      # MySQL migration tool
└── beats/               # Filebeat/Metricbeat plugins (Go)

fe/be-java-extensions/   # Core Java extensions
├── java-udf/            # Java UDF/UDAF framework
├── jdbc-scanner/        # JDBC external table scanning
├── hadoop-hudi-scanner/ # Hudi catalog support
├── paimon-scanner/      # Paimon catalog support
├── iceberg-metadata-scanner/  # Iceberg catalog support
├── max-compute-scanner/ # MaxCompute external table
├── avro-scanner/        # Avro format support
├── trino-connector-scanner/  # Trino connector integration
└── java-common/         # Shared utilities
```

## Where to Look

| Task | Path | Key Files |
|------|------|-----------|
| **UDF development** | `fe/be-java-extensions/java-udf/` | UdfExecutor.java, UdafExecutor.java, BaseExecutor.java |
| **UDAF state mgmt** | `fe/be-java-extensions/java-udf/` | UdafExecutor.java (create/add/merge/getValue/serialize/destroy) |
| **Vectorized execution** | `fe/be-java-extensions/java-udf/` | VectorTable.java (columnar data handling) |
| **dbt integration** | `extension/dbt-doris/` | dbt/adapters/doris/, dbt/include/ |
| **DataX loading** | `extension/DataX/doriswriter/` | DorisWriter.java, writer configuration |
| **ETL tools** | `extension/kettle/`, `extension/logstash/` | Tool-specific plugins |
| **JDBC catalog** | `fe/be-java-extensions/jdbc-scanner/` | JdbcScanner.java |
| **Hive/Hudi/Paimon** | `fe/be-java-extensions/*-scanner/` | Catalog-specific scanners |

## UDF Framework Conventions

### UDF (Scalar Functions)
- **Required method**: `public Type evaluate(Type... args)` - single public `evaluate()` method
- **Null handling**: Return null for null inputs (e.g., `return a == null ? null : a + b`)
- **Type matching**: Method signature must match SQL types defined in CREATE FUNCTION

Example:
```java
public class SimpleAddUdf {
    public Integer evaluate(Integer a, int b) {
        return a == null ? null : a + b;
    }
}
```

### UDAF (Aggregate Functions)
UDAFs require state management across aggregation phases:

- **`create()`**: Initialize new state object (called once per aggregation instance)
- **`add(State state, Type... args)`**: Add row to aggregation (hot path, called per row)
- **`getValue(State state)`**: Return final result after all rows processed
- **`merge(State dest, State src)`**: Combine partial aggregates (for distributed execution)
- **`serialize(State state, DataOutputStream out)`**: Write state to byte array
- **`deserialize(DataInputStream in)`**: Read state from byte array
- **`reset(State state)`**: Reset state to initial value
- **`destroy(State state)`**: Cleanup resources

### VectorTable for Vectorized Execution
- **Input**: `VectorTable.createReadableTable(inputParams)` for reading columnar batches
- **Output**: `VectorTable.createWritableTable(outputParams, numRows)` for writing results
- **Materialization**: `getMaterializedData()` converts columnar to row-based for UDF calls
- **Performance**: Batched processing reduces JNI call overhead

### Registration
```sql
CREATE FUNCTION my_udf(INTEGER, INTEGER) RETURNS INTEGER
PROPERTIES (
    "file"="hdfs://path/to/my-udf.jar",
    "symbol"="com.example.MyUdf"
);
```

## Anti-Patterns

❌ **Never confuse extension/ with fe/be-java-extensions/**
- `extension/` = third-party ecosystem tools (independent repos, own tests)
- `fe/be-java-extensions/` = core Doris Java extensions (built with FE, shipped with Doris)

❌ **Never commit ecosystem extensions without their own tests**
- Each ecosystem tool has its own test framework
- dbt-doris uses pytest, DataX uses its own test runner

❌ **Never modify generated UDF boilerplate**
- UDF classes should contain only business logic
- Framework handles serialization, type conversion, vectorization

❌ **Never forget null handling in UDFs**
- SQL NULL ≠ Java null in all contexts
- Always check for null inputs before operations

## Build Commands

```bash
# Core Java extensions (UDF, catalogs)
./build.sh --be-java-extensions           # Build all core extensions
./build.sh --be-extension-ignore avro-scanner   # Exclude specific module

# Individual extension modules
cd fe/be-java-extensions/java-udf && mvn clean package

# Ecosystem extensions (third-party tools)
cd extension/dbt-doris && pip install -e .
cd extension/DataX && python setup.py install
cd extension/logstash && gem build logstash-output-doris.gemspec
cd extension/kettle && mvn clean package
cd extension/beats && go build
```

## Key Differences

| Aspect | extension/ | fe/be-java-extensions/ |
|--------|------------|----------------------|
| **Purpose** | Third-party tools | Core Doris functionality |
| **Language** | Python, Ruby, Go | Java |
| **Built with** | tool-specific (pip/mvn/go) | Maven (fe build) |
| **Shipped with** | No (separate install) | Yes (in be/lib/java_extensions/) |
| **Tests** | Tool-specific frameworks | JUnit in src/test/ |
| **Examples** | dbt-doris, DataX, Logstash | java-udf, jdbc-scanner, paimon-scanner |
