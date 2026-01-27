# APACHE DORIS PROJECT KNOWLEDGE BASE

**Generated:** 2026-01-27
**Commit:** 09d9883773e
**Branch:** opencode-agent

## OVERVIEW

Apache Doris is a real-time analytical database using MPP architecture. Two-process design: FE (Java, query planning, metadata) + BE (C++, storage, execution). Cloud-native mode adds meta-service for compute-storage separation.

## STRUCTURE

```
doris/
├── fe/                 # Frontend - Java, Maven multi-module
│   ├── fe-core/        # Main FE logic: catalog, nereids optimizer, planner, qe
│   ├── fe-common/      # Shared FE utilities
│   └── be-java-extensions/  # Java extensions for BE (JDBC, UDF, etc.)
├── be/                 # Backend - C++, CMake
│   └── src/
│       ├── olap/       # Storage engine: tablets, rowsets, compaction
│       ├── vec/        # Vectorized execution: Block, Column, functions
│       ├── pipeline/   # Pipeline execution framework
│       ├── exec/       # Query operators and scanners
│       └── runtime/    # Runtime state, memory management
├── cloud/              # Cloud-native components - C++
│   └── src/
│       ├── meta-service/   # Centralized metadata management
│       └── recycler/       # Cloud storage garbage collection
├── gensrc/             # Protocol definitions
│   ├── thrift/         # Thrift IDL for FE-BE communication
│   └── proto/          # Protobuf for internal services
├── regression-test/    # Groovy-based test framework
│   ├── suites/         # Test cases by category
│   └── framework/      # Test infrastructure
├── thirdparty/         # Pre-built dependencies
├── bin/                # Startup scripts (start_fe.sh, start_be.sh)
└── conf/               # Configuration templates
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Add SQL function | `fe/.../nereids/trees/expressions/functions/` + `be/src/vec/functions/` | FE defines signature, BE implements |
| Modify query optimizer | `fe/.../nereids/rules/` | Rewrite rules in `rewrite/`, CBO in `exploration/` |
| Add storage feature | `be/src/olap/` | Tablet, Rowset, Segment abstractions |
| Add data type | `be/src/vec/data_types/` + `be/src/vec/columns/` | DataType + Column pair |
| Add external catalog | `fe/.../datasource/` | Hive, Iceberg, etc. integrations |
| Modify FE-BE protocol | `gensrc/thrift/` or `gensrc/proto/` | Regenerate after changes |
| Add regression test | `regression-test/suites/` | Groovy scripts, `suite("name") {}` |
| Cloud-specific change | `cloud/src/` + `be/src/cloud/` | Meta-service or recycler |

## CONVENTIONS

### Code Style
- **Java**: Checkstyle enforced (120 char lines), `org.apache.doris` namespace
- **C++**: Google style via `.clang-format` (100 char lines, 4-space indent), `doris::` namespace
- **Groovy tests**: Use `def` for local vars, avoid global session changes

### Naming
- Java: `CamelCase` classes, `camelCase` methods
- C++: `snake_case` files, `CamelCase` classes, `snake_case` methods/vars
- Tests: `test_*.groovy`, C++ `*_test.cpp`

### Module Boundaries
- FE-BE: Thrift RPC (`FrontendService`, `BackendService`)
- BE-Cloud: Protobuf RPC (`MetaService`)
- No direct cross-language calls

## ANTI-PATTERNS (THIS PROJECT)

### Critical Warnings
- **DO NOT** modify PartitionKey/ListPartitionItem/RangePartitionItem without compatibility guarantee
- **DO NOT** change PrimitiveType.desc or toString (used in persistence)
- **DO NOT** call hdfsDisconnect() - causes "Filesystem closed" errors
- **DO NOT** alter doris_audit_log or internal statistics tables
- **DO NOT** interrupt DynamicPartitionScheduler thread
- **DO NOT** write edit log in schema change loops
- **NEVER** touch transaction constants in cloud/src/meta-service/doris_txn.cpp

### Deprecated (Avoid)
- LakeSoul catalog support (scheduled for removal)
- Legacy planner (use Nereids)
- Many OperationType/Privilege enums marked @Deprecated

## COMMANDS

```bash
# Build
./build.sh                    # All components
./build.sh --fe               # Frontend only
./build.sh --be               # Backend only
./build.sh --cloud            # Cloud components
./build.sh --clean            # Clean build
./build.sh -j 16              # Parallel build

# Test
./run-fe-ut.sh                # FE unit tests
./run-be-ut.sh                # BE unit tests
./run-regression-test.sh      # Regression tests

# Start/Stop
./bin/start_fe.sh             # Start Frontend
./bin/start_be.sh             # Start Backend
./bin/stop_fe.sh
./bin/stop_be.sh
```

## NOTES

- **Nereids** is the new optimizer replacing the legacy planner. All new optimization work goes there.
- **Vectorized engine** (be/src/vec/) processes data in columnar Blocks for SIMD efficiency.
- **Cloud mode** separates compute/storage; BE queries meta-service instead of local metadata.
- **Regression tests** run in parallel by default; use `nonConcurrent` tag for stateful tests.
- **Thirdparty** deps are pre-built; run `thirdparty/build-thirdparty.sh` if missing.
