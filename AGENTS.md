# Doris Development Guide

This guide provides instructions for building, testing, and maintaining the Apache Doris codebase.

## Project Overview

Apache Doris is a real-time analytical MPP database with a 3-tier architecture:

- **Frontend (FE)**: Java - query planning, metadata management, MySQL protocol handler
- **Backend (BE)**: C++ - storage engine, vectorized execution, pipeline model
- **Cloud**: C++ - separate compute-storage, Meta-Service, Recycler for cloud-native mode

**Stats**: ~35,900 files, ~660K lines of code

### Architecture Flow

```
Client (MySQL protocol) → FE (query planning) → BE (execution) → Storage
                               ↓
                          Metadata (BDB JE)
```

**FE Roles**:
- **Master**: Read/write metadata, syncs to Followers/Observers via BDB JE
- **Follower**: Read metadata, electable as new Master
- **Observer**: Read metadata, increases query concurrency

## Build Commands

Doris has three main components: Frontend (FE), Backend (BE), and Cloud.

- **Build everything:**
  ```bash
  ./build.sh
  ```

- **Build individual components:**
  ```bash
  ./build.sh --fe       # Build Frontend (Java)
  ./build.sh --be       # Build Backend (C++)
  ./build.sh --cloud    # Build Cloud component
  ./build.sh --broker   # Build Broker
  ```

- **Clean build:**
  ```bash
  ./build.sh --clean
  ```

- **Parallel build:**
  ```bash
  ./build.sh -j 4       # Run with 4 threads
  ```

## Test Commands

### Regression Tests (End-to-End)
The regression test framework is written in Groovy and located in `regression-test/`.

- **Run all tests:**
  ```bash
  ./run-regression-test.sh
  ```

- **Run a specific suite:**
  ```bash
  ./run-regression-test.sh --run -s <suite_name>
  # Example: ./run-regression-test.sh --run -s test_select
  ```

- **Run a specific directory:**
  ```bash
  ./run-regression-test.sh --run -d <directory_path>
  # Example: ./run-regression-test.sh --run -d demo
  ```

- **Run a specific file:**
  ```bash
  ./run-regression-test.sh --run -f <file_path>
  # Example: ./run-regression-test.sh --run -f regression-test/suites/demo/case.groovy
  ```

### Unit Tests
- **Frontend (Java) Tests:**
  ```bash
  cd fe
  mvn test
  ```
- **Backend (C++) Tests:**
  ```bash
  # Assuming standard build in be/build_Release
  cd be/build_Release
  # Run all tests
  ctest
  # Run specific test
  ctest -R <test_name_regex>
  ```

## Code Style & Conventions

### C++ (Backend)
- **Style:** Follows Google C++ Style Guide.
- **Formatter:** `clang-format` is used. Configuration is in `.clang-format` at the root.
- **Linting:** Use `clang-tidy` or similar tools if configured.
- **Naming:**
  - Class names: `PascalCase`
  - Function names: `PascalCase` (Doris specific variant of Google style) or `snake_case` depending on context (check surrounding code).
  - Variable names: `snake_case`
  - Member variables: `_snake_case` (often prefixed with `_`)
  - Constants: `kPascalCase` or `ALL_CAPS`

### Java (Frontend)
- **Style:** Standard Java conventions.
- **Checkstyle:** Configured in `fe/check/checkstyle/checkstyle.xml`.
  - To run checkstyle: `mvn checkstyle:check`
- **Naming:**
  - Classes: `PascalCase`
  - Methods: `camelCase`
  - Variables: `camelCase`
  - Constants: `UPPER_SNAKE_CASE`

### General Development Rules
- **Imports:**
  - Organize imports logically (system/standard libraries first, then third-party, then internal).
  - Remove unused imports.
- **Error Handling:**
  - C++: Use `Status` objects for return values to propagate errors. Avoid exceptions for control flow.
  - Java: Use standard exception handling.
- **Logging:**
  - C++: Use `LOG(INFO)`, `LOG(WARNING)`, etc. (glog style).
  - Java: Use `LOG.info()`, `LOG.warn()`, etc. (Log4j/Slf4j).

### Workflow
1. **Understand Context:** Before editing, search for similar implementations (using `grep` or `glob`).
2. **Implementation:** Follow existing patterns in the file/module.
3. **Verification:**
   - Always run relevant tests.
   - For new features, add new regression tests in `regression-test/suites/`.

## Codebase Structure

```
./
├── fe/                  # Frontend: Java query planning, metadata, MySQL protocol
│   ├── fe-core/         # Core FE logic
│   │   └── src/main/java/org/apache/doris/
│   │       ├── nereids/         # New Cascades optimizer (RBO + CBO + HBO)
│   │       ├── catalog/         # Env.java, metadata, BDB JE persistence
│   │       ├── load/            # StreamLoad, BrokerLoad, RoutineLoad, SparkLoad
│   │       ├── mysql/           # Auth.java, AuthenticatorManager, protocol handler
│   │       └── analysis/        # SQL parsing, AST building
│   └── check/checkstyle/       # checkstyle.xml (120 char line limit)
├── be/                  # Backend: C++ storage, vectorized execution, pipeline model
│   ├── src/
│   │   ├── vec/                 # Vectorized execution engine
│   │   ├── pipeline/            # Pipeline execution engine
│   │   ├── olap/                # Storage engine: segments, rowsets, compaction
│   │   ├── cloud/               # Cloud mode support
│   │   └── exec/                # Query execution operators
│   └── build_Release/           # CMake build output
├── cloud/               # Cloud: Separate compute-storage, Meta-Service, Recycler
│   ├── meta-service/             # Cloud metadata service
│   ├── recycler/                 # Data recycler for cleanup
│   └── resource-manager/         # Cloud resource management
├── ui/                  # Web UI: React/TypeScript dashboard
│   └── src/
│       ├── api/                  # API bindings
│       ├── pages/                # React components
│       └── components/           # Reusable UI components
├── regression-test/     # E2E tests: Groovy-based with p0/p1/p2 priorities
│   └── suites/                  # Test suites organized by feature
├── extension/           # Ecosystem: UDFs, connectors, ETL tools
│   ├── kettle/                   # Kettle ETL plugin
│   └── DataX/                    # Alibaba DataX plugin
├── gensrc/              # Generated code: Thrift/Protobuf (SKIP - don't document)
├── conf/                # Configurations: fe.conf, be.conf
└── docker/              # Containers: Compilation and runtime images
```

## Where to Look (Task-Based)

| Task | Location | Notes |
|------|----------|-------|
| **Query planning** | `fe/fe-core/src/main/java/org/apache/doris/nereids/` | New Cascades optimizer (RBO+CBO+HBO), HyperGraph join order, stats estimation |
| **Query execution** | `be/src/vec/` + `be/src/pipeline/` | Vectorized + Pipeline engine, columnar data structures |
| **Storage engine** | `be/src/olap/` | Segments, rowsets, compaction, tablet management, indexes |
| **Metadata** | `fe/fe-core/src/main/java/org/apache/doris/catalog/` | Env.java, BDB JE persistence, table/database schemas |
| **Data import** | `fe/fe-core/src/main/java/org/apache/doris/load/` | StreamLoad, BrokerLoad, RoutineLoad (Kafka), SparkLoad |
| **Authentication** | `fe/fe-core/src/main/java/org/apache/doris/mysql/` | Auth.java, AuthenticatorManager, LDAP support |
| **Cloud mode** | `cloud/src/` + `be/src/cloud/` | Meta-Service, storage vaults, compute-storage separation |

## Cloud Component

### Architecture
Cloud mode separates compute from storage using centralized Meta-Service and Recycler for multi-tenant cloud-native deployments.

**Structure:**
```
cloud/
├── src/
│   ├── main.cpp            # Entry point: meta-service + recycler via brpc
│   ├── meta-service/       # MetaServiceImpl: txn/rowset/tablet lifecycle mgmt
│   ├── recycler/           # Recycler: automated cleanup of expired data
│   ├── meta-store/         # TxnKv: FoundationDB/in-memory transactional KV
│   ├── resource-manager/   # ResourceManager: quotas and allocation
│   ├── rate-limiter/       # RateLimiter: API and bandwidth throttling
│   ├── snapshot/           # SnapshotManager: cluster snapshots
│   └── common/            # Cloud utilities and configs
└── CMakeLists.txt          # Build configuration
```

### Where to Look (Cloud)
| Task | Path | Key Files |
|------|------|-----------|
| **Meta-service** | `meta-service/` | meta_service.h, txn_lazy_committer.h |
| **Recycler** | `recycler/` | recycler.h, storage_vault_accessor.h |
| **Storage vaults** | `recycler/` | S3Accessor, HdfsAccessor, AzureAccessor |
| **Transaction KV** | `meta-store/` | txn_kv.h, keys.h, versionstamp.h |
| **Configuration** | `cloud/conf/` | doris_cloud.conf |

### Cloud Conventions
- **Namespace**: `doris::cloud`
- **Error handling**: `TxnErrorCode` for KV operations, `Status` for BE calls
- **Logging**: glog with `cloud_unique_id` tagging for multi-tenant tracing
- **Configuration**: `cloud_unique_id` format: `${version}:${instance_id}:${unique_id}`
- **Concurrency**: bthreads for async operations, thread pools for recycler tasks
- **Storage access**: `StorageVaultAccessor` abstraction for S3/HDFS/Azure

### Key Differences from Standalone
| Aspect | Standalone BE | Cloud Mode |
|---------|--------------|-------------|
| **Storage** | Local disks (olap/) | Object storage (S3/HDFS/Azure) via Storage Vaults |
| **Metadata** | BDB JE in FE | Centralized Meta-Service via TxnKV |
| **Multi-tenancy** | Single cluster | `instance_id` + `cloud_unique_id` for isolation |
| **Data lifecycle** | Manual cleanup | Recycler service for automated GC |
| **Entry point** | be/src/main.cpp | cloud/src/main.cpp (meta-service + recycler) |

### Cloud Commands
```bash
./build.sh --cloud                 # Build cloud component
cloud/script/start.sh              # Start meta-service + recycler
cloud/script/stop.sh               # Stop services
```

### Cloud Anti-Patterns
- **DON'T** use standalone BE configs (`be.conf`) with cloud mode
- **DON'T** bypass `cloud_unique_id` validation in meta-service requests
- **DON'T** assume local storage paths - use StorageVaultAccessor abstraction
- **DON'T** confuse meta-service with FE metadata (BDB JE vs TxnKV)
- **DON'T** edit generated protobuf files in `gensrc/`

## Anti-Patterns (FORBIDDEN)

- **C++ exceptions for control flow**: Use `Status` objects instead
- **Row-based iteration in hot paths**: Always use vectorized execution (`be/src/vec/`)
- **Bypassing checkstyle/clang-format**: Tools are strictly enforced
- **Committing to gensrc/**: Generated code only - never edit manually
- **snake_case for C++ functions**: Use `PascalCase` (Doris-specific variant)
- **Large monolithic files**: Aim for <3000 lines when possible
- **Deep nesting**: Prefer depth <10; refactor complex logic into separate methods

## Code Characteristics

**Large files (>3000 lines)**:
- `fe/fe-core/src/main/java/org/apache/doris/catalog/Env.java`
- `fe/fe-core/src/main/java/org/apache/doris/nereids/LogicalPlanBuilder.java`
- `fe/fe-core/src/main/java/org/apache/doris/common/SessionVariable.java`

**Cloud mode check**: `config::is_cloud_mode()` in C++, `Config.is_cloud_mode` in Java

**Generated code**: `gensrc/` contains Thrift/Protobuf generated files, regenerated during build
