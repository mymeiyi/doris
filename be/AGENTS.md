# BE (Backend) Development Guide

## Overview
Backend (BE) - C++ storage engine, vectorized query execution, and pipeline processing. 3,500+ files, 95% C++ code. Highest complexity component.

## Architecture

```
FE (query planning) → BE (execution) → Storage
                         ↓
                 Vectorized + Pipeline
```

**Key Architecture:**
- **Vectorized Engine**: Column-oriented Block, IColumn, DataType for batch processing (5-10x faster)
- **Pipeline Model**: Multi-stage task execution, dependency tracking, adaptive scheduling

## Structure

```
be/src/
├── olap/              # Storage: tablet, segment, rowset, compaction
├── vec/               # Vectorized: columns, functions, Block, DataTypes
├── pipeline/          # Pipeline: tasks, schedulers, operators
├── runtime/           # Query runtime: memory, thread, result writers
├── exprs/             # Expression evaluation
├── service/           # doris_main.cpp, internal/HTTP services
├── agent/             # Heartbeat, task execution
├── http/              # HTTP endpoints with auth
├── io/                # IO layer: file, cache, compression
├── util/              # Utilities: bitmap, bloom filter, etc.
└── cloud/             # Cloud mode integration
```

## Build & Test

```bash
./build.sh --be                          # Build BE
cd be/build_Release && ctest             # Run tests
ctest -R <test_name_regex>              # Specific tests
be/src/service/doris_main                # Binary (after build)
```

## Conventions

**Style**: Google C++ + Doris variants
- **Classes**: `PascalCase` (e.g., `Tablet`, `Block`, `Pipeline`)
- **Functions**: `PascalCase` (Doris specific!) NOT `snake_case`
- **Variables**: `snake_case`
- **Members**: `_snake_case`
- **Formatter**: `clang-format` (100 char lines, 4-space indent)

**Error Handling**: `Status` objects, NO exceptions in hot paths
**Logging**: `LOG(INFO)`, `LOG(WARNING)`, `LOG(ERROR)`

## Key Components

| Component | Path | Key Files |
|-----------|------|-----------|
| Storage | `olap/` | tablet.cpp, segment_v2/, compaction.cpp |
| Vectorized | `vec/` | core/block.h, columns/column.h, data_types/ |
| Pipeline | `pipeline/` | pipeline.h, task_scheduler.h, exec/operator.h |
| Formats | `vec/exec/format/` | orc/, parquet/, csv/ readers |
| Runtime | `runtime/` | mem_tracker.h, exec_env.h, fragment_mgr.h |

## Anti-Patterns
- ❌ Use exceptions in performance-critical code
- ❌ Use `snake_case` for functions (use `PascalCase`)
- ❌ Bypass clang-format
- ❌ Commit without running ctests
