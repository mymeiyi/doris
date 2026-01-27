# BE (BACKEND) MODULE

C++ storage and query execution engine. CMake-based build.

## STRUCTURE

```
be/
├── src/
│   ├── olap/               # OLAP storage engine
│   │   ├── tablet*.cpp     # Tablet management
│   │   ├── rowset/         # Rowset and segment storage
│   │   ├── compaction*.cpp # Compaction logic
│   │   └── delta_writer*   # Write path
│   ├── vec/                # Vectorized execution engine
│   │   ├── columns/        # Column implementations (vector, nullable, array)
│   │   ├── data_types/     # DataType definitions
│   │   ├── functions/      # Scalar and aggregate functions
│   │   ├── core/           # Block (batch of columns)
│   │   └── exec/           # Vectorized operators
│   ├── pipeline/           # Pipeline execution framework
│   │   ├── pipeline*.cpp   # Pipeline construction
│   │   └── exec/           # Pipeline operators
│   ├── exec/               # Query operators and scanners
│   ├── runtime/            # Runtime state, memory, descriptors
│   ├── service/            # Backend services (BRPC, HTTP)
│   ├── cloud/              # Cloud-mode integrations
│   ├── io/                 # I/O abstractions, file cache
│   ├── util/               # General utilities
│   └── common/             # Common types, status codes
├── test/                   # Unit tests (gtest)
└── CMakeLists.txt
```

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Add scalar function | `vec/functions/` (implement IFunction) |
| Add aggregate function | `vec/aggregate_functions/` |
| Add data type | `vec/data_types/` + `vec/columns/` |
| Modify storage | `olap/` (Tablet, Rowset, Segment) |
| Add scan operator | `vec/exec/` or `pipeline/exec/` |
| Add index type | `olap/rowset/segment_v2/` |
| Modify compaction | `olap/compaction*.cpp` |
| Add HTTP endpoint | `http/action/` |

## CONVENTIONS

- Entry point: `service/doris_main.cpp`
- Namespace: `doris::` with sub-namespaces (`vectorized::`, `cloud::`)
- Headers and sources co-located (no separate include/)
- Use `Status` for error handling (no exceptions in hot paths)
- Memory: Arena allocators, tracked allocations
- Tests: `*_test.cpp` files using gtest

## KEY ABSTRACTIONS

### Storage
- **Tablet**: Partition unit, contains multiple Rowsets
- **Rowset**: Immutable versioned data, contains Segments
- **Segment**: Physical columnar file with indexes
- **Memtable**: In-memory buffer for writes

### Execution
- **Block**: Batch of Columns (vectorized unit)
- **Column**: Columnar data (ColumnVector, ColumnNullable, ColumnArray)
- **IFunction**: Scalar function interface
- **IAggregateFunction**: Aggregate function interface

## ANTI-PATTERNS

- **DO NOT** call hdfsDisconnect() (causes filesystem errors)
- **DO NOT** use dcheck before glog initialization
- **DO NOT** remove index after rowset writer created
- **DO NOT** update LRU cache timestamp on release
- Avoid raw pointers for ownership (use unique_ptr/shared_ptr)
