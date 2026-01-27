# CLOUD MODULE

Cloud-native components for compute-storage separation. C++, CMake-based.

## OVERVIEW

Enables Doris cloud mode where BE/FE query a centralized meta-service instead of local metadata. Supports elastic compute with shared storage (S3, HDFS, Azure).

## STRUCTURE

```
cloud/
├── src/
│   ├── main.cpp            # Entry point (meta-service + recycler)
│   ├── meta-service/       # Centralized metadata management
│   │   ├── meta_service.cpp    # Main service implementation
│   │   ├── doris_txn.cpp       # Transaction management (CRITICAL)
│   │   └── meta_service_*.cpp  # Various meta operations
│   ├── meta-store/         # Underlying KV storage
│   │   └── txn_kv.cpp      # Transactional key-value store
│   ├── recycler/           # Garbage collection
│   │   ├── recycler.cpp    # Main recycler logic
│   │   ├── s3_accessor.cpp # S3 storage cleanup
│   │   └── hdfs_accessor.cpp   # HDFS storage cleanup
│   ├── rate-limiter/       # Rate limiting for cloud APIs
│   ├── resource-manager/   # Cloud resource management
│   ├── snapshot/           # Snapshot management
│   └── common/             # Shared utilities
└── test/                   # Unit tests
```

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Add meta-service RPC | `meta-service/meta_service*.cpp` |
| Modify transaction logic | `meta-service/doris_txn.cpp` |
| Add storage backend | `recycler/*_accessor.cpp` |
| Modify KV store | `meta-store/txn_kv.cpp` |
| Add rate limit | `rate-limiter/` |

## CONVENTIONS

- Proto definitions: `gensrc/proto/cloud.proto`
- MetaService and RecyclerService defined in protobuf
- BE/FE use `cloud_meta_mgr` to communicate with meta-service
- Recycler runs as separate process or alongside meta-service

## ANTI-PATTERNS

- **DO NOT EVER TOUCH** transaction constants in `doris_txn.cpp` without extreme caution
- **DO NOT EDIT** main.cpp casually ("KNOW WHAT YOU ARE DOING!")
- Rate limits exist to prevent cloud storage throttling - respect them
