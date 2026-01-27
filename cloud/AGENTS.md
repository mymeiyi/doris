# Cloud Component Development Guide

Cloud mode provides compute-storage separation for cloud-native Doris deployments. 200+ files, distinct architecture from standalone BE.

## Architecture

```
Cloud Meta-Service ←→ Recycler (cleanup) ←→ Object Storage (S3/HDFS/Azure)
        ↓
   Compute Nodes (BE) ←→ Query Clients
```

**Key Components:**
- **Meta-Service**: Centralized metadata via TxnKV (FoundationDB or in-memory)
- **Recycler**: Automated data cleanup and storage vault management
- **Storage Vaults**: Abstract S3/HDFS/Azure backends for data storage

## Structure

```
cloud/src/
├── main.cpp              # Entry point (meta-service + recycler)
├── meta-service/         # MetaServiceImpl, txn_kv, storage vaults
├── recycler/             # Recycler service, vault accessors
├── meta-store/           # Transactional KV (FoundationDB)
├── resource-manager/     # Resource allocation, quotas
├── rate-limiter/         # API/bandwidth throttling
└── common/               # Cloud utilities
```

## Where to Look

| Task | Path | Key Files |
|------|------|-----------|
| Meta-service | meta-service/ | meta_service.h, txn_kv.h |
| Recycler | recycler/ | recycler.h, storage_vault_accessor |
| Storage | meta-service/ | S3Accessor, HdfsAccessor |
| TxnKV | meta-store/ | txn_kv.h, mem_txn_kv.h |
| Config | conf/ | doris_cloud.conf |

## Conventions

- **Namespace**: `doris::cloud`
- **Error handling**: `TxnErrorCode` for KV ops, `Status` for BE calls
- **Logging**: Glog with `cloud_unique_id` tagging
- **Config**: `cloud_unique_id` format `${version}:${instance_id}:${unique_id}`
- **Async**: Bthreads for concurrent operations

## Key Differences from Standalone

| Aspect | Standalone BE | Cloud Mode |
|--------|--------------|------------|
| Storage | Local disks | Object storage (S3/HDFS/Azure) |
| Metadata | BDB JE in FE | Centralized Meta-Service |
| Multi-tenancy | Single cluster | instance_id + cloud_unique_id |
| Data lifecycle | Manual cleanup | Recycler service |
| Entry point | be/src/main.cpp | cloud/src/main.cpp |

## Commands

```bash
./build.sh --cloud              # Build cloud component
cloud/script/start.sh           # Start services
cloud/script/stop.sh            # Stop services
# Check cloud mode: config::is_cloud_mode() in C++
```

## Anti-Patterns

❌ DON'T confuse standalone BE with cloud mode (different configs)
❌ DON'T bypass cloud_unique_id validation in requests
❌ DON'T use standalone BE configs (be.conf) with cloud mode
❌ DON'T confuse meta-service with FE metadata (TxnKV vs BDB JE)
