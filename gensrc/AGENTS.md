# GENSRC (PROTOCOL DEFINITIONS)

Thrift and Protobuf definitions for inter-component communication.

## STRUCTURE

```
gensrc/
├── thrift/             # Thrift IDL files (FE-BE communication)
│   ├── FrontendService.thrift   # FE APIs (query, metadata)
│   ├── BackendService.thrift    # BE data operations
│   ├── HeartbeatService.thrift  # Health monitoring
│   ├── PlanNodes.thrift         # Query plan structures
│   ├── Types.thrift             # Common type definitions
│   ├── Descriptors.thrift       # Table/column descriptors
│   └── ...                      # 27 total .thrift files
├── proto/              # Protobuf definitions (internal services)
│   ├── internal_service.proto   # PBackendService (BE internal)
│   ├── cloud.proto              # MetaService, RecyclerService
│   ├── function_service.proto   # UDF RPC
│   ├── olap_file.proto          # Storage file formats
│   └── ...                      # 12 total .proto files
├── script/             # Code generation scripts
├── build/              # Generated output
│   └── gen_cpp/        # Generated C++ headers/sources
└── Makefile            # Build coordination
```

## KEY SERVICES

| Service | Protocol | Purpose |
|---------|----------|---------|
| FrontendService | Thrift | FE query planning, metadata ops |
| BackendService | Thrift | BE data operations |
| HeartbeatService | Thrift | Node health monitoring |
| PBackendService | Protobuf | BE internal communication |
| MetaService | Protobuf | Cloud metadata management |
| PFunctionService | Protobuf | UDF/UDAF execution |

## CONVENTIONS

- Thrift for FE-BE RPC (Java client, C++ server)
- Protobuf for BE-internal and cloud services (C++ to C++)
- Generated code goes to:
  - C++: `gensrc/build/gen_cpp/`
  - Java: `fe/fe-common/target/generated-sources/`

## MODIFYING PROTOCOLS

1. Edit `.thrift` or `.proto` file
2. Run `make` in `gensrc/` to regenerate
3. Update both FE (Java) and BE (C++) code
4. Ensure backward compatibility for rolling upgrades

## ANTI-PATTERNS

- Don't remove or rename existing fields (breaks compatibility)
- Don't change field IDs in Thrift/Protobuf
- Always add new fields as optional with defaults
