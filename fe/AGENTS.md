# FE (FRONTEND) MODULE

Java-based query coordinator and metadata manager. Maven multi-module project.

## STRUCTURE

```
fe/
├── fe-core/            # Main FE logic
│   └── src/main/java/org/apache/doris/
│       ├── catalog/        # Metadata: Database, Table, Partition, Schema
│       ├── nereids/        # New query optimizer (Cascades-based)
│       │   ├── rules/      # Transformation rules
│       │   ├── trees/      # Plan and expression trees
│       │   ├── jobs/       # Optimization job scheduler
│       │   └── memo/       # Memoization for plan exploration
│       ├── planner/        # Legacy planner (deprecated)
│       ├── qe/             # Query execution: Coordinator, session
│       ├── analysis/       # SQL parsing and semantic analysis
│       ├── persist/        # Metadata persistence via EditLog
│       ├── datasource/     # External catalogs (Hive, Iceberg, JDBC)
│       ├── load/           # Data loading (Stream Load, Broker Load)
│       ├── mtmv/           # Materialized views
│       └── mysql/          # MySQL protocol handler
├── fe-common/          # Shared utilities, Config, I/O helpers
├── be-java-extensions/ # Java code running in BE
│   ├── java-udf/       # Java UDF support
│   ├── jdbc-scanner/   # JDBC data source scanner
│   └── hadoop-*/       # Hadoop ecosystem scanners
└── pom.xml             # Parent POM
```

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Add SQL function | `nereids/trees/expressions/functions/` |
| Add rewrite rule | `nereids/rules/rewrite/` |
| Add CBO exploration | `nereids/rules/exploration/` |
| Modify cost model | `nereids/cost/CostModel.java` |
| Add external catalog | `datasource/` (extend ExternalCatalog) |
| Add DDL command | `nereids/trees/plans/commands/` |
| Modify metadata persistence | `persist/` + `catalog/` |
| Add session variable | `qe/SessionVariable.java` + `common/Config.java` |

## CONVENTIONS

- Entry point: `DorisFE.java`
- Config: `fe-common/.../Config.java` (annotated fields)
- All metadata changes must go through EditLog for HA
- Use Nereids for new optimizer features (legacy planner frozen)
- Tests in `src/test/java/`, use JUnit 4/5

## ANTI-PATTERNS

- **DO NOT** wait on journal replay in MasterCatalogExecutor (deadlock risk)
- **DO NOT** modify partition items without compatibility guarantee
- **DO NOT** use deprecated OperationType enums for new features
- Avoid setting global session variables in code
