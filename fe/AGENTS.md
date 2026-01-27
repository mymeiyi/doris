# FE Development Guide

Frontend (FE) - Java query planner and metadata manager. **5995 Java files**, 95% Java.

## Architecture

FE handles three primary responsibilities: **MySQL protocol handling**, **query planning via Cascades optimizer**, and **metadata management** with BDB JE persistence. Master/Follower/Observer roles enable HA through metadata quorum sync.

## Key Directories

| Path | Purpose | Key Files |
|------|---------|-----------|
| `nereids/` | **Cascades optimizer** (RBO+CBO+HBO) | NereidsPlanner.java, CascadesContext, Memo, HyperGraph join ordering |
| `catalog/` | Metadata management | **Env.java** (7415 lines!), Database, Table, OlapTable |
| `qe/` | Query execution & session | **SessionVariable.java** (5944 lines), ConnectContext, Coordinator |
| `mysql/` | MySQL protocol & auth | **Auth.java**, AuthenticatorManager, DefaultAuthenticator, LDAP support |
| `load/` | Data import framework | StreamLoadHandler, BrokerLoadJob, RoutineLoadJob (Kafka) |
| `datasource/` | External catalogs (20+) | Hive, Iceberg, Hudi, JDBC, Paimon, MaxCompute, ES, ODBC |
| `service/` | RPC services | FrontendServiceImpl, MySQLServer, ArrowFlight service |
| `transaction/` | Distributed transactions | GlobalTransactionMgr, AbstractExternalTransactionManager |
| `persist/` | BDB JE persistence | 79 log classes for metadata replay |
| `planner/` | **Legacy planner** (deprecated) | Use nereids/ instead |

## Conventions

**Style**: Checkstyle enforces 120-char limit, Apache header, strict import ordering (`mvn checkstyle:check`)

**Naming**: Standard Java - camelCase methods/variables, PascalCase classes, UPPER_SNAKE_CASE constants

**Logging**: Log4j with LOG.info()/warn()/error() (not System.out)

**Testing**: JUnit 5 in `fe-core/src/test/java/` mirrors main package structure, mock with Mockito

## Anti-Patterns

❌ **Never add code to legacy `planner/`** - All query planning uses nereids/ Cascades optimizer

❌ **Never bypass checkstyle** - Configured in `fe/check/checkstyle/checkstyle.xml`

❌ **Never use exceptions for control flow** - Return Status/Result objects

❌ **Never edit generated code** - All thrift/proto in `gensrc/` is regenerated

## Commands

```bash
cd fe
mvn test                           # Run unit tests
mvn checkstyle:check                # Verify style compliance
mvn clean compile                   # Build FE
```

## What Makes FE Unique

Unlike typical Java web apps, FE is a **distributed database metadata engine** with:
- **Hybrid Cascades optimizer** combining RBO, CBO, and HBO (inspired by Spark parser + NoisePage optimizer)
- **BDB JE quorum-based metadata replication** for Master/Follower/Observer HA
- **MySQL protocol wire-level implementation** (not just JDBC driver compatibility)
- **20+ federated catalog integrations** (Hive/Iceberg/Hudi/JDBC/etc.) with transaction consistency
- **MPP query coordination** across BE nodes with runtime filter pushdown and adaptive execution

Nereids is the **largest single subsystem** at 2533 files - it's where 90% of query optimization changes belong.
