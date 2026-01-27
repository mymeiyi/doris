# Regression Test Framework Guide

## Overview

End-to-end regression test framework written in Groovy (NOT standard unit tests). Contains 2000+ test files organized by priority levels: `p0` (critical), `p1` (important), `p2` (extended).

## Groovy DSL & Priority System

**Priority Levels:** `*_p0/` (blocking issues), `*_p1/` (high priority), `*_p2/` (extended)

**Test Naming:** `test_*.groovy` (standard), `ddl_*.groovy` (DDL), `insert_*.groovy` (data loading)

**DSL Methods:**
```groovy
suite("test_name") {
    def tableName = "my_table"              // Use def (prevents global pollution)
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """CREATE TABLE ${tableName} (...)"""
    qt_test1 "SELECT * FROM ${tableName}"   // Query test with result verification
    streamLoad { ... }                     // Load data
    sql """sync"""                         // Multi-FE stability
    sql """DROP TABLE ${tableName}"""      // Always cleanup
}
```

**Critical Rules:**
1. Use `def` before variables (prevents global state)
2. Use `sql` NOT `set global` for session variables
3. Add `sync` after streamLoad in multi-FE
4. Use fixed timestamps NOT `now()` for time tests
5. Always cleanup test data
6. One feature per file

## Commands

```bash
./run-regression-test.sh                    # Run all
./run-regression-test.sh --run -s <suite>   # Specific suite
./run-regression-test.sh --run -d <dir>     # Directory
./run-regression-test.sh --run -f <file>    # Specific file
```

## Where to Look

| Task | Path |
|------|------|
| Write new test | `suites/` |
| Framework code | `framework/src/main/groovy/` |
| Test data | `data/` |
| Full guide | `regression-test/README.md` |

## Anti-Patterns

- DON'T skip teardown
- DON'T use `set global` (affects others)
- DON'T use p0 for non-critical
- DON'T commit flaky tests
- DON'T create same table in multiple files
- DON'T mix concerns
