# REGRESSION TEST FRAMEWORK

Groovy-based integration and regression testing framework.

## STRUCTURE

```
regression-test/
├── suites/             # Test cases organized by category
│   ├── nereids_*/      # Optimizer tests
│   ├── query_p0/       # P0 priority query tests
│   ├── load_p0/        # Data loading tests
│   ├── partition_p0/   # Partitioning tests
│   └── ...             # Many more categories
├── data/               # Test data files (CSV, JSON, etc.)
├── framework/          # Test infrastructure
│   └── src/main/groovy/org/apache/doris/regression/
│       └── suite/Suite.groovy  # Core test class
├── conf/               # Configuration
│   └── regression-conf.groovy
├── pipeline/           # CI/CD scripts
└── plugins/            # Test plugins
```

## WRITING TESTS

```groovy
suite("test_name", "p0,nonConcurrent") {
    // Setup
    sql "CREATE TABLE IF NOT EXISTS t1 ..."
    sql "INSERT INTO t1 VALUES ..."
    
    // Test with result comparison
    qt_select "SELECT * FROM t1"  // Compares to data/test_name.out
    
    // Test with assertion
    def result = sql "SELECT COUNT(*) FROM t1"
    assertEquals(10, result[0][0])
    
    // Cleanup (optional - tables auto-cleaned)
    sql "DROP TABLE IF EXISTS t1"
}
```

## CONVENTIONS

- **Always use `def`** for local variables (avoid global state)
- **Avoid global session changes** (`set global ...`) - affects other tests
- **Use `nonConcurrent` tag** for tests that modify cluster state
- **Add `sql "sync"`** after stream load before querying
- **Use fixed timestamps** instead of `now()` for deterministic tests
- **Expected results** go in `data/<suite_name>/` as `.out` files

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Add query test | `suites/query_p0/` or appropriate category |
| Add load test | `suites/load_p0/` |
| Modify framework | `framework/src/main/groovy/` |
| Add test data | `data/<category>/` |
| Configure runs | `conf/regression-conf.groovy` |

## RUNNING TESTS

```bash
# Run all P0 tests
./run-regression-test.sh --run -times 1 -suiteParallel 10

# Run specific suite
./run-regression-test.sh --run -s test_name

# Run specific group
./run-regression-test.sh --run -g p0
```

## ANTI-PATTERNS

- Don't create same table name across different tests in same directory
- Don't use `now()` or dynamic values that change over time
- Don't forget `sync` after stream load operations
- Mark injection tests as `nonConcurrent` and cleanup after
