# Doris Development Guide

This guide provides instructions for building, testing, and maintaining the Apache Doris codebase.

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
