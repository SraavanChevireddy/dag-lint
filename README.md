# dag-lint

> `flake8` for Apache Airflow. A fast static analyzer that catches DAG anti-patterns — top-level I/O, missing retries, worker-hogging sensors, accidental backfills — **before** they hit production. No running Airflow required.

[![CI](https://github.com/SraavanChevireddy/dag-lint/actions/workflows/ci.yml/badge.svg)](https://github.com/SraavanChevireddy/dag-lint/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)

Airflow lets you write DAGs that *look* fine and pass review, then quietly melt your scheduler or flood prod with backfill runs on deploy. These mistakes are invisible in a code diff but obvious to a linter. **dag-lint** parses your DAG files with Python's AST (it never imports or executes them, so it's fast and safe) and flags the classics.

## What it catches

| Code | Severity | Problem |
|------|----------|---------|
| **DL001** | warning | DAG with no explicit `catchup` — defaults to `True` and can launch a flood of backfills on deploy. |
| **DL002** | warning | No `retries` configured — one transient blip fails the whole run. |
| **DL003** | error | I/O (HTTP, DB, file reads) at module top level — runs on *every* scheduler parse, not at task runtime. |
| **DL004** | warning | A `Sensor` in poke mode (default) — holds a worker slot the entire time it waits, risking pool deadlock. |
| **DL005** | warning | A `Sensor` with no `timeout` — can hang forever if the upstream never arrives. |

## Install

```bash
pip install dag-lint            # from PyPI (once published)
# or from source:
git clone https://github.com/SraavanChevireddy/dag-lint.git
cd dag-lint && pip install -e .
```

## Usage

```bash
# Lint a whole DAGs directory
dag-lint dags/

# Lint a single file
dag-lint my_dag.py

# Treat warnings as failures too (great for CI)
dag-lint dags/ --strict
```

### Example

```bash
$ dag-lint examples/bad_dag.py
examples/bad_dag.py:10:DL003 [error] Top-level call to `get(...)` runs on every DAG parse. Move I/O inside a task/operator.
examples/bad_dag.py:13:DL001 [warning] DAG has no explicit `catchup`; default is True and may trigger unexpected backfill runs.
examples/bad_dag.py:13:DL002 [warning] No `retries` found; transient failures will fail the run.
examples/bad_dag.py:16:DL004 [warning] `PythonSensor` uses poke mode (default) and holds a worker slot while waiting.
examples/bad_dag.py:16:DL005 [warning] `PythonSensor` has no `timeout`; it can hang indefinitely.

Linted 1 file(s): 1 error(s), 4 warning(s).
```

It exits non-zero when errors are found (or any finding under `--strict`), so it drops straight into CI.

## Use it in CI

```yaml
# .github/workflows/lint-dags.yml
- run: pip install dag-lint
- run: dag-lint dags/ --strict
```

## Why AST, not import?

Importing a DAG to inspect it runs all its top-level code — the very thing DL003 warns about. dag-lint reads the source and analyzes the syntax tree, so linting is milliseconds-fast and has zero side effects.

## Extending it

Each rule is a pure function `(ast_tree) -> list[Finding]` in [`daglint/rules.py`](daglint/rules.py). Add a function, append it to `ALL_RULES`, write a test. PRs for new rules (e.g. detecting `datetime.now()` in DAG logic, dynamic task generation in for-loops) very welcome.

## License

[MIT](LICENSE) © Sraavan Chevireddy
