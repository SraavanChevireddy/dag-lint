"""Command-line interface for dag-lint.

Usage:
    dag-lint dags/                 # lint every .py under dags/
    dag-lint my_dag.py             # lint a single file
    dag-lint dags/ --strict        # treat warnings as errors too
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .rules import lint_source


def _iter_py_files(paths: list[str]):
    for p in paths:
        path = Path(p)
        if path.is_dir():
            yield from sorted(path.rglob("*.py"))
        elif path.suffix == ".py":
            yield path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="dag-lint",
        description="Catch Apache Airflow DAG anti-patterns before they hit production.",
    )
    parser.add_argument("paths", nargs="+", help="DAG files or directories to lint.")
    parser.add_argument("--strict", action="store_true",
                        help="Exit non-zero on warnings too (not just errors).")
    args = parser.parse_args(argv)

    total_errors = 0
    total_warnings = 0
    files = list(_iter_py_files(args.paths))

    for file in files:
        try:
            findings = lint_source(file.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            print(f"{file}: skipped (syntax error: {exc.msg})")
            continue
        for f in findings:
            print(f"{file}:{f}")
            if f.severity == "error":
                total_errors += 1
            else:
                total_warnings += 1

    print(f"\nLinted {len(files)} file(s): "
          f"{total_errors} error(s), {total_warnings} warning(s).")

    if total_errors or (args.strict and total_warnings):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
