"""Static-analysis rules for Airflow DAG files.

Each rule is a function that inspects a parsed AST module and yields Finding
objects. Rules are pure (AST in, findings out) so they're trivial to unit-test
without a running Airflow.

We analyze source with the `ast` module rather than importing the DAG, so
linting is fast and safe (no top-level user code is executed).
"""
from __future__ import annotations

import ast
from dataclasses import dataclass


@dataclass(frozen=True)
class Finding:
    code: str          # stable rule id, e.g. "DL001"
    message: str       # human-readable explanation
    line: int          # 1-based line number
    severity: str      # "error" | "warning"

    def __str__(self) -> str:
        return f"{self.line}:{self.code} [{self.severity}] {self.message}"


# Operators whose `op_args`/`callable` running at parse time would be a problem,
# and call names that indicate I/O happening at module import (top-level) time.
_IO_CALLS = {
    "get", "post", "put", "delete", "request",   # requests / http clients
    "read_sql", "read_csv", "read_parquet",       # pandas
    "connect", "execute", "query",                # db drivers
    "Variable",                                   # Airflow Variable.get at top level
}


def _is_dag_constructor(node: ast.AST) -> bool:
    """True for `DAG(...)` calls or `@dag` decorator usage."""
    if isinstance(node, ast.Call):
        func = node.func
        if isinstance(func, ast.Name) and func.id == "DAG":
            return True
        if isinstance(func, ast.Attribute) and func.attr == "DAG":
            return True
    return False


def _kwarg(call: ast.Call, name: str) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    return None


def _iter_dag_calls(tree: ast.AST):
    """Yield every DAG(...) call and @dag(...) decorator call in the module."""
    for node in ast.walk(tree):
        if _is_dag_constructor(node):
            yield node
        # @dag(...) decorator form
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and _decorator_name(dec.func) == "dag":
                    yield dec


def _decorator_name(func: ast.expr) -> str | None:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

def rule_missing_catchup(tree: ast.AST) -> list[Finding]:
    """DL001: DAG without an explicit `catchup` defaults to True, which can
    silently launch a flood of backfill runs on deploy."""
    findings = []
    for call in _iter_dag_calls(tree):
        if _kwarg(call, "catchup") is None:
            findings.append(Finding(
                "DL001",
                "DAG has no explicit `catchup`; default is True and may trigger "
                "unexpected backfill runs. Set catchup=False unless you want it.",
                call.lineno, "warning",
            ))
    return findings


def rule_missing_retries(tree: ast.AST) -> list[Finding]:
    """DL002: No `retries` configured (neither in default_args nor on the DAG)
    means a single transient blip fails the whole run."""
    findings = []
    has_retries = False
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg in ("retries", "default_args"):
            has_retries = True
        # also catch `"retries": n` inside a dict literal
        if isinstance(node, ast.Dict):
            for key in node.keys:
                if isinstance(key, ast.Constant) and key.value == "retries":
                    has_retries = True
    if not has_retries:
        for call in _iter_dag_calls(tree):
            findings.append(Finding(
                "DL002",
                "No `retries` found; transient failures will fail the run. "
                "Add retries via default_args.",
                call.lineno, "warning",
            ))
            break
    return findings


def rule_top_level_io(tree: ast.AST) -> list[Finding]:
    """DL003: I/O or heavy calls at module top level run on EVERY DAG parse
    (every few seconds across all schedulers), hammering external systems and
    slowing the scheduler. Such work belongs inside a task."""
    findings = []
    # Collect line ranges of function/class bodies; anything outside them is top-level.
    body_ranges = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            start = node.lineno
            end = max(
                (n.lineno for n in ast.walk(node) if hasattr(n, "lineno")),
                default=start,
            )
            body_ranges.append((start, end))

    def is_top_level(lineno: int) -> bool:
        return not any(start < lineno <= end for start, end in body_ranges)

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _decorator_name(node.func)
            if name in _IO_CALLS and is_top_level(node.lineno):
                findings.append(Finding(
                    "DL003",
                    f"Top-level call to `{name}(...)` runs on every DAG parse. "
                    "Move I/O inside a task/operator.",
                    node.lineno, "error",
                ))
    return findings


def rule_poke_sensor_without_reschedule(tree: ast.AST) -> list[Finding]:
    """DL004: A Sensor in poke mode (the default) occupies a worker slot the
    whole time it waits, which can deadlock the pool. Use mode='reschedule' or a
    deferrable sensor."""
    findings = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _decorator_name(node.func)
            if name and name.endswith("Sensor"):
                mode = _kwarg(node, "mode")
                ok = isinstance(mode, ast.Constant) and mode.value in ("reschedule",)
                deferrable = _kwarg(node, "deferrable")
                ok = ok or (isinstance(deferrable, ast.Constant) and deferrable.value is True)
                if not ok:
                    findings.append(Finding(
                        "DL004",
                        f"`{name}` uses poke mode (default) and holds a worker slot "
                        "while waiting. Set mode='reschedule' or use a deferrable sensor.",
                        node.lineno, "warning",
                    ))
    return findings


def rule_missing_timeout_on_sensor(tree: ast.AST) -> list[Finding]:
    """DL005: A Sensor without `timeout` can wait forever if its upstream never
    arrives, silently blocking the pipeline."""
    findings = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _decorator_name(node.func)
            if name and name.endswith("Sensor") and _kwarg(node, "timeout") is None:
                findings.append(Finding(
                    "DL005",
                    f"`{name}` has no `timeout`; it can hang indefinitely. "
                    "Set a timeout so a missing upstream fails the run.",
                    node.lineno, "warning",
                ))
    return findings


ALL_RULES = [
    rule_missing_catchup,
    rule_missing_retries,
    rule_top_level_io,
    rule_poke_sensor_without_reschedule,
    rule_missing_timeout_on_sensor,
]


def lint_source(source: str) -> list[Finding]:
    """Parse `source` and run every rule. Returns findings sorted by line."""
    tree = ast.parse(source)
    findings: list[Finding] = []
    for rule in ALL_RULES:
        findings.extend(rule(tree))
    return sorted(findings, key=lambda f: (f.line, f.code))
