"""Static validation of the YAML ingestion config against the BronzeExtract engine
contract (spark/scala/src/main/scala/jobs/BronzeExtract.scala).

The Scala job THROWS on an unknown column type (`sparkTypeFor`) or an unknown rule
(`violatedColumn`), so a typo in a vld file only surfaces mid-Spark-run. These tests
mirror those two match statements to catch it in CI instead. Pure Python -- no Airflow.
Keep the type / rule sets below in sync if the engine gains new ones.
"""
import glob
import os
import re

import pytest
import yaml

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_CONFIG = os.path.join(_REPO, "dags", "config")
_VLD = os.path.join(_CONFIG, "validation")

# Mirrors sparkTypeFor: accepted scalar types (lower-cased), plus `timestamp*` and
# `numeric(p,s)`/`decimal(p,s)` handled separately.
_SCALAR_TYPES = {
    "string", "utf8", "int64", "int32", "float64", "float32",
    "bool", "boolean", "bytes", "json", "jsonb", "date", "time",
    "numeric", "decimal",
}
_DECIMAL_RE = re.compile(r"^(?:numeric|decimal)\(\d+,\d+\)$")

# Mirrors violatedColumn: rule id -> keys the rule requires.
_RULE_REQUIRED = {
    "not_null": ["columns"],
    "positive_number": ["columns"],
    "value_range": ["column", "min", "max"],
    "logical_check": ["expression"],
}


def _type_ok(t: str) -> bool:
    t = t.lower().strip()
    return t in _SCALAR_TYPES or t.startswith("timestamp") or bool(_DECIMAL_RE.match(t))


def _load(path):
    with open(path) as f:
        return yaml.safe_load(f)


def _vld_files():
    return sorted(glob.glob(os.path.join(_VLD, "*_vld.yaml")))


def _table_list_files():
    return sorted(glob.glob(os.path.join(_CONFIG, "*_table_list.yaml")))


@pytest.mark.parametrize("path", _vld_files(), ids=os.path.basename)
def test_vld_contract(path):
    cfg = _load(path)
    assert cfg.get("primary_key"), "missing primary_key"
    schema = cfg.get("schema")
    assert isinstance(schema, list) and schema, "schema must be a non-empty list"

    names = set()
    for col in schema:
        assert col.get("name"), f"schema entry missing name: {col}"
        assert col.get("type"), f"{col['name']} missing type"
        assert _type_ok(col["type"]), f"{col['name']}: unknown engine type {col['type']!r}"
        names.add(col["name"])

    assert cfg["primary_key"] in names, \
        f"primary_key {cfg['primary_key']!r} not among schema columns {sorted(names)}"

    for rule in cfg.get("validation_rules") or []:
        rtype = rule.get("rule")
        assert rtype in _RULE_REQUIRED, f"unknown rule type {rtype!r}"
        for key in _RULE_REQUIRED[rtype]:
            assert key in rule, f"rule {rule.get('id', rtype)!r} missing required key {key!r}"
        for c in rule.get("columns", []):
            assert c in names, f"rule {rule.get('id')!r} references unknown column {c!r}"
        if rtype == "value_range":
            assert rule["column"] in names, \
                f"value_range references unknown column {rule['column']!r}"


@pytest.mark.parametrize("path", _table_list_files(), ids=os.path.basename)
def test_table_list_has_matching_vld(path):
    source = os.path.basename(path).replace("_table_list.yaml", "")
    cfg = _load(path)
    tables = cfg.get("tables")
    assert isinstance(tables, list) and tables, "table_list must have a non-empty 'tables'"
    for t in tables:
        name = t.get("table_name")
        assert name, f"table entry missing table_name: {t}"
        vld = os.path.join(_VLD, f"{source}_{name}_vld.yaml")
        assert os.path.exists(vld), \
            f"table {source}.{name} has no validation file {os.path.relpath(vld, _REPO)}"
