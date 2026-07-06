"""Cached loaders for the config-driven bronze ingestion (ported from boreas).

Config lives under dags/config/:
  - <source>_table_list.yaml         source manifest (tables + load windows)
  - validation/<source>_<table>_vld.yaml   per-table primary_key + schema + rules

Loads are @lru_cache'd so repeated DAG parses don't re-read/parse the files. The
returned dicts are treated as read-only by callers; do not mutate them.
"""
import os
from functools import lru_cache

import yaml

_CONFIG_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "config"))


@lru_cache(maxsize=None)
def load_table_list(source: str) -> dict:
    """Read config/<source>_table_list.yaml."""
    path = os.path.join(_CONFIG_DIR, f"{source}_table_list.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


@lru_cache(maxsize=None)
def load_validation(source: str, table: str) -> dict:
    """Read config/validation/<source>_<table>_vld.yaml."""
    path = os.path.join(_CONFIG_DIR, "validation", f"{source}_{table}_vld.yaml")
    with open(path) as f:
        return yaml.safe_load(f)
