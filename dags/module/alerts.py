"""Shared retry defaults + failure alerting for every DAG.

Centralizes what used to be copy-pasted `default_args` blocks so retries/alerting are
uniform across the bronze factory, the dbt DAGs, and the standalone PDF/API DAGs.

`notify_failure` is the single on_failure_callback: it always emits one structured ERROR
log line (so the log-based monitoring stack can alert on `event=task_failed`), and if the
optional `ALERT_SLACK_WEBHOOK` Airflow Variable is set, also posts to Slack. Absent the
webhook it degrades to logging only -- no hard dependency on external infra.

Per-DAG `start_date` is supplied by `default_args(...)` since it can legitimately differ.
"""
import json
import logging
import urllib.request
from datetime import datetime, timedelta

from airflow.models import Variable

log = logging.getLogger("airflow.task")


def notify_failure(context) -> None:
    """on_failure_callback: structured log line + best-effort Slack. Never raises."""
    ti = context.get("task_instance")
    exc = context.get("exception")
    fields = {
        "event": "task_failed",
        "dag": getattr(ti, "dag_id", None),
        "task": getattr(ti, "task_id", None),
        "run_id": context.get("run_id"),
        "ds": context.get("ds"),
        "try": getattr(ti, "try_number", None),
        "exception": repr(exc) if exc is not None else None,
        "log_url": getattr(ti, "log_url", None),
    }
    log.error("TASK FAILED %s", json.dumps(fields))

    webhook = Variable.get("ALERT_SLACK_WEBHOOK", default_var=None)
    if not webhook:
        return
    try:
        text = (f":red_circle: *{fields['dag']}.{fields['task']}* failed "
                f"(run {fields['run_id']}, try {fields['try']})\n{fields['exception']}")
        payload = json.dumps({"text": text}).encode()
        req = urllib.request.Request(
            webhook, data=payload, headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=10)  # noqa: S310 (operator-configured webhook)
    except Exception as e:  # alerting must never fail the callback / mask the real error
        log.warning("Slack alert failed: %s", e)


def default_args(*, start_date: datetime = datetime(2024, 1, 1), retries: int = 2) -> dict:
    """Canonical default_args: bounded retries with exponential backoff, and the shared
    failure callback on every task. Sources needing a different cadence override `retries`."""
    return {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": start_date,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=20),
        "on_failure_callback": notify_failure,
    }
