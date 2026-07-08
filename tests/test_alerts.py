"""Unit tests for the shared retry/alerting defaults (module.alerts)."""
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.airflow


def test_default_args_enables_retries_and_alerting():
    from module.alerts import default_args, notify_failure

    args = default_args()
    assert args["retries"] >= 1
    assert args["on_failure_callback"] is notify_failure
    assert args["retry_exponential_backoff"] is True
    # override path
    assert default_args(retries=0)["retries"] == 0


def test_notify_failure_never_raises_without_webhook(monkeypatch):
    import module.alerts as alerts

    # No Slack webhook configured -> callback must log and return, not raise.
    monkeypatch.setattr(alerts.Variable, "get", lambda *a, **k: None)
    ctx = {
        "task_instance": SimpleNamespace(
            dag_id="d", task_id="t", try_number=1, log_url="http://x"
        ),
        "run_id": "manual__1",
        "ds": "2024-01-01",
        "exception": ValueError("boom"),
    }
    alerts.notify_failure(ctx)  # should not raise


def test_notify_failure_swallows_slack_errors(monkeypatch):
    import module.alerts as alerts

    monkeypatch.setattr(alerts.Variable, "get", lambda *a, **k: "http://hook.invalid")

    def _boom(*a, **k):
        raise RuntimeError("network down")

    # Even if posting to Slack blows up, the callback must not propagate.
    monkeypatch.setattr(alerts.urllib.request, "urlopen", _boom)
    ctx = {
        "task_instance": SimpleNamespace(
            dag_id="d", task_id="t", try_number=1, log_url="http://x"
        ),
        "run_id": "manual__1",
        "ds": "2024-01-01",
        "exception": ValueError("boom"),
    }
    alerts.notify_failure(ctx)  # should not raise
