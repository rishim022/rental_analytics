"""Task lifecycle callbacks: structured logging + a pluggable notifier.

send_notification() is the single extension point for real alerting -- swap
the body for a Slack incoming-webhook POST or an smtplib email call once you
have credentials. Read them from an Airflow Connection/Variable at call time;
never hardcode a webhook URL or SMTP password here.
"""

from __future__ import annotations

import logging
from typing import Any

from .config import get_env_config

logger = logging.getLogger("rental_analytics.callbacks")


def send_notification(subject: str, message: str) -> None:
    config = get_env_config()
    if not config.notify_enabled:
        logger.info("[notify:%s:suppressed] %s | %s", config.env, subject, message)
        return
    # Stub: replace with e.g. requests.post(slack_webhook_url, json={...})
    logger.info("[notify:%s] %s | %s", config.env, subject, message)


def on_task_failure(context: dict[str, Any]) -> None:
    ti = context["task_instance"]
    send_notification(
        subject=f"FAILED: {ti.dag_id}.{ti.task_id}",
        message=(
            f"run_id={context['run_id']} try={ti.try_number} "
            f"exception={context.get('exception')!r}"
        ),
    )


def on_task_success(context: dict[str, Any]) -> None:
    ti = context["task_instance"]
    send_notification(
        subject=f"SUCCESS: {ti.dag_id}.{ti.task_id}",
        message=f"run_id={context['run_id']} duration={ti.duration}",
    )


def on_task_retry(context: dict[str, Any]) -> None:
    ti = context["task_instance"]
    logger.warning(
        "[retry] %s.%s try=%s/%s exception=%r",
        ti.dag_id,
        ti.task_id,
        ti.try_number,
        ti.max_tries + 1,
        context.get("exception"),
    )
