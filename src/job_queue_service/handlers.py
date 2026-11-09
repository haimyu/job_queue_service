"""Job handlers.

Each handler takes (payload, report_progress, settings) and returns a result dict.
The payload is already a validated pydantic model. The settings argument is an
addition to the (payload, report_progress) signature in CLAUDE.md: it carries
JOB_SLEEP_SCALE and WEBHOOK_FAILURE_RATE so tests can make the handlers fast and
deterministic.
"""

import random
import time
import uuid
from collections.abc import Callable

from pydantic import ValidationError

from job_queue_service.config import Settings
from job_queue_service.models import (
    PAYLOAD_MODELS,
    BatchPayload,
    EmailPayload,
    ReportPayload,
    WebhookPayload,
)


class RetryableJobError(Exception):
    """A transient failure (network, timeout). The job is retried with backoff."""


class PermanentJobError(Exception):
    """A failure that retrying cannot fix, such as an invalid payload. The job fails at once."""


def _work(low: float, high: float, settings: Settings) -> None:
    """Simulate work by sleeping a random time between low and high seconds, scaled for tests."""
    time.sleep(random.uniform(low, high) * settings.job_sleep_scale)


def run_email(
    payload: EmailPayload, report_progress: Callable[[int], None], settings: Settings
):
    _work(1, 3, settings)
    return {"message_id": str(uuid.uuid7())}


def run_webhook(
    payload: WebhookPayload, report_progress: Callable[[int], None], settings: Settings
):
    _work(1, 2, settings)
    if random.random() < settings.webhook_failure_rate:
        raise RetryableJobError("simulated webhook failure")
    return {"status_code": 200}


def run_report(
    payload: ReportPayload, report_progress: Callable[[int], None], settings: Settings
):
    _work(3, 5, settings)
    return {"file_url": f"https://files.example.com/reports/{uuid.uuid7()}.pdf"}


def run_batch(
    payload: BatchPayload, report_progress: Callable[[int], None], settings: Settings
):
    total = len(payload.items)
    for done in range(1, total + 1):
        _work(0.1, 0.3, settings)
        report_progress(done * 100 // total)
    return {"processed": total, "succeeded": total, "failed": 0}


HANDLERS = {
    "email": run_email,
    "webhook": run_webhook,
    "report": run_report,
    "batch": run_batch,
}


def run_handler(
    job_type: str,
    payload: dict,
    report_progress: Callable[[int], None],
    settings: Settings,
) -> dict:
    """Validate the stored payload, then run the handler for the job type."""
    try:
        validated = PAYLOAD_MODELS[job_type].model_validate(payload)
    except ValidationError as error:
        # A payload that was valid at submission can only be invalid here if the row was
        # written some other way. Retrying cannot fix it.
        raise PermanentJobError(f"invalid payload: {error}") from error
    return HANDLERS[job_type](validated, report_progress, settings)
