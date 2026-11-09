"""Runtime configuration, read from environment variables.

DATABASE_URL and REDIS_URL are required: there is no sensible default for where
the services live, so a missing value stops the process at startup. Every other
setting is a design value with a default. Tests build a Settings object
directly instead of using environment variables.
"""

import os
from collections.abc import Callable
from dataclasses import dataclass, field, replace


@dataclass(frozen=True)
class Settings:
    database_url: str
    redis_url: str
    lease_seconds: float = 30
    pop_timeout_seconds: float = 2
    relay_interval_seconds: float = 1
    job_sleep_scale: float = 1.0
    webhook_failure_rate: float = 0.2
    # Delay before attempt 2, attempt 3, and so on. See the worker for how it is indexed.
    backoff_seconds: tuple[float, ...] = (30, 120)
    # Maximum run time per job type. Enforced by the worker in a later phase.
    job_timeouts: dict[str, float] = field(
        default_factory=lambda: {"email": 10, "webhook": 10, "report": 15}
    )
    log_level: str = "INFO"


def _parse_float_list(raw: str) -> tuple[float, ...]:
    """Parse '30,120' into (30.0, 120.0)."""
    return tuple(float(part) for part in raw.split(","))


def _parse_float_map(raw: str) -> dict[str, float]:
    """Parse 'email:10,webhook:10' into {'email': 10.0, 'webhook': 10.0}."""
    parsed = {}
    for pair in raw.split(","):
        key, seconds = pair.split(":")  # raises ValueError unless there is exactly one colon
        parsed[key.strip()] = float(seconds)
    return parsed


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"environment variable {name} is required but not set")
    return value


def _optional[T](name: str, default: T, parse: Callable[[str], T]) -> T:
    """Return the parsed env var, or the default when it is not set."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return parse(raw)
    except ValueError as error:
        raise ValueError(f"environment variable {name} has an invalid value: {raw!r}") from error


def load_settings() -> Settings:
    base = Settings(database_url=_required("DATABASE_URL"), redis_url=_required("REDIS_URL"))
    return replace(
        base,
        lease_seconds=_optional("LEASE_SECONDS", base.lease_seconds, float),
        pop_timeout_seconds=_optional("POP_TIMEOUT_SECONDS", base.pop_timeout_seconds, float),
        relay_interval_seconds=_optional(
            "RELAY_INTERVAL_SECONDS", base.relay_interval_seconds, float
        ),
        job_sleep_scale=_optional("JOB_SLEEP_SCALE", base.job_sleep_scale, float),
        webhook_failure_rate=_optional("WEBHOOK_FAILURE_RATE", base.webhook_failure_rate, float),
        backoff_seconds=_optional("BACKOFF_SECONDS", base.backoff_seconds, _parse_float_list),
        job_timeouts=_optional("JOB_TIMEOUTS", base.job_timeouts, _parse_float_map),
        log_level=_optional("LOG_LEVEL", base.log_level, str),
    )
