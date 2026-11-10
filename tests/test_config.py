import pytest

from job_queue_service.config import load_settings


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://db/jobs")
    monkeypatch.setenv("REDIS_URL", "redis://cache/0")
    return monkeypatch


@pytest.mark.parametrize("name", ["DATABASE_URL", "REDIS_URL"])
def test_missing_required_variable_fails_and_names_it(env, name):
    env.delenv(name)
    with pytest.raises(RuntimeError, match=name):
        load_settings()


def test_defaults_apply_when_only_required_variables_are_set(env):
    settings = load_settings()
    assert settings.backoff_seconds == (30, 120)
    assert settings.job_timeouts == {"email": 10, "webhook": 10, "report": 15}
    assert settings.lease_seconds == 30


def test_lists_and_mappings_are_parsed(env):
    env.setenv("BACKOFF_SECONDS", "1,2.5, 3")
    env.setenv("JOB_TIMEOUTS", "email:5,report:20")
    env.setenv("LEASE_SECONDS", "0.5")
    env.setenv("HEARTBEAT_SECONDS", "0.1")
    settings = load_settings()
    assert settings.backoff_seconds == (1.0, 2.5, 3.0)
    assert settings.job_timeouts == {"email": 5.0, "report": 20.0}
    assert settings.lease_seconds == 0.5
    assert settings.heartbeat_seconds == 0.1


def test_lease_must_be_longer_than_the_heartbeat(env):
    env.setenv("LEASE_SECONDS", "10")
    env.setenv("HEARTBEAT_SECONDS", "10")
    with pytest.raises(ValueError, match="LEASE_SECONDS.*HEARTBEAT_SECONDS"):
        load_settings()


@pytest.mark.parametrize(
    "name, value",
    [
        ("LEASE_SECONDS", "soon"),
        ("BACKOFF_SECONDS", "30,x"),
        ("JOB_TIMEOUTS", "email=10"),
    ],
)
def test_invalid_value_fails_and_names_the_variable(env, name, value):
    env.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        load_settings()
