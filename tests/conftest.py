"""Test fixtures. Tests use a real Postgres (database jobs_test) and a real Redis (db 15)."""

import os
import socket
import subprocess
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

import psycopg
import pytest
import redis
from fastapi.testclient import TestClient
from psycopg.rows import dict_row

from job_queue_service.api import create_app
from job_queue_service.config import Settings
from job_queue_service.worker import Worker

ROOT = Path(__file__).resolve().parent.parent


def _credentials() -> tuple[str, str]:
    """Read the Postgres user and password from the environment, falling back to .env."""
    values = dict(os.environ)
    env_file = ROOT / ".env"
    if "POSTGRES_USER" not in values and env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                key, _, value = line.partition("=")
                values.setdefault(key.strip(), value.strip())
    return values["POSTGRES_USER"], values["POSTGRES_PASSWORD"]


@pytest.fixture(scope="session")
def settings() -> Settings:
    user, password = _credentials()
    return Settings(
        database_url=f"postgresql://{user}:{password}@localhost:5432/jobs_test",
        redis_url="redis://localhost:6379/15",
        lease_seconds=30,
        pop_timeout_seconds=1,
        job_sleep_scale=0.01,
    )


@pytest.fixture(scope="session", autouse=True)
def schema(settings: Settings) -> None:
    """Recreate the public schema from db/001_schema.sql once per test session."""
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")
        conn.execute((ROOT / "db" / "001_schema.sql").read_text())


@pytest.fixture
def conn(settings: Settings):
    """Autocommit connection, the same mode the worker uses. Tables are emptied before each test."""
    with psycopg.connect(
        settings.database_url, autocommit=True, row_factory=dict_row
    ) as connection:
        connection.execute("TRUNCATE job_logs, jobs")
        yield connection


@pytest.fixture
def r(settings: Settings):
    client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    client.flushdb()
    yield client
    client.close()


@pytest.fixture
def client(settings: Settings, conn, r):
    # Depending on conn and r guarantees the tables and Redis are empty before the app starts.
    with TestClient(create_app(replace(settings))) as test_client:
        yield test_client


@pytest.fixture
def wait_until():
    """Poll until predicate() is truthy, or fail after a timeout.

    Used to wait for other processes and threads. It never hides a race: if the condition is
    not reached within the deadline the test fails with a clear message.
    """

    def wait(predicate, timeout: float = 15.0, message: str = "condition not reached"):
        deadline = time.monotonic() + timeout
        while True:
            value = predicate()
            if value:
                return value
            if time.monotonic() >= deadline:
                raise AssertionError(f"{message} within {timeout:g} s")
            time.sleep(0.05)

    return wait


@pytest.fixture
def spawn_worker(settings, tmp_path):
    """Start real worker processes against the test database. Survivors are killed afterwards."""
    processes = []

    def spawn(**env_overrides: str) -> subprocess.Popen:
        env = {
            **os.environ,
            "DATABASE_URL": settings.database_url,
            "REDIS_URL": settings.redis_url,
            "POP_TIMEOUT_SECONDS": "1",
            "JOB_SLEEP_SCALE": "0.01",
            "LOG_LEVEL": "INFO",
            **env_overrides,
        }
        log_path = tmp_path / f"worker-{len(processes)}.log"
        process = subprocess.Popen(
            [sys.executable, "-m", "job_queue_service.worker"],
            env=env,
            stdout=open(log_path, "w"),  # noqa: SIM115
            stderr=subprocess.STDOUT,
        )
        process.log_path = log_path
        processes.append(process)
        return process

    yield spawn
    for process in processes:
        if process.poll() is None:
            process.kill()
        process.wait()


class FreezableProxy:
    """A TCP proxy in front of Postgres that can be frozen.

    While frozen, connections stay open but no bytes get through in either direction. To a
    client that is what a paused or hung Postgres looks like: no error and no answer.
    """

    def __init__(self, target_host: str, target_port: int) -> None:
        self._target = (target_host, target_port)
        self._server = socket.create_server(("127.0.0.1", 0))
        self.port = self._server.getsockname()[1]
        self.frozen = threading.Event()
        self._closed = threading.Event()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                client, _ = self._server.accept()
            except OSError:  # the listening socket was closed
                return
            upstream = socket.create_connection(self._target)
            for source, sink in ((client, upstream), (upstream, client)):
                threading.Thread(
                    target=self._forward, args=(source, sink), daemon=True
                ).start()

    def _forward(self, source: socket.socket, sink: socket.socket) -> None:
        try:
            while data := source.recv(65536):
                while self.frozen.is_set() and not self._closed.is_set():
                    time.sleep(0.02)  # hold the bytes back
                sink.sendall(data)
        except OSError:
            pass
        finally:
            source.close()
            sink.close()

    def close(self) -> None:
        self._closed.set()
        self._server.close()


@pytest.fixture
def freezable_proxy(settings: Settings):
    real = urlsplit(settings.database_url)
    proxy = FreezableProxy(real.hostname, real.port)
    credentials = real.netloc.rsplit("@", 1)[0]
    proxy.database_url = urlunsplit(
        real._replace(netloc=f"{credentials}@127.0.0.1:{proxy.port}")
    )
    yield proxy
    proxy.close()


def process_next(
    conn: psycopg.Connection, r: redis.Redis, settings: Settings, worker_id: str
) -> UUID | None:
    """Run one iteration on connections the caller supplies. Used by tests."""
    return Worker(settings, worker_id, conn, r).process_next()
