from __future__ import annotations

from contextlib import contextmanager

import pytest

from app.db import database


class _Connection:
    def __init__(self, table_names: set[str]) -> None:
        self.table_names = table_names

    def execute(self, _statement):
        return None


class _Engine:
    def __init__(self, table_sequences: list[set[str]]) -> None:
        self.table_sequences = table_sequences
        self.connect_count = 0

    @contextmanager
    def connect(self):
        index = min(self.connect_count, len(self.table_sequences) - 1)
        self.connect_count += 1
        yield _Connection(self.table_sequences[index])


def test_wait_for_database_ready_retries_until_required_table_exists(
    monkeypatch,
) -> None:
    fake_engine = _Engine([set(), {"lt_user_accounts"}])
    monkeypatch.setattr(database, "engine", fake_engine)
    monkeypatch.setattr(
        database,
        "_table_names",
        lambda connection: connection.table_names,
    )
    monkeypatch.setattr(database.time, "sleep", lambda _seconds: None)

    database.wait_for_database_ready(
        timeout_seconds=1,
        retry_interval_seconds=0.01,
    )

    assert fake_engine.connect_count == 2


def test_wait_for_database_ready_times_out_without_required_table(
    monkeypatch,
) -> None:
    fake_engine = _Engine([set()])
    monotonic_values = iter([0.0, 2.0])
    monkeypatch.setattr(database, "engine", fake_engine)
    monkeypatch.setattr(database, "_table_names", lambda _connection: set())
    monkeypatch.setattr(database.time, "monotonic", lambda: next(monotonic_values))

    with pytest.raises(RuntimeError, match="等待数据库就绪超时"):
        database.wait_for_database_ready(
            timeout_seconds=1,
            retry_interval_seconds=0.01,
        )
