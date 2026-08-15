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


class _ScalarResultStub:
    def __init__(self, values: list[int]) -> None:
        self._values = list(values)

    def scalars(self):
        return iter(self._values)


class _BackfillConnection:
    def __init__(self, candidate_ids: list[int]) -> None:
        self._candidate_ids = list(candidate_ids)
        self.statements: list[tuple[str, object]] = []

    def execute(self, statement, params=None):
        self.statements.append((str(statement), params))
        if str(statement).lstrip().startswith("SELECT"):
            return _ScalarResultStub(self._candidate_ids)
        return None


def test_backfill_listed_dates_prefilters_candidates_without_json_scan() -> None:
    candidate_ids = list(range(1, 1201))
    connection = _BackfillConnection(candidate_ids)

    database._backfill_listed_product_dates(connection)

    select_statement, _params = connection.statements[0]
    assert select_statement.lstrip().startswith("SELECT")
    assert "review_status IN ('listed', 'listed_master')" in select_statement
    assert "JSON_VALID" not in select_statement
    assert "JSON_EXTRACT" not in select_statement

    update_statements = connection.statements[1:]
    assert len(update_statements) == 3
    for index, (statement, params) in enumerate(update_statements):
        assert "JSON_VALID" in statement
        assert "STR_TO_DATE" in statement
        assert params == {"ids": candidate_ids[index * 500 : (index + 1) * 500]}


def test_backfill_listed_dates_skips_update_without_candidates() -> None:
    connection = _BackfillConnection([])

    database._backfill_listed_product_dates(connection)

    assert len(connection.statements) == 1
    assert connection.statements[0][0].lstrip().startswith("SELECT")


def test_backfill_listed_dates_uses_single_chunk_up_to_limit() -> None:
    connection = _BackfillConnection([7, 8, 9])

    database._backfill_listed_product_dates(connection)

    assert len(connection.statements) == 2
    _statement, params = connection.statements[1]
    assert params == {"ids": [7, 8, 9]}
