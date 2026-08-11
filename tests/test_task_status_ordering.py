from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import Boolean, DateTime, Integer, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from app.services.task_status_ordering import task_status_order_by


class OrderingBase(DeclarativeBase):
    pass


class OrderingTask(OrderingBase):
    __tablename__ = "test_task_status_ordering"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


def test_task_status_ordering_groups_by_priority_then_newest() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    OrderingBase.metadata.create_all(engine)
    now = datetime(2026, 8, 11, 12, 0, 0)
    rows = [
        OrderingTask(id=1, status="success", enabled=True, created_at=now),
        OrderingTask(id=2, status="failed", enabled=True, created_at=now),
        OrderingTask(id=3, status="queued", enabled=True, created_at=now),
        OrderingTask(id=4, status="running", enabled=True, created_at=now),
        OrderingTask(id=5, status="completed", enabled=True, created_at=now + timedelta(minutes=1)),
        OrderingTask(id=6, status="partial", enabled=True, created_at=now + timedelta(minutes=1)),
        OrderingTask(id=7, status="idle", enabled=True, created_at=now + timedelta(minutes=1)),
        OrderingTask(id=8, status="running", enabled=True, created_at=now + timedelta(minutes=1)),
        OrderingTask(id=9, status="disabled", enabled=False, created_at=now + timedelta(minutes=2)),
    ]
    with Session(engine) as session:
        session.add_all(rows)
        session.commit()
        ordered_ids = session.scalars(
            select(OrderingTask).order_by(*task_status_order_by(OrderingTask))
        ).all()

    assert [row.id for row in ordered_ids] == [8, 4, 7, 3, 6, 2, 5, 1, 9]
    engine.dispose()


def test_task_status_ordering_supports_derived_failure_and_disabled_conditions() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    OrderingBase.metadata.create_all(engine)
    now = datetime(2026, 8, 11, 12, 0, 0)
    rows = [
        OrderingTask(id=1, status="success", enabled=True, failed_count=0, created_at=now),
        OrderingTask(id=2, status="success", enabled=True, failed_count=1, created_at=now),
        OrderingTask(id=3, status="idle", enabled=False, failed_count=0, created_at=now),
        OrderingTask(id=4, status="idle", enabled=True, failed_count=0, created_at=now),
    ]
    with Session(engine) as session:
        session.add_all(rows)
        session.commit()
        ordered_rows = session.scalars(
            select(OrderingTask).order_by(
                *task_status_order_by(
                    OrderingTask,
                    disabled_condition=OrderingTask.enabled.is_(False),
                    failure_condition=OrderingTask.failed_count > 0,
                )
            )
        ).all()

    assert [row.id for row in ordered_rows] == [4, 2, 1, 3]
    engine.dispose()
