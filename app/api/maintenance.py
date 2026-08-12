from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, model_validator

from app.core.auth import require_superadmin
from app.services import maintenance_service

router = APIRouter(prefix="/maintenance", tags=["maintenance"])


class MaintenanceSettingsPayload(BaseModel):
    enabled: bool = False
    title: str = Field(min_length=1, max_length=255)
    message: str = Field(min_length=1, max_length=5000)
    startsAt: datetime | None = None
    estimatedEndsAt: datetime | None = None

    @model_validator(mode="after")
    def validate_time_range(self):
        if (
            self.startsAt is not None
            and self.estimatedEndsAt is not None
            and self.estimatedEndsAt <= self.startsAt
        ):
            raise ValueError("预计维护完成时间必须晚于开始维护时间")
        return self


@router.get("/status")
def get_maintenance_status() -> dict:
    return {"maintenance": maintenance_service.get_maintenance_status()}


@router.get("/settings")
def get_maintenance_settings(_: dict = Depends(require_superadmin)) -> dict:
    return {"maintenance": maintenance_service.get_maintenance_status()}


@router.put("/settings")
def update_maintenance_settings(
    payload: MaintenanceSettingsPayload,
    user: dict = Depends(require_superadmin),
) -> dict:
    try:
        maintenance = maintenance_service.save_maintenance_settings(
            enabled=payload.enabled,
            title=payload.title,
            message=payload.message,
            starts_at=payload.startsAt,
            estimated_ends_at=payload.estimatedEndsAt,
            updated_by=user["username"],
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"maintenance": maintenance}
