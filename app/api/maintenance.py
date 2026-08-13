from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel, Field, model_validator

from app.core.auth import require_authenticated_account, require_superadmin
from app.services import announcement_service, maintenance_service

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


class AnnouncementPayload(BaseModel):
    title: str = Field(min_length=1, max_length=255)
    content: str = Field(default="", max_length=20_000)
    imageUrls: list[str] = Field(default_factory=list, max_length=12)
    published: bool = False


class AnnouncementImageDeletePayload(BaseModel):
    imageUrl: str = Field(min_length=1, max_length=1000)


class AnnouncementReadPayload(BaseModel):
    announcementIds: list[int] = Field(default_factory=list, max_length=100)


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


@router.get("/announcements")
def list_published_announcements(
    user: dict = Depends(require_authenticated_account),
) -> dict:
    return {
        "announcements": announcement_service.list_announcements(
            include_unpublished=False,
            username=user["username"],
        )
    }


@router.get("/announcements/unread")
def get_unread_announcement_status(
    user: dict = Depends(require_authenticated_account),
) -> dict:
    return {
        "hasUnread": announcement_service.has_unread_announcements(
            user["username"]
        )
    }


@router.post("/announcements/read")
def mark_announcements_read(
    payload: AnnouncementReadPayload,
    user: dict = Depends(require_authenticated_account),
) -> dict:
    return {
        "readAnnouncementIds": announcement_service.mark_announcements_read(
            payload.announcementIds,
            username=user["username"],
        )
    }


@router.get("/announcements/manage")
def list_managed_announcements(
    _: dict = Depends(require_superadmin),
) -> dict:
    return {
        "announcements": announcement_service.list_announcements(
            include_unpublished=True,
        )
    }


@router.post("/announcements")
def create_announcement(
    payload: AnnouncementPayload,
    user: dict = Depends(require_superadmin),
) -> dict:
    try:
        announcement = announcement_service.create_announcement(
            title=payload.title,
            content=payload.content,
            image_urls=payload.imageUrls,
            published=payload.published,
            operated_by=user["username"],
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"announcement": announcement}


@router.put("/announcements/{announcement_id}")
def update_announcement(
    announcement_id: int,
    payload: AnnouncementPayload,
    user: dict = Depends(require_superadmin),
) -> dict:
    try:
        announcement = announcement_service.update_announcement(
            announcement_id,
            title=payload.title,
            content=payload.content,
            image_urls=payload.imageUrls,
            published=payload.published,
            operated_by=user["username"],
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"announcement": announcement}


@router.delete("/announcements/{announcement_id}")
def delete_announcement(
    announcement_id: int,
    _: dict = Depends(require_superadmin),
) -> dict:
    announcement_service.delete_announcement(announcement_id)
    return {"deleted": True}


@router.post("/announcement-images")
def upload_announcement_image(
    file: UploadFile = File(...),
    _: dict = Depends(require_superadmin),
) -> dict:
    try:
        image_url = announcement_service.save_announcement_image(file)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"imageUrl": image_url}


@router.api_route("/announcement-images", methods=["DELETE"])
def delete_announcement_image(
    payload: AnnouncementImageDeletePayload,
    _: dict = Depends(require_superadmin),
) -> dict:
    try:
        announcement_service.delete_unreferenced_announcement_image(
            payload.imageUrl
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"deleted": True}


@router.get("/task-control")
def get_task_control(_: dict = Depends(require_superadmin)) -> dict:
    return {"taskControl": maintenance_service.get_task_control_status()}


@router.post("/task-control/stop-all")
def stop_all_tasks(user: dict = Depends(require_superadmin)) -> dict:
    return {
        "taskControl": maintenance_service.stop_all_tasks(
            operated_by=user["username"],
        )
    }


@router.post("/task-control/resume-all")
def resume_all_tasks(user: dict = Depends(require_superadmin)) -> dict:
    return {
        "taskControl": maintenance_service.resume_all_tasks(
            operated_by=user["username"],
        )
    }
