from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.auth import router as auth_router
from app.api.crawler import router as crawler_router
from app.api.profile import router as profile_router
from app.api.users import router as users_router
from app.core.config import settings
from app.db.database import init_database

app = FastAPI(title=settings.app_name, version=settings.app_version)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5174", "http://127.0.0.1:5174"],
    allow_origin_regex=r"^http://(localhost|127\\.0\\.0\\.1):\\d+$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/api")
app.include_router(users_router, prefix="/api")
app.include_router(profile_router, prefix="/api")
app.include_router(crawler_router, prefix="/api")


@app.on_event("startup")
def startup() -> None:
    init_database()


@app.get("/")
def root() -> dict[str, str]:
    return {"name": settings.app_name, "version": settings.app_version}
