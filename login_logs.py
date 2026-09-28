"""
login_logs.py — Ghi log IP + thiết bị (user-agent) mỗi lần đăng nhập thành công
=================================================================================
Dùng chung SUPABASE_URL / SUPABASE_SERVICE_KEY đã có (credits.py).
Cần chạy config/migration_login_logs.sql trước (tạo bảng login_logs).
"""

import os
import logging
import httpx
from fastapi import Request

log = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")


def _headers() -> dict:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }


def get_client_ip(request: Request) -> str:
    """Render (và hầu hết PaaS) đặt IP thật của client vào header này qua proxy."""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


async def log_login(profile_id: str, email: str, ip_address: str, user_agent: str) -> None:
    """
    Ghi 1 dòng vào login_logs. Cố tình KHÔNG raise lỗi ra ngoài —
    ghi log thất bại không được phép làm hỏng luồng đăng nhập chính của user.
    """
    if not (SUPABASE_URL and SUPABASE_KEY):
        log.warning("login_logs: SUPABASE chưa cấu hình — bỏ qua ghi log")
        return
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/login_logs",
                headers=_headers(),
                json={
                    "profile_id": profile_id,
                    "email": email,
                    "ip_address": ip_address,
                    "user_agent": (user_agent or "")[:500],
                },
            )
    except Exception as e:
        log.warning(f"login_logs: ghi thất bại (không ảnh hưởng đăng nhập): {e}")


async def get_recent_logs(limit: int = 50, profile_id: str | None = None) -> list[dict]:
    """Lấy log gần nhất, có thể lọc theo profile_id. Dùng cho route admin xem log."""
    if not (SUPABASE_URL and SUPABASE_KEY):
        return []
    params = {"select": "*", "order": "created_at.desc", "limit": str(limit)}
    if profile_id:
        params["profile_id"] = f"eq.{profile_id}"
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/login_logs", headers=_headers(), params=params)
        r.raise_for_status()
        return r.json()
