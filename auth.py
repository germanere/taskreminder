"""
auth.py — Đăng ký / đăng nhập qua Supabase Auth (email + mật khẩu, có xác minh email)
=======================================================================================
Supabase Auth tự lo toàn bộ: hash mật khẩu, gửi email xác minh, tạo access token (JWT).
Ở đây chỉ gọi REST API của Supabase Auth qua httpx — không tự viết logic mật khẩu.

Cần 2 biến môi trường:
  SUPABASE_URL          (đã có sẵn, dùng chung với credits.py)
  SUPABASE_ANON_KEY      Settings > API > anon / public key — KHÔNG phải service_role.
                          Đây là key được thiết kế để public-safe, chuyên dùng cho các
                          thao tác Auth phía client (signup, login).

Yêu cầu đã bật trên Supabase Dashboard trước khi dùng module này:
  Authentication > Providers > Email: bật
  Authentication > Settings > Confirm email: bật (bắt buộc xác minh email trước khi login)
"""

import os
import httpx
from fastapi import Header, HTTPException

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")


def _require_configured():
    if not (SUPABASE_URL and SUPABASE_ANON_KEY):
        raise RuntimeError("SUPABASE_URL / SUPABASE_ANON_KEY chưa được cấu hình trên server")


def _auth_headers() -> dict:
    return {
        "apikey": SUPABASE_ANON_KEY,
        "Content-Type": "application/json",
    }


async def sign_up(email: str, password: str) -> dict:
    """Đăng ký user mới. Supabase tự gửi email xác minh (đã bật Confirm email)."""
    _require_configured()
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"{SUPABASE_URL}/auth/v1/signup",
            headers=_auth_headers(),
            json={"email": email, "password": password},
        )
    data = r.json()
    if r.status_code >= 400:
        msg = data.get("msg") or data.get("error_description") or data.get("error") or "Đăng ký thất bại"
        raise ValueError(msg)
    return data


async def sign_in(email: str, password: str) -> dict:
    """
    Đăng nhập bằng email + mật khẩu.
    Nếu chưa xác minh email, Supabase trả lỗi 400 với message rõ ràng — bắn lên nguyên văn.
    """
    _require_configured()
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"{SUPABASE_URL}/auth/v1/token?grant_type=password",
            headers=_auth_headers(),
            json={"email": email, "password": password},
        )
    data = r.json()
    if r.status_code >= 400:
        msg = data.get("error_description") or data.get("msg") or "Email hoặc mật khẩu không đúng"
        raise ValueError(msg)
    return data  # gồm access_token, refresh_token, user {id, email, ...}


async def get_user_from_token(access_token: str) -> dict | None:
    """Xác thực access_token, trả về thông tin user Supabase Auth (id, email...)."""
    _require_configured()
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={**_auth_headers(), "Authorization": f"Bearer {access_token}"},
        )
    if r.status_code >= 400:
        return None
    return r.json()


async def require_auth(authorization: str = Header(None)) -> dict:
    """
    FastAPI dependency — gắn vào route cần đăng nhập:
        @app.get("/api/...")
        async def route(user: dict = Depends(require_auth)): ...
    Đọc token từ header "Authorization: Bearer <access_token>".
    Ném lỗi 401 nếu thiếu token hoặc token không hợp lệ/hết hạn.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Thiếu access token — vui lòng đăng nhập")

    token = authorization.removeprefix("Bearer ").strip()
    user = await get_user_from_token(token)
    if user is None:
        raise HTTPException(status_code=401, detail="Token không hợp lệ hoặc đã hết hạn — vui lòng đăng nhập lại")
    return user
