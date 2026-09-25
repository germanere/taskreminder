"""
credits.py — Quản lý credit + role user, gắn với Supabase Auth (bảng `profiles`)
================================================================================
Đổi từ bản trước: khoá bằng api_key tự sinh -> khoá bằng chính user id của
Supabase Auth. Xem config/migration_auth.sql cho phần schema.

Cần 2 biến môi trường:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY   (service_role key — bypass RLS, chỉ dùng ở backend)

Tuỳ chọn:
  ADMIN_SECRET            Dùng cho các route admin (topup, set-role) trong main.py.

Quy tắc:
  - role = 'premium' hoặc 'admin'  → KHÔNG trừ credit (unlimited).
  - role = 'free_tier'             → trừ credit như bình thường, hết credit thì chặn.
"""

import os
import logging
import httpx

log = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")

UNLIMITED_ROLES = {"premium", "admin"}


def _headers() -> dict:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _require_configured():
    if not (SUPABASE_URL and SUPABASE_KEY):
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_KEY chưa được cấu hình trên server")


def _role_name(profile: dict) -> str:
    """profile['roles'] là object lồng nhau do PostgREST embed, dạng {"name": "free_tier"}."""
    roles_obj = profile.get("roles")
    if isinstance(roles_obj, dict):
        return roles_obj.get("name", "free_tier")
    return "free_tier"


async def get_profile(user_id: str) -> dict | None:
    """
    Lấy profile (kèm role) theo user_id — đây CHÍNH LÀ id của user trong Supabase Auth,
    không phải id riêng của bảng profiles (chúng bằng nhau theo thiết kế 1-1).
    """
    _require_configured()
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/profiles",
            headers=_headers(),
            params={"id": f"eq.{user_id}", "select": "*,roles(name)"},
        )
        r.raise_for_status()
        rows = r.json()
        return rows[0] if rows else None


async def deduct_credit(user_id: str, amount: int = 1, max_retries: int = 3) -> tuple[bool, int, str]:
    """
    Trừ `amount` credit nếu user thuộc free_tier và đủ số dư.
    premium/admin: luôn cho qua, không đụng vào credits trong DB.
    Trả về (thành_công, số_dư_hiển_thị, role_name).
    """
    _require_configured()
    profile = await get_profile(user_id)
    if profile is None:
        return False, 0, ""

    role = _role_name(profile)
    if role in UNLIMITED_ROLES:
        return True, profile["credits"], role

    for attempt in range(max_retries):
        profile = await get_profile(user_id) if attempt > 0 else profile
        current = profile["credits"]
        if current < amount:
            return False, current, role

        new_balance = current - amount
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.patch(
                f"{SUPABASE_URL}/rest/v1/profiles",
                headers=_headers(),
                params={"id": f"eq.{user_id}", "credits": f"eq.{current}"},
                json={"credits": new_balance},
            )
            r.raise_for_status()
            rows = r.json()

        if rows:  # PATCH khớp đúng 1 row nghĩa là không bị request khác chen ngang
            return True, new_balance, role

        log.warning(f"deduct_credit race detected cho user {user_id[:8]}..., thử lại (attempt {attempt+1})")

    return False, current, role


async def add_credit(user_id: str, amount: int) -> int | None:
    """Cộng thêm credit (topup thủ công/admin). Trả về số dư mới, None nếu user không tồn tại."""
    _require_configured()
    profile = await get_profile(user_id)
    if profile is None:
        return None

    new_balance = profile["credits"] + amount
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.patch(
            f"{SUPABASE_URL}/rest/v1/profiles",
            headers=_headers(),
            params={"id": f"eq.{user_id}"},
            json={"credits": new_balance},
        )
        r.raise_for_status()
    return new_balance


async def set_role(user_id: str, role_name: str) -> dict | None:
    """
    Đổi role user (VD: nâng lên 'premium' sau khi thanh toán).
    role_name phải là 1 trong 'admin' | 'free_tier' | 'premium'.
    """
    _require_configured()
    async with httpx.AsyncClient(timeout=10) as client:
        r_role = await client.get(
            f"{SUPABASE_URL}/rest/v1/roles",
            headers=_headers(),
            params={"name": f"eq.{role_name}", "select": "id"},
        )
        r_role.raise_for_status()
        role_rows = r_role.json()
        if not role_rows:
            return None
        role_id = role_rows[0]["id"]

        r = await client.patch(
            f"{SUPABASE_URL}/rest/v1/profiles",
            headers=_headers(),
            params={"id": f"eq.{user_id}", "select": "*,roles(name)"},
            json={"role_id": role_id},
        )
        r.raise_for_status()
        rows = r.json()
        return rows[0] if rows else None
