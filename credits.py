"""
credits.py — Quản lý credit + role user qua Supabase (Postgres REST API)
====================================================================
Cần 2 biến môi trường trên Render:
  SUPABASE_URL          VD: https://xxxxx.supabase.co
  SUPABASE_SERVICE_KEY  service_role key (Settings > API trong Supabase dashboard)
                         KHÔNG dùng anon key — service key mới bypass được RLS.

Tuỳ chọn:
  ADMIN_SECRET           Chuỗi bí mật để gọi endpoint nạp credit / đổi role thủ công.

Schema Supabase (roles + users với trigger set_default_role) — dùng đúng bản
bạn đã tạo: bảng `roles` (admin/free_tier/premium) và `users.role_id` FK tới `roles.id`.

Quy tắc áp dụng ở module này:
  - role = 'premium' hoặc 'admin'  → KHÔNG trừ credit (unlimited).
  - role = 'free_tier'             → trừ credit như bình thường, hết credit thì chặn.
"""

import os
import secrets
import logging
import httpx

log = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")

FREE_TRIAL_CREDITS = 5
UNLIMITED_ROLES = {"premium", "admin"}


def _headers() -> dict:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _configured() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY)


def _require_configured():
    if not _configured():
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_KEY chưa được cấu hình trên server")


def _role_name(user: dict) -> str:
    """
    user['roles'] là object lồng nhau do PostgREST embed trả về, dạng {"name": "free_tier"}.
    Trả về "free_tier" nếu vì lý do gì đó không lấy được (an toàn — mặc định giới hạn).
    """
    roles_obj = user.get("roles")
    if isinstance(roles_obj, dict):
        return roles_obj.get("name", "free_tier")
    return "free_tier"


async def create_user() -> dict:
    """Tạo user mới với 1 api_key random. Role mặc định do trigger DB tự gán = free_tier."""
    _require_configured()
    api_key = secrets.token_hex(20)  # 40 ký tự hex, đủ khó đoán
    payload = {"api_key": api_key, "credits": FREE_TRIAL_CREDITS}

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{SUPABASE_URL}/rest/v1/users", headers=_headers(), json=payload)
        r.raise_for_status()
        rows = r.json()
        return rows[0] if rows else payload


async def get_user_by_key(api_key: str) -> dict | None:
    """
    Lấy user + role (embed join qua FK role_id -> roles.id).
    Trả về dict có thêm field 'roles': {'name': 'free_tier'|'premium'|'admin'}.
    """
    _require_configured()
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/users",
            headers=_headers(),
            params={"api_key": f"eq.{api_key}", "select": "*,roles(name)"},
        )
        r.raise_for_status()
        rows = r.json()
        return rows[0] if rows else None


async def deduct_credit(api_key: str, amount: int = 1, max_retries: int = 3) -> tuple[bool, int, str]:
    """
    Trừ `amount` credit nếu user thuộc free_tier và đủ số dư.
    premium/admin: luôn cho qua, không đụng vào credits trong DB.
    Trả về (thành_công, số_dư_hiển_thị, role_name).
    """
    _require_configured()
    user = await get_user_by_key(api_key)
    if user is None:
        return False, 0, ""

    role = _role_name(user)
    if role in UNLIMITED_ROLES:
        return True, user["credits"], role

    for attempt in range(max_retries):
        user = await get_user_by_key(api_key) if attempt > 0 else user
        current = user["credits"]
        if current < amount:
            return False, current, role

        new_balance = current - amount
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.patch(
                f"{SUPABASE_URL}/rest/v1/users",
                headers=_headers(),
                params={"api_key": f"eq.{api_key}", "credits": f"eq.{current}"},
                json={"credits": new_balance},
            )
            r.raise_for_status()
            rows = r.json()

        if rows:  # PATCH khớp đúng 1 row nghĩa là không bị request khác chen ngang
            return True, new_balance, role

        log.warning(f"deduct_credit race detected cho {api_key[:8]}..., thử lại (attempt {attempt+1})")

    return False, current, role


async def add_credit(api_key: str, amount: int) -> int | None:
    """Cộng thêm credit (topup thủ công/admin). Trả về số dư mới, None nếu user không tồn tại."""
    _require_configured()
    user = await get_user_by_key(api_key)
    if user is None:
        return None

    new_balance = user["credits"] + amount
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.patch(
            f"{SUPABASE_URL}/rest/v1/users",
            headers=_headers(),
            params={"api_key": f"eq.{api_key}"},
            json={"credits": new_balance},
        )
        r.raise_for_status()
    return new_balance


async def set_role(api_key: str, role_name: str) -> dict | None:
    """
    Đổi role user (VD: nâng lên 'premium' sau khi thanh toán).
    role_name phải là 1 trong 'admin' | 'free_tier' | 'premium' (đúng bảng roles đã seed).
    Trả về user row mới (kèm role), None nếu api_key hoặc role_name không tồn tại.
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
            f"{SUPABASE_URL}/rest/v1/users",
            headers=_headers(),
            params={"api_key": f"eq.{api_key}", "select": "*,roles(name)"},
            json={"role_id": role_id},
        )
        r.raise_for_status()
        rows = r.json()
        return rows[0] if rows else None
