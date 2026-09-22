"""
credits.py — Quản lý credit user qua Supabase (Postgres REST API)
====================================================================
Cần 2 biến môi trường trên Render:
  SUPABASE_URL         VD: https://xxxxx.supabase.co
  SUPABASE_SERVICE_KEY  service_role key (Settings > API trong Supabase dashboard)
                        KHÔNG dùng anon key — service key mới bypass được RLS
                        để server tự do đọc/ghi credits.

Tuỳ chọn:
  ADMIN_SECRET          Chuỗi bí mật để gọi endpoint nạp credit thủ công.

SQL cần chạy 1 lần trong Supabase SQL Editor để tạo bảng:
------------------------------------------------------------------
create table users (
    id uuid primary key default gen_random_uuid(),
    api_key text unique not null,
    credits integer not null default 5,
    created_at timestamptz not null default now()
);
create index on users (api_key);
------------------------------------------------------------------
(Mặc định user mới được tặng 5 credit dùng thử — chỉnh trong hàm create_user bên dưới nếu muốn số khác.)
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


def _headers() -> dict:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _configured() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY)


async def create_user() -> dict:
    """Tạo user mới với 1 api_key random + credit dùng thử. Trả về row user."""
    if not _configured():
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_KEY chưa được cấu hình trên server")

    api_key = secrets.token_hex(20)  # 40 ký tự hex, đủ khó đoán
    payload = {"api_key": api_key, "credits": FREE_TRIAL_CREDITS}

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{SUPABASE_URL}/rest/v1/users", headers=_headers(), json=payload)
        r.raise_for_status()
        rows = r.json()
        return rows[0] if rows else payload


async def get_user_by_key(api_key: str) -> dict | None:
    """Lấy thông tin user theo api_key. None nếu không tồn tại."""
    if not _configured():
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_KEY chưa được cấu hình trên server")

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/users",
            headers=_headers(),
            params={"api_key": f"eq.{api_key}", "select": "*"},
        )
        r.raise_for_status()
        rows = r.json()
        return rows[0] if rows else None


async def deduct_credit(api_key: str, amount: int = 1, max_retries: int = 3) -> tuple[bool, int]:
    """
    Trừ `amount` credit của user nếu đủ số dư.
    Dùng optimistic concurrency (đọc giá trị hiện tại, ghi kèm điều kiện credits=eq.<giá_trị_cũ>)
    để giảm rủi ro 2 request cùng lúc trừ trùng credit.
    Trả về (thành_công, số_dư_còn_lại_sau_khi_trừ_hoặc_hiện_tại_nếu_thất_bại).
    """
    if not _configured():
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_KEY chưa được cấu hình trên server")

    for attempt in range(max_retries):
        user = await get_user_by_key(api_key)
        if user is None:
            return False, 0

        current = user["credits"]
        if current < amount:
            return False, current

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
            return True, new_balance

        log.warning(f"deduct_credit race detected cho {api_key[:8]}..., thử lại (attempt {attempt+1})")

    return False, current


async def add_credit(api_key: str, amount: int) -> int | None:
    """Cộng thêm credit (dùng cho topup thủ công/admin). Trả về số dư mới, None nếu user không tồn tại."""
    if not _configured():
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_KEY chưa được cấu hình trên server")

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
