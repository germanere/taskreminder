"""
config/security.py — Cấu hình bảo mật, tách riêng khỏi main.py
=================================================================
Dùng trong main.py:

    from config.security import setup_cors, SecurityHeadersMiddleware, RateLimiter

    app = FastAPI(...)
    setup_cors(app)
    app.add_middleware(SecurityHeadersMiddleware)

    register_limiter = RateLimiter(max_requests=5, window_seconds=3600)

    @app.post("/api/credit/register", dependencies=[Depends(register_limiter)])
    async def register_credit_user():
        ...

Biến môi trường liên quan (tùy chọn):
    ALLOWED_ORIGINS   Danh sách domain được phép gọi API, cách nhau bởi dấu phẩy.
                      VD: "https://your-app.onrender.com,https://yourdomain.com"
                      Chưa set → mặc định cho phép tất cả (giữ hành vi cũ của app,
                      không phá vỡ gì) — nhưng NÊN set khi đã có domain chính thức
                      để tránh site khác gọi trộm API của bạn.
"""

import os
import time
import logging
from collections import defaultdict, deque

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CORS — giới hạn domain được phép gọi API
# ─────────────────────────────────────────────

def setup_cors(app: FastAPI) -> None:
    """
    Gắn CORS middleware. Đọc ALLOWED_ORIGINS từ env (phân tách dấu phẩy).
    Nếu chưa set, fallback "*" để không phá app đang chạy — nhưng sẽ log cảnh báo
    mỗi lần khởi động để nhắc bạn cấu hình sớm.
    """
    raw = os.getenv("ALLOWED_ORIGINS", "").strip()
    if raw:
        origins = [o.strip() for o in raw.split(",") if o.strip()]
        log.info(f"CORS: giới hạn origin cho {origins}")
    else:
        origins = ["*"]
        log.warning(
            "CORS: ALLOWED_ORIGINS chưa set trên biến môi trường — đang cho phép "
            "TẤT CẢ origin (*). Nên set ALLOWED_ORIGINS khi đã có domain chính thức "
            "(vd: static/index.html deploy ở đâu thì set domain đó)."
        )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )


# ─────────────────────────────────────────────
# SECURITY HEADERS — chặn 1 số kiểu tấn công phổ biến qua trình duyệt
# ─────────────────────────────────────────────

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Thêm header bảo mật cơ bản vào mọi response:
    - X-Content-Type-Options: ngăn trình duyệt tự đoán sai loại file (MIME sniffing)
    - X-Frame-Options: ngăn site khác nhúng app của bạn vào iframe (clickjacking)
    - Referrer-Policy: hạn chế rò rỉ URL đầy đủ khi user click link ra ngoài
    - Permissions-Policy: tắt các quyền trình duyệt không dùng đến (camera, mic, vị trí)
    """

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        return response


# ─────────────────────────────────────────────
# RATE LIMITER — chặn spam theo IP (in-memory, đơn giản)
# ─────────────────────────────────────────────
# Lưu ý: chỉ đúng khi app chạy 1 instance duy nhất (đúng với Render free tier hiện tại).
# Nếu sau này scale nhiều instance cùng lúc, bộ đếm này không còn chính xác — lúc đó
# cần chuyển sang lưu trạng thái dùng chung (VD: Redis) thay vì biến trong RAM.

class RateLimiter:
    """
    Dùng làm FastAPI dependency để giới hạn số request/IP trong 1 khoảng thời gian.

        register_limiter = RateLimiter(max_requests=5, window_seconds=3600)

        @app.post("/api/credit/register", dependencies=[Depends(register_limiter)])
        async def register_credit_user(): ...

    Vượt hạn mức → trả 429 Too Many Requests, kèm thời gian gợi ý thử lại.
    """

    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict[str, deque] = defaultdict(deque)

    def _client_ip(self, request: Request) -> str:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            return xff.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    async def __call__(self, request: Request):
        ip = self._client_ip(request)
        now = time.time()
        q = self._hits[ip]

        while q and now - q[0] > self.window_seconds:
            q.popleft()

        if len(q) >= self.max_requests:
            wait_min = max(1, self.window_seconds // 60)
            raise HTTPException(
                status_code=429,
                detail=f"Quá nhiều yêu cầu từ IP này. Thử lại sau khoảng {wait_min} phút.",
            )

        q.append(now)
