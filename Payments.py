"""
payments.py — Mua thêm token qua VNPay. Đặt CÙNG THƯ MỤC với main.py.

Luồng:
  user chọn gói -> POST /api/payment/create -> chuyển sang VNPay trả tiền
  -> VNPay gọi IPN (server-to-server) -> xác minh chữ ký + số tiền -> TỰ CỘNG TOKEN.
Token CHỈ được cộng ở IPN. Trang "return" chỉ để hiển thị kết quả (user có thể tự gõ URL đó).
"""
import os
import hmac
import hashlib
import time
import secrets
import logging
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import httpx
import pytz
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse

import auth
import credits

log = logging.getLogger(__name__)
ICT = pytz.timezone("Asia/Ho_Chi_Minh")
router = APIRouter()

# ── CÁC GÓI TOKEN — SỬA GIÁ / SỐ LƯỢNG Ở ĐÂY (amount = VND) ──
PACKS = {
    "p10":  {"label": "Gói 10 token",  "credits": 10,  "amount": 20000},
    "p50":  {"label": "Gói 50 token",  "credits": 50,  "amount": 90000},
    "p120": {"label": "Gói 120 token", "credits": 120, "amount": 200000},
}
UNLIMITED_ROLES = ("premium", "admin")
TRIAL_CREDITS = 5  # số token gói dùng thử, mỗi user chỉ nhận 1 lần


def _cfg() -> dict:
    return {
        "tmn": os.getenv("VNPAY_TMN_CODE", ""),
        "secret": os.getenv("VNPAY_HASH_SECRET", ""),
        "pay_url": os.getenv("VNPAY_PAY_URL", "https://sandbox.vnpayment.vn/paymentv2/vpcpay.html"),
        "base": os.getenv("PUBLIC_BASE_URL", "").rstrip("/"),
    }


# ─────────────── CHỮ KÝ VNPAY (HMAC-SHA512) ───────────────

def _query_string(params: dict) -> str:
    """Sắp xếp theo tên tham số, mã hóa giá trị kiểu quote_plus — đúng như code mẫu Python của VNPay."""
    return "&".join(f"{k}={quote_plus(str(v))}" for k, v in sorted(params.items()))


def _sign(params: dict, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), _query_string(params).encode("utf-8"), hashlib.sha512).hexdigest()


def build_payment_url(params: dict, secret: str, pay_url: str) -> str:
    return f"{pay_url}?{_query_string(params)}&vnp_SecureHash={_sign(params, secret)}"


def verify_signature(params: dict, secret: str) -> bool:
    got = str(params.get("vnp_SecureHash", ""))
    data = {
        k: v for k, v in params.items()
        if k.startswith("vnp_") and k not in ("vnp_SecureHash", "vnp_SecureHashType")
    }
    if not got or not data:
        return False
    return hmac.compare_digest(_sign(data, secret).lower(), got.lower())


# ─────────────── SUPABASE (bảng public.credit_orders) ───────────────

def _sb():
    url = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    headers = {"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    return f"{url}/rest/v1/credit_orders", headers


async def _insert_order(row: dict) -> bool:
    url, h = _sb()
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(url, headers={**h, "Prefer": "return=minimal"}, json=row)
    if r.status_code >= 400:
        log.error(f"credit_orders insert lỗi {r.status_code}: {r.text[:200]}")
    return r.status_code < 400


async def _get_order(code: str):
    url, h = _sb()
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get(url, headers=h, params={"order_code": f"eq.{code}", "select": "*"})
    rows = r.json() if r.status_code < 400 else []
    return rows[0] if rows else None


async def _claim_order(code: str) -> bool:
    """pending -> processing, NGUYÊN TỬ. Chỉ đúng 1 lần gọi thành công => chống cộng token 2 lần."""
    url, h = _sb()
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.patch(
            url,
            headers={**h, "Prefer": "return=representation"},
            params={"order_code": f"eq.{code}", "status": "eq.pending"},
            json={"status": "processing"},
        )
    return r.status_code < 400 and bool(r.json())


async def _set_status(code: str, status: str, **extra) -> None:
    url, h = _sb()
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.patch(
            url, headers={**h, "Prefer": "return=minimal"},
            params={"order_code": f"eq.{code}"}, json={"status": status, **extra},
        )
    if r.status_code >= 400:
        log.error(f"credit_orders set_status({code},{status}) lỗi {r.status_code}: {r.text[:200]}")


def _sb_trial():
    url = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    headers = {"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    return f"{url}/rest/v1/trial_claims", headers


async def _claim_trial_row(user_id: str, credits_amount: int) -> bool:
    """
    Insert 1 dòng vào trial_claims(user_id PRIMARY KEY, ...).
    user_id là khóa chính => 2 request cùng lúc (double-click, tab kép) chỉ 1 cái insert được,
    cái còn lại nhận lỗi 409 (unique violation) => an toàn tuyệt đối trước khi cộng token.
    """
    url, h = _sb_trial()
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(url, headers={**h, "Prefer": "return=minimal"},
                          json={"user_id": user_id, "credits": credits_amount})
    if r.status_code == 409:
        return False
    if r.status_code >= 400:
        log.error(f"trial_claims insert lỗi {r.status_code}: {r.text[:200]}")
        return False
    return True


async def _delete_trial_row(user_id: str) -> None:
    """Gỡ claim nếu add_credit thất bại sau đó, để user thử lại được thay vì mất suất."""
    url, h = _sb_trial()
    async with httpx.AsyncClient(timeout=10) as c:
        await c.delete(url, headers=h, params={"user_id": f"eq.{user_id}"})
    ip = ""
    for h in ("cf-connecting-ip", "x-real-ip"):
        if request.headers.get(h):
            ip = request.headers[h].strip()
            break
    if not ip and request.headers.get("x-forwarded-for"):
        ip = request.headers["x-forwarded-for"].split(",")[0].strip()
    if not ip and request.client:
        ip = request.client.host
    # IP chỉ để VNPay ghi nhận; IPv6/unknown thì dùng IPv4 mặc định cho khỏi bị từ chối
    return ip if ip and ":" not in ip else "127.0.0.1"


# ─────────────── ROUTES ───────────────

@router.post("/api/payment/claim-trial")
async def claim_trial(user: dict = Depends(auth.require_auth)):
    profile = await credits.get_profile(user["id"])
    role = credits._role_name(profile) if profile else ""
    if role in UNLIMITED_ROLES:
        return JSONResponse(status_code=400, content={"error": "Tài khoản của bạn đã dùng không giới hạn, không cần gói dùng thử"})

    if not await _claim_trial_row(user["id"], TRIAL_CREDITS):
        return JSONResponse(status_code=409, content={"error": "Bạn đã nhận gói dùng thử rồi, mỗi tài khoản chỉ nhận 1 lần"})

    new_balance = await credits.add_credit(user["id"], TRIAL_CREDITS)
    if new_balance is None:
        await _delete_trial_row(user["id"])  # gỡ claim để user bấm lại được
        return JSONResponse(status_code=503, content={"error": "Có lỗi khi cộng token, thử lại sau"})

    log.info(f"Trial claim: +{TRIAL_CREDITS} token cho user {user['id']}")
    return {"credits_added": TRIAL_CREDITS, "new_balance": new_balance}


@router.get("/api/payment/packs")
async def list_packs():
    return [{"id": k, **v} for k, v in PACKS.items()]


@router.post("/api/payment/create")
async def create_payment(payload: dict, request: Request, user: dict = Depends(auth.require_auth)):
    cfg = _cfg()
    if not (cfg["tmn"] and cfg["secret"] and cfg["base"]):
        return JSONResponse(status_code=503, content={"error": "Thanh toán chưa được cấu hình trên server"})

    pack = PACKS.get(str(payload.get("pack_id") or ""))
    if not pack:
        return JSONResponse(status_code=400, content={"error": "Gói không hợp lệ"})

    profile = await credits.get_profile(user["id"])
    role = credits._role_name(profile) if profile else ""
    if role in UNLIMITED_ROLES:
        return JSONResponse(status_code=400, content={"error": "Tài khoản của bạn đã dùng không giới hạn, không cần mua token"})

    order_code = f"MH{int(time.time())}{secrets.token_hex(3).upper()}"
    ok = await _insert_order({
        "order_code": order_code,
        "user_id": user["id"],
        "email": user.get("email"),
        "pack_id": payload["pack_id"],
        "credits": pack["credits"],
        "amount_vnd": pack["amount"],
        "provider": "vnpay",
        "status": "pending",
    })
    if not ok:
        return JSONResponse(status_code=503, content={"error": "Không tạo được đơn hàng, thử lại sau"})

    now = datetime.now(ICT)
    params = {
        "vnp_Version": "2.1.0",
        "vnp_Command": "pay",
        "vnp_TmnCode": cfg["tmn"],
        "vnp_Amount": pack["amount"] * 100,          # VNPay yêu cầu nhân 100
        "vnp_CurrCode": "VND",
        "vnp_TxnRef": order_code,
        "vnp_OrderInfo": f"Mua {pack['credits']} token {order_code}",
        "vnp_OrderType": "other",
        "vnp_Locale": "vn",
        "vnp_ReturnUrl": f"{cfg['base']}/api/payment/vnpay-return",
        "vnp_IpAddr": _client_ip(request),
        "vnp_CreateDate": now.strftime("%Y%m%d%H%M%S"),
        "vnp_ExpireDate": (now + timedelta(minutes=15)).strftime("%Y%m%d%H%M%S"),
    }
    return {"payment_url": build_payment_url(params, cfg["secret"], cfg["pay_url"]), "order_code": order_code}


def _ipn(code: str, msg: str) -> JSONResponse:
    return JSONResponse({"RspCode": code, "Message": msg})


@router.get("/api/payment/vnpay-ipn")
async def vnpay_ipn(request: Request):
    """VNPay gọi endpoint này (server -> server). ĐÂY là nơi DUY NHẤT cộng token."""
    params = dict(request.query_params)
    secret = _cfg()["secret"]
    if not secret or not verify_signature(params, secret):
        return _ipn("97", "Invalid signature")

    code = params.get("vnp_TxnRef", "")
    order = await _get_order(code)
    if not order:
        return _ipn("01", "Order not found")

    try:
        amount_ok = int(params.get("vnp_Amount", "0")) == int(order["amount_vnd"]) * 100
    except (TypeError, ValueError):
        amount_ok = False
    if not amount_ok:
        return _ipn("04", "Invalid amount")

    if order["status"] == "paid":
        return _ipn("02", "Order already confirmed")

    paid_ok = params.get("vnp_ResponseCode") == "00" and params.get("vnp_TransactionStatus") == "00"
    if not paid_ok:
        if order["status"] == "pending":
            await _set_status(code, "failed", provider_txn_id=params.get("vnp_TransactionNo"))
        return _ipn("00", "Confirm Success")

    if not await _claim_order(code):
        return _ipn("02", "Order already confirmed")   # lần IPN khác đang/đã xử lý

    try:
        new_balance = await credits.add_credit(order["user_id"], int(order["credits"]))
    except Exception as e:
        log.error(f"add_credit lỗi cho đơn {code}: {e}")
        new_balance = None

    if new_balance is None:
        await _set_status(code, "pending")              # trả về pending để VNPay gọi lại IPN
        return _ipn("99", "Unknown error")

    await _set_status(
        code, "paid",
        provider_txn_id=params.get("vnp_TransactionNo"),
        paid_at=datetime.now(ICT).isoformat(),
    )
    log.info(f"Đã cộng {order['credits']} token cho user {order['user_id']} (đơn {code})")
    return _ipn("00", "Confirm Success")


@router.get("/api/payment/vnpay-return")
async def vnpay_return(request: Request):
    """Trình duyệt user được VNPay chuyển về đây. Chỉ hiển thị kết quả, KHÔNG cộng token."""
    params = dict(request.query_params)
    secret = _cfg()["secret"]
    if not secret or not verify_signature(params, secret):
        return RedirectResponse("/buytoken.html?status=invalid")
    ok = params.get("vnp_ResponseCode") == "00" and params.get("vnp_TransactionStatus") == "00"
    code = quote_plus(params.get("vnp_TxnRef", ""))
    return RedirectResponse(f"/buytoken.html?status={'success' if ok else 'failed'}&order={code}")


@router.get("/api/payment/order/{order_code}")
async def order_status(order_code: str, user: dict = Depends(auth.require_auth)):
    order = await _get_order(order_code)
    if not order or order.get("user_id") != user["id"]:
        return JSONResponse(status_code=404, content={"error": "Không tìm thấy đơn hàng"})
    return {"order_code": order["order_code"], "status": order["status"],
            "credits": order["credits"], "amount": order["amount_vnd"]}


@router.get("/buytoken.html")
def buy_page():
    return FileResponse("static/buytoken.html")
