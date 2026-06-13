"""
Market Research Hub — Backend
==============================
- FastAPI server
- WebSocket proxy: Bybit → client (replaces geo-blocked Binance)
- REST APIs: Bybit klines, CoinGecko, Yahoo Finance, TCBS, SJC, Vietcombank
- Telegram alerts: BTC, ETH, USD/VND, Gold (SJC)
- Serve static files

CHANGES vs original:
  • ws_kline      → Bybit linear WebSocket (wss://stream.bybit.com)
  • ws_orderbook  → Bybit orderbook.50 WebSocket
  • /api/klines   → Bybit REST v5 with Binance as fallback
  • job_alert     → Bybit REST for BTC/ETH price, Binance as fallback
  • /api/gold     → sjc.com.vn/giavang/textContent.aspx → BTMC API fallback
                    (PriceService.ashx bị block từ Render US IP)
"""

import os, re, json, logging, asyncio, time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import httpx
import pytz
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import websockets
from apscheduler.schedulers.asyncio import AsyncIOScheduler

ICT = pytz.timezone("Asia/Ho_Chi_Minh")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG — ENV VARS
# ─────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

BTC_MIN    = float(os.getenv("BTC_MIN",    "55000"))
BTC_MAX    = float(os.getenv("BTC_MAX",    "75000"))
ETH_MIN    = float(os.getenv("ETH_MIN",    "1500"))
ETH_MAX    = float(os.getenv("ETH_MAX",    "2000"))
CHANGE_PCT = float(os.getenv("CHANGE_PCT", "5.0"))
USD_MIN    = float(os.getenv("USD_MIN",    "24000"))
USD_MAX    = float(os.getenv("USD_MAX",    "27000"))
GOLD_MIN   = float(os.getenv("GOLD_MIN",   "100000000"))
GOLD_MAX   = float(os.getenv("GOLD_MAX",   "150000000"))

# 50 mã HOSE vốn hóa lớn nhất (tháng 6/2025)
HOSE_TOP50 = [
    "VCB","BID","VIC","VHM","CTG","GAS","VNM","SAB","MSN","TCB",
    "MBB","FPT","ACB","PLX","HPG","VPB","STB","HDB","GVR","POW",
    "MWG","PNJ","REE","SSI","VND","HCM","DPM","DCM","VEA","KDH",
    "NVL","PDR","DXG","PVD","HSG","NKG","PHR","DRC","IDC","KBC",
    "NTC","LHG","EIB","EVF","CMG","VGI","FRT","DGW","GEX","VRE",
]

HOSE_INFO = {
    "VCB":  {"name": "Vietcombank",        "sector": "Ngân hàng"},
    "BID":  {"name": "BIDV",               "sector": "Ngân hàng"},
    "VIC":  {"name": "Vingroup",           "sector": "Bất động sản"},
    "VHM":  {"name": "Vinhomes",           "sector": "Bất động sản"},
    "CTG":  {"name": "VietinBank",         "sector": "Ngân hàng"},
    "GAS":  {"name": "PV Gas",             "sector": "Năng lượng"},
    "VNM":  {"name": "Vinamilk",           "sector": "Tiêu dùng"},
    "SAB":  {"name": "Sabeco",             "sector": "Tiêu dùng"},
    "MSN":  {"name": "Masan Group",        "sector": "Tiêu dùng"},
    "TCB":  {"name": "Techcombank",        "sector": "Ngân hàng"},
    "MBB":  {"name": "MB Bank",            "sector": "Ngân hàng"},
    "FPT":  {"name": "FPT Corp",           "sector": "Công nghệ"},
    "ACB":  {"name": "ACB",                "sector": "Ngân hàng"},
    "PLX":  {"name": "Petrolimex",         "sector": "Năng lượng"},
    "HPG":  {"name": "Hòa Phát Group",     "sector": "Vật liệu"},
    "VPB":  {"name": "VPBank",             "sector": "Ngân hàng"},
    "STB":  {"name": "Sacombank",          "sector": "Ngân hàng"},
    "HDB":  {"name": "HDBank",             "sector": "Ngân hàng"},
    "GVR":  {"name": "VRG",                "sector": "Công nghiệp"},
    "POW":  {"name": "PV Power",           "sector": "Năng lượng"},
    "MWG":  {"name": "Thế Giới Di Động",   "sector": "Tiêu dùng"},
    "PNJ":  {"name": "PNJ",                "sector": "Tiêu dùng"},
    "REE":  {"name": "Cơ Điện Lạnh REE",   "sector": "Công nghiệp"},
    "SSI":  {"name": "SSI Securities",     "sector": "Chứng khoán"},
    "VND":  {"name": "VNDirect",           "sector": "Chứng khoán"},
    "HCM":  {"name": "HSC",                "sector": "Chứng khoán"},
    "DPM":  {"name": "Đạm Phú Mỹ",        "sector": "Hóa chất"},
    "DCM":  {"name": "Đạm Cà Mau",        "sector": "Hóa chất"},
    "VEA":  {"name": "VEAM",               "sector": "Công nghiệp"},
    "KDH":  {"name": "Khang Điền",         "sector": "Bất động sản"},
    "NVL":  {"name": "Novaland",           "sector": "Bất động sản"},
    "PDR":  {"name": "Phát Đạt",           "sector": "Bất động sản"},
    "DXG":  {"name": "Đất Xanh Group",     "sector": "Bất động sản"},
    "PVD":  {"name": "PV Drilling",        "sector": "Năng lượng"},
    "HSG":  {"name": "Hoa Sen Group",      "sector": "Vật liệu"},
    "NKG":  {"name": "Nam Kim Steel",      "sector": "Vật liệu"},
    "PHR":  {"name": "Cao su Phước Hòa",   "sector": "Vật liệu"},
    "DRC":  {"name": "Cao su Đà Nẵng",     "sector": "Vật liệu"},
    "IDC":  {"name": "IDICO",              "sector": "Bất động sản"},
    "KBC":  {"name": "Kinh Bắc City",      "sector": "Bất động sản"},
    "NTC":  {"name": "Nam Tân Uyên",       "sector": "Bất động sản"},
    "LHG":  {"name": "Long Hậu",           "sector": "Bất động sản"},
    "EIB":  {"name": "Eximbank",           "sector": "Ngân hàng"},
    "EVF":  {"name": "EVNFinance",         "sector": "Ngân hàng"},
    "CMG":  {"name": "CMC Corp",           "sector": "Công nghệ"},
    "VGI":  {"name": "Viettel Global",     "sector": "Công nghệ"},
    "FRT":  {"name": "FPT Retail",         "sector": "Tiêu dùng"},
    "DGW":  {"name": "Digiworld",          "sector": "Tiêu dùng"},
    "GEX":  {"name": "Gelex Group",        "sector": "Công nghiệp"},
    "VRE":  {"name": "Vincom Retail",      "sector": "Bất động sản"},
}

_hose_cache: dict = {}
HOSE_TTL = 60

_multitf_cache: dict = {}
MULTITF_TTL = 300

# ─────────────────────────────────────────────
# ALERT STATE
# ─────────────────────────────────────────────

_prev: dict    = {}
_alerted: dict = {}   # key → timestamp of last alert (float)
ALERT_COOLDOWN = 3600  # giây — không spam cùng 1 alert trong 1 giờ

def _should_alert(key: str) -> bool:
    last = _alerted.get(key, 0)
    return (time.time() - last) >= ALERT_COOLDOWN

def _mark_alerted(key: str):
    _alerted[key] = time.time()

def _clear_alert(key: str):
    _alerted.pop(key, None)

# ─────────────────────────────────────────────
# TELEGRAM — async
# ─────────────────────────────────────────────

async def send_telegram_async(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set — skipping notification")
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            )
        log.info(f"Telegram sent: {text[:60]}...")
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ─────────────────────────────────────────────
# ALERT LOGIC
# ─────────────────────────────────────────────

def _fmt(value: float, unit: str) -> str:
    if unit.startswith("$"):
        return f"${value:,.2f}" if value < 1000 else f"${value:,.0f}"
    elif unit:
        return f"{value:,.0f} {unit}"
    return f"{value:,.0f}"


def check_alert(
    key: str, value: float,
    min_val: float, max_val: float,
    label: str, unit: str = "",
) -> list[str]:
    alerts = []
    now = datetime.now(ICT).strftime("%H:%M %d/%m")

    key_min = f"{key}_min"
    key_max = f"{key}_max"
    key_pct = f"{key}_pct"

    v_str   = _fmt(value,   unit)
    min_str = _fmt(min_val, unit)
    max_str = _fmt(max_val, unit)

    if min_val and value < min_val:
        if _should_alert(key_min):
            _mark_alerted(key_min)
            alerts.append(
                f"🔴 *{label} XUỐNG NGƯỠNG*\n"
                f"💰 {v_str} < {min_str}\n🕐 {now}"
            )
    else:
        _clear_alert(key_min)

    if max_val and value > max_val:
        if _should_alert(key_max):
            _mark_alerted(key_max)
            alerts.append(
                f"🟢 *{label} VƯỢT NGƯỠNG*\n"
                f"💰 {v_str} > {max_str}\n🕐 {now}"
            )
    else:
        _clear_alert(key_max)

    prev = _prev.get(key)
    if prev and prev > 0:
        pct = (value - prev) / prev * 100
        if abs(pct) >= CHANGE_PCT and _should_alert(key_pct):
            _mark_alerted(key_pct)
            icon = "📈" if pct > 0 else "📉"
            prev_str = _fmt(prev, unit)
            alerts.append(
                f"{icon} *{label} BIẾN ĐỘNG MẠNH*\n"
                f"{pct:+.2f}% | {prev_str} → {v_str}\n🕐 {now}"
            )
        elif abs(pct) < CHANGE_PCT * 0.5:
            _clear_alert(key_pct)

    _prev[key] = value
    return alerts

# ─────────────────────────────────────────────
# PRICE FETCH HELPERS (Bybit primary, Binance fallback)
# ─────────────────────────────────────────────

async def fetch_price(symbol_bybit: str, symbol_binance: str) -> float:
    for category in ("linear", "spot"):
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                r = await client.get(
                    "https://api.bybit.com/v5/market/tickers",
                    params={"category": category, "symbol": symbol_bybit},
                )
            data = r.json()
            lst  = data.get("result", {}).get("list", [])
            if lst:
                price = float(lst[0]["lastPrice"])
                if price > 0:
                    log.info(f"Bybit {category} price {symbol_bybit}: {price}")
                    return price
        except Exception as e:
            log.warning(f"Bybit {category} price error ({symbol_bybit}): {e}")

    # Binance fallback
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(
                f"https://api.binance.com/api/v3/ticker/price?symbol={symbol_binance}"
            )
        data = r.json()
        if not isinstance(data, dict):
            raise ValueError(f"Binance unexpected response type: {type(data)}")
        price = float(data.get("price", 0))
        if price > 0:
            log.info(f"Binance fallback price {symbol_binance}: {price}")
            return price
    except Exception as e:
        log.warning(f"Binance fallback price error ({symbol_binance}): {e}")

    raise ValueError(f"Không lấy được giá cho {symbol_bybit} / {symbol_binance}")

# ─────────────────────────────────────────────
# GOLD PRICE FETCH
# ─────────────────────────────────────────────

async def fetch_gold_price() -> float | None:
    """
    Giá vàng SJC (VND/lượng).
    Yahoo Finance XAU/USD → quy đổi VND/lượng
    1 troy oz = 31.1035g | 1 lượng VN = 37.5g → 1 lượng = 1.20565 troy oz
    """
    LUONG_PER_OZ = 37.5 / 31.1035

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            rg = await client.get(
                "https://query1.finance.yahoo.com/v8/finance/chart/GC%3DF",
                params={"interval": "1d", "range": "1d"},
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            )
            gold_usd_oz = rg.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]

            import xml.etree.ElementTree as ET
            rv = await client.get(
                "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx?b=10",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            root = ET.fromstring(rv.text)
            usd_vnd = 0.0
            for ex in root.findall(".//Exrate"):
                if ex.get("CurrencyCode") == "USD":
                    usd_vnd = float(ex.get("Sell", "0").replace(",", ""))
                    break

            if gold_usd_oz > 0 and usd_vnd > 0:
                price = gold_usd_oz * usd_vnd * LUONG_PER_OZ * 1.08
                price = round(price / 100_000) * 100_000
                log.info(f"Gold Yahoo: {gold_usd_oz} USD/oz × {usd_vnd} VND × {LUONG_PER_OZ:.5f} × 1.08 = {price:,.0f} VND/lượng")
                return price

    except Exception as e:
        log.error(f"fetch_gold_price Yahoo error: {e}")

    return None

# ─────────────────────────────────────────────
# SCHEDULER JOB
# ─────────────────────────────────────────────

async def job_alert():
    all_alerts: list[str] = []

    try:
        btc = await fetch_price("BTCUSDT", "BTCUSDT")
        all_alerts += check_alert("BTC", btc, BTC_MIN, BTC_MAX, "BTC/USDT", "$")
    except Exception as e:
        log.error(f"BTC alert error: {e}")

    try:
        eth = await fetch_price("ETHUSDT", "ETHUSDT")
        all_alerts += check_alert("ETH", eth, ETH_MIN, ETH_MAX, "ETH/USDT", "$")
    except Exception as e:
        log.error(f"ETH alert error: {e}")

    try:
        import xml.etree.ElementTree as ET
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx?b=10",
                headers={"User-Agent": "Mozilla/5.0"},
            )
        root = ET.fromstring(r.text)
        for ex in root.findall(".//Exrate"):
            if ex.get("CurrencyCode") == "USD":
                usd = float(ex.get("Sell", "0").replace(",", ""))
                if usd > 0:
                    all_alerts += check_alert("USD", usd, USD_MIN, USD_MAX, "USD/VND", "đ")
                else:
                    log.warning("Vietcombank USD sell = 0, bỏ qua")
                break
    except Exception as e:
        log.error(f"USD alert error: {e}")

    try:
        gold_price = await fetch_gold_price()
        if gold_price and gold_price > 0:
            all_alerts += check_alert("GOLD", gold_price, GOLD_MIN, GOLD_MAX, "Vàng SJC", "đ/lượng")
        else:
            log.warning("Gold: không lấy được giá từ tất cả endpoint")
    except Exception as e:
        log.error(f"Gold alert error: {e}")

    for alert in all_alerts:
        await send_telegram_async(alert)

    log.info(f"Alert check done — {len(all_alerts)} alert(s) sent")

# ─────────────────────────────────────────────
# LIFESPAN
# ─────────────────────────────────────────────

alert_scheduler = AsyncIOScheduler(timezone=ICT)
alert_scheduler.add_job(job_alert, "interval", minutes=5, id="alert_job")

@asynccontextmanager
async def lifespan(app: FastAPI):
    alert_scheduler.start()
    log.info("Alert scheduler started — checking every 5 minutes")
    yield
    alert_scheduler.shutdown(wait=False)
    log.info("Alert scheduler stopped")

# ─────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────

app = FastAPI(title="Market Research Hub", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def index():
    return FileResponse("static/index.html")

# ─────────────────────────────────────────────
# HELPERS — Bybit symbol mapping
# ─────────────────────────────────────────────

def to_bybit_symbol(symbol: str) -> str:
    return symbol.upper()

# ─────────────────────────────────────────────
# WEBSOCKET — BYBIT KLINE
# ─────────────────────────────────────────────

BYBIT_INTERVAL_MAP = {
    "1m": "1",   "3m": "3",   "5m": "5",   "15m": "15",  "30m": "30",
    "1h": "60",  "2h": "120", "4h": "240", "6h": "360",  "12h": "720",
    "1d": "D",   "1w": "W",   "1M": "M",
}

@app.websocket("/ws/kline")
async def ws_kline(ws: WebSocket, symbol: str = "btcusdt", interval: str = "1h"):
    await ws.accept()

    bybit_symbol   = to_bybit_symbol(symbol)
    bybit_interval = BYBIT_INTERVAL_MAP.get(interval, "60")
    bybit_url      = "wss://stream.bybit.com/v5/public/linear"
    subscribe_msg  = json.dumps({
        "op": "subscribe",
        "args": [f"kline.{bybit_interval}.{bybit_symbol}"],
    })

    RECONNECT_DELAY = 3
    MAX_RETRIES     = 10

    for attempt in range(MAX_RETRIES):
        try:
            async with websockets.connect(
                bybit_url,
                ping_interval=20,
                ping_timeout=10,
            ) as bybit_ws:
                await bybit_ws.send(subscribe_msg)
                log.info(f"Bybit kline connected: {bybit_symbol} {bybit_interval}")

                while True:
                    try:
                        msg = await asyncio.wait_for(bybit_ws.recv(), timeout=35)
                    except asyncio.TimeoutError:
                        await ws.send_json({"ping": True})
                        continue

                    data = json.loads(msg)

                    if "data" not in data:
                        continue

                    for k in data["data"]:
                        await ws.send_json({
                            "time":      int(k.get("start",  0)) // 1000,
                            "open":      float(k.get("open",   0)),
                            "high":      float(k.get("high",   0)),
                            "low":       float(k.get("low",    0)),
                            "close":     float(k.get("close",  0)),
                            "volume":    float(k.get("volume", 0)),
                            "is_closed": k.get("confirm", False),
                        })

        except WebSocketDisconnect:
            log.info("Client disconnected from kline WS")
            return
        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"Bybit kline closed (attempt {attempt+1}): {e} — retry in {RECONNECT_DELAY}s")
        except Exception as e:
            log.error(f"Bybit kline error (attempt {attempt+1}): {e}")

        try:
            await ws.send_json({"reconnecting": True, "attempt": attempt + 1})
        except Exception:
            return

        await asyncio.sleep(RECONNECT_DELAY)
        RECONNECT_DELAY = min(RECONNECT_DELAY * 2, 60)

    log.error(f"Bybit kline: max retries reached for {bybit_symbol}")

# ─────────────────────────────────────────────
# WEBSOCKET — BYBIT ORDERBOOK
# ─────────────────────────────────────────────

@app.websocket("/ws/orderbook")
async def ws_orderbook(ws: WebSocket, symbol: str = "btcusdt"):
    await ws.accept()

    bybit_symbol  = to_bybit_symbol(symbol)
    bybit_url     = "wss://stream.bybit.com/v5/public/linear"
    subscribe_msg = json.dumps({
        "op": "subscribe",
        "args": [f"orderbook.50.{bybit_symbol}"],
    })

    RECONNECT_DELAY = 3
    MAX_RETRIES     = 10

    local_bids: dict[str, float] = {}
    local_asks: dict[str, float] = {}

    def apply_delta(book: dict, entries: list):
        for price, qty in entries:
            if float(qty) == 0:
                book.pop(price, None)
            else:
                book[price] = float(qty)

    for attempt in range(MAX_RETRIES):
        try:
            local_bids.clear()
            local_asks.clear()

            async with websockets.connect(
                bybit_url,
                ping_interval=20,
                ping_timeout=10,
            ) as bybit_ws:
                await bybit_ws.send(subscribe_msg)
                log.info(f"Bybit orderbook connected: {bybit_symbol}")

                while True:
                    msg  = await bybit_ws.recv()
                    data = json.loads(msg)

                    if "data" not in data:
                        continue

                    book_data = data["data"]
                    msg_type  = data.get("type", "snapshot")

                    if msg_type == "snapshot":
                        local_bids = {p: float(q) for p, q in book_data.get("b", [])}
                        local_asks = {p: float(q) for p, q in book_data.get("a", [])}
                    else:
                        apply_delta(local_bids, book_data.get("b", []))
                        apply_delta(local_asks, book_data.get("a", []))

                    sorted_bids = sorted(local_bids.items(), key=lambda x: float(x[0]), reverse=True)[:10]
                    sorted_asks = sorted(local_asks.items(), key=lambda x: float(x[0]))[:10]

                    await ws.send_json({
                        "bids": [[float(p), q] for p, q in sorted_bids],
                        "asks": [[float(p), q] for p, q in sorted_asks],
                    })

        except WebSocketDisconnect:
            log.info("Client disconnected from orderbook WS")
            return
        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"Bybit orderbook closed (attempt {attempt+1}): {e}")
        except Exception as e:
            log.error(f"Bybit orderbook error (attempt {attempt+1}): {e}")

        try:
            await ws.send_json({"reconnecting": True, "attempt": attempt + 1})
        except Exception:
            return

        await asyncio.sleep(RECONNECT_DELAY)
        RECONNECT_DELAY = min(RECONNECT_DELAY * 2, 60)

    log.error(f"Bybit orderbook: max retries reached for {bybit_symbol}")

# ─────────────────────────────────────────────
# REST — HISTORICAL KLINES
# Primary: Bybit v5  →  Fallback: Binance
# FIX: Binance fallback nằm TRONG hàm get_klines (indent đúng)
#      + kiểm tra isinstance(data, list) trước khi iterate
# ─────────────────────────────────────────────

@app.get("/api/klines")
async def get_klines(symbol: str = "BTCUSDT", interval: str = "1h", limit: int = 200):
    bybit_symbol   = to_bybit_symbol(symbol)
    bybit_interval = BYBIT_INTERVAL_MAP.get(interval, "60")

    # Thử cả linear lẫn spot
    for category in ("linear", "spot"):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(
                    "https://api.bybit.com/v5/market/kline",
                    params={
                        "category": category,
                        "symbol":   bybit_symbol,
                        "interval": bybit_interval,
                        "limit":    limit,
                    },
                )
            result = r.json()
            if result.get("retCode") == 0:
                raw = result["result"]["list"]
                if isinstance(raw, list) and len(raw) > 0:
                    raw = raw[::-1]
                    log.info(f"Bybit kline OK ({category}): {bybit_symbol} {bybit_interval}")
                    return [{
                        "time":   int(k[0]) // 1000,
                        "open":   float(k[1]),
                        "high":   float(k[2]),
                        "low":    float(k[3]),
                        "close":  float(k[4]),
                        "volume": float(k[5]),
                    } for k in raw]
            log.warning(f"Bybit kline {category} retCode: {result.get('retMsg')}")
        except Exception as e:
            log.warning(f"Bybit kline {category} error: {e}")

    # Binance fallback — chỉ dùng nếu Bybit hoàn toàn fail, bỏ qua nếu 451
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://api.binance.com/api/v3/klines"
                f"?symbol={symbol.upper()}&interval={interval}&limit={limit}"
            )
        if r.status_code == 451:
            log.warning("Binance 451 geo-block — skip")
            return JSONResponse(status_code=503, content={"error": "Bybit unavailable, Binance geo-blocked"})

        data = r.json()
        if not isinstance(data, list):
            return JSONResponse(status_code=503, content={"error": "Kline fetch failed"})

        return [{
            "time":   int(k[0]) // 1000,
            "open":   float(k[1]),
            "high":   float(k[2]),
            "low":    float(k[3]),
            "close":  float(k[4]),
            "volume": float(k[5]),
        } for k in data]

    except Exception as e:
        log.error(f"Binance fallback error: {e}")
        return JSONResponse(status_code=503, content={"error": str(e)})

# ─────────────────────────────────────────────
# REST — CRYPTO PRICES (CoinGecko) — rate-limited cache
# ─────────────────────────────────────────────

_coingecko_cache: dict = {}
COINGECKO_TTL = 60
async def _fetch_yahoo_stock(client: httpx.AsyncClient, sym: str) -> dict:
    base = {"symbol": sym.replace(".VN", ""), "price": 0, "change": 0, "volume": 0}
    try:
        r = await client.get(
            f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}",
            params={"interval": "1d", "range": "2d"},
        )
        meta  = r.json()["chart"]["result"][0]["meta"]
        prev  = meta.get("previousClose") or meta.get("chartPreviousClose") or 1
        price = meta.get("regularMarketPrice", 0)
        base.update({
            "price":  price,
            "change": round((price - prev) / prev * 100, 2),
            "volume": meta.get("regularMarketVolume", 0),
        })
    except Exception as e:
        log.warning(f"Yahoo v8 error [{sym}]: {e}")
    return base
  

async def _coingecko_get(url: str, ttl: int = COINGECKO_TTL):
    cached = _coingecko_cache.get(url)
    if cached and (time.time() - cached[0]) < ttl:
        return cached[1]
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url)
        if r.status_code == 429:
            log.warning("CoinGecko 429 — returning cached data if available")
            return cached[1] if cached else []
        data = r.json()
    _coingecko_cache[url] = (time.time(), data)
    return data

@app.get("/api/crypto/prices")
async def get_crypto_prices(ids: str = "bitcoin,ethereum,solana,binancecoin,ripple"):
    url = (
        f"https://api.coingecko.com/api/v3/simple/price"
        f"?ids={ids}&vs_currencies=usd&include_24hr_change=true&include_market_cap=true"
    )
    return await _coingecko_get(url)

@app.get("/api/crypto/top200")
async def get_top200(page: int = 1):
    url = (
        f"https://api.coingecko.com/api/v3/coins/markets"
        f"?vs_currency=usd&order=market_cap_desc&per_page=100&page={page}"
        f"&sparkline=false&price_change_percentage=24h,7d"
    )
    return await _coingecko_get(url, ttl=300)

# ─────────────────────────────────────────────
# REST — FEAR & GREED
# ─────────────────────────────────────────────

@app.get("/api/fear-greed")
async def get_fear_greed():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get("https://api.alternative.me/fng/?limit=1")
        return r.json()

# ─────────────────────────────────────────────
# REST — TỶ GIÁ (Vietcombank)
# ─────────────────────────────────────────────

@app.get("/api/forex/vnd")
async def get_forex_vnd():
    import xml.etree.ElementTree as ET
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx?b=10",
                headers={"User-Agent": "Mozilla/5.0"},
            )
        root = ET.fromstring(r.text)
        rates = {}
        for ex in root.findall(".//Exrate"):
            code = ex.get("CurrencyCode", "")
            sell = ex.get("Sell", "0").replace(",", "")
            buy  = ex.get("Buy",  "0").replace(",", "")
            if code in ["USD", "EUR", "JPY", "CNY", "GBP"]:
                rates[code] = {
                    "sell": float(sell) if sell else 0,
                    "buy":  float(buy)  if buy  else 0,
                }
        return rates
    except Exception as e:
        log.error(f"Forex VND error: {e}")
        return {"error": str(e)}

# ─────────────────────────────────────────────
# REST — GIÁ VÀNG SJC
# ─────────────────────────────────────────────

@app.get("/api/gold")
async def get_gold():
    try:
        price = await fetch_gold_price()
        if price:
            return {"price": price, "unit": "VND/lượng", "source": "SJC/BTMC"}
        return JSONResponse(status_code=503, content={"error": "Không lấy được giá vàng"})
    except Exception as e:
        log.error(f"Gold endpoint error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

# ─────────────────────────────────────────────
# REST — VN STOCK (Yahoo Finance v8)
# ─────────────────────────────────────────────

YAHOO_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept":          "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://finance.yahoo.com/",
    "Origin":          "https://finance.yahoo.com",
}

@app.get("/api/vn/stocks")
async def get_vn_stocks(symbols: str = "VNM.VN,FPT.VN,VCB.VN,HPG.VN,MWG.VN,TCB.VN,VIC.VN,VHM.VN,BID.VN,CTG.VN,VPB.VN,MBB.VN,ACB.VN,STB.VN,HDB.VN,VIB.VN,SSI.VN,VND.VN,HCM.VN,MSN.VN,VRE.VN,PDR.VN,DXG.VN,NVL.VN,KDH.VN,GVR.VN,SAB.VN,GAS.VN,PLX.VN,POW.VN"):
    sym_list = [s.strip() for s in symbols.split(",")]

    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=10) as client:
        tasks = [
            client.get(
                f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}",
                params={"interval": "1d", "range": "2d"},
            )
            for sym in sym_list
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    results = []
    for sym, resp in zip(sym_list, responses):
        base = {
            "symbol": sym.replace(".VN", ""),
            "price": 0, "change": 0,
            "volume": 0, "pe": 0, "market_cap": 0,
        }
        try:
            if isinstance(resp, Exception):
                raise resp
            data = resp.json()
            meta = data["chart"]["result"][0]["meta"]
            prev  = meta.get("previousClose") or meta.get("chartPreviousClose") or 1
            price = meta.get("regularMarketPrice", 0)
            base.update({
                "price":  price,
                "change": round((price - prev) / prev * 100, 2),
                "volume": meta.get("regularMarketVolume", 0),
            })
        except Exception as e:
            log.warning(f"Yahoo v8 error [{sym}]: {e}")
        results.append(base)

    return results

# ─────────────────────────────────────────────
# REST — ECONOMIC CALENDAR
# ─────────────────────────────────────────────

@app.get("/api/calendar")
async def get_calendar():
    now = datetime.now(ICT)
    return [
        {"date": (now + timedelta(days=1)).strftime("%d/%m/%Y"), "time": "19:30", "event": "US CPI MoM",        "impact": "high",   "prev": "0.3%",  "forecast": "0.2%"},
        {"date": (now + timedelta(days=2)).strftime("%d/%m/%Y"), "time": "02:00", "event": "FED Rate Decision",  "impact": "high",   "prev": "5.50%", "forecast": "5.50%"},
        {"date": (now + timedelta(days=3)).strftime("%d/%m/%Y"), "time": "08:00", "event": "BTC Options Expiry", "impact": "medium", "prev": "$1.8B", "forecast": "$2.1B"},
        {"date": (now + timedelta(days=5)).strftime("%d/%m/%Y"), "time": "21:30", "event": "US NFP",             "impact": "high",   "prev": "175K",  "forecast": "180K"},
    ]

# ─────────────────────────────────────────────
# REST — LIQUIDATION DATA (Coinglass)
# ─────────────────────────────────────────────

@app.get("/api/liquidations")
async def get_liquidations(symbol: str = "BTC"):
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://open-api.coinglass.com/public/v2/liquidation_history?symbol={symbol}&timeType=0",
                headers={"coinglassSecret": ""},
            )
        return r.json()
    except Exception:
        return {"data": []}

# ─────────────────────────────────────────────
# REST — HEALTH & ALERT MANAGEMENT
# ─────────────────────────────────────────────

@app.get("/health")
def health():
    return {
        "status":     "ok",
        "time":       datetime.now(ICT).isoformat(),
        "scheduler":  alert_scheduler.running,
        "next_alert": str(alert_scheduler.get_job("alert_job").next_run_time),
    }

@app.get("/api/alert/test")
async def test_alert():
    msg = (
        "✅ *Market Hub Alert Test*\n"
        "Bot đang hoạt động bình thường!\n"
        "🕐 " + datetime.now(ICT).strftime("%H:%M %d/%m/%Y")
    )
    await send_telegram_async(msg)
    return {"status": "sent", "message": msg}

@app.get("/api/alert/config")
def alert_config():
    return {
        "BTC":                  {"min": BTC_MIN,  "max": BTC_MAX},
        "ETH":                  {"min": ETH_MIN,  "max": ETH_MAX},
        "USD_VND":              {"min": USD_MIN,  "max": USD_MAX},
        "GOLD_SJC":             {"min": GOLD_MIN, "max": GOLD_MAX},
        "change_pct_threshold": CHANGE_PCT,
        "alert_cooldown_sec":   ALERT_COOLDOWN,
    }

@app.get("/api/gold/debug")
async def gold_debug():
    results = {}

    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            r = await client.get(
                "https://www.pnj.com.vn/blog/gia-vang/",
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            )
        results["pnj"] = {"status": r.status_code, "length": len(r.text), "preview": r.text[:300]}
    except Exception as e:
        results["pnj"] = {"error": str(e)}

    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            r = await client.get("https://giavang.net/", headers={"User-Agent": "Mozilla/5.0"})
        results["giavang_net"] = {"status": r.status_code, "length": len(r.text), "preview": r.text[:300]}
    except Exception as e:
        results["giavang_net"] = {"error": str(e)}

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://dojigroup.vn/api/product/gold-price", headers={"User-Agent": "Mozilla/5.0"})
        results["doji"] = {"status": r.status_code, "preview": r.text[:300]}
    except Exception as e:
        results["doji"] = {"error": str(e)}

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx?b=10",
                headers={"User-Agent": "Mozilla/5.0"},
            )
        results["vcb_baseline"] = {"status": r.status_code, "length": len(r.text)}
    except Exception as e:
        results["vcb_baseline"] = {"error": str(e)}

    return results

@app.get("/api/gold/debug2")
async def gold_debug2():
    results = {}

    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            r = await client.get(
                "https://www.pnj.com.vn/blog/gia-vang/",
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            )
        nums = re.findall(r'[\d]{2,3}[.,][\d]{3}[.,][\d]{3}', r.text)
        valid = [n for n in nums if 70_000_000 < float(n.replace(',','').replace('.','')) < 200_000_000]
        results["pnj"] = {
            "status": r.status_code,
            "gold_nums_found": valid[:10],
            "sjc_context": r.text[r.text.find('SJC')-50:r.text.find('SJC')+200] if 'SJC' in r.text else "SJC not found",
        }
    except Exception as e:
        results["pnj"] = {"error": str(e)}

    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            r = await client.get("https://giavang.net/", headers={"User-Agent": "Mozilla/5.0"})
        nums = re.findall(r'[\d]{2,3}[.,][\d]{3}[.,][\d]{3}', r.text)
        valid = [n for n in nums if 70_000_000 < float(n.replace(',','').replace('.','')) < 200_000_000]
        results["giavang_net"] = {
            "status": r.status_code,
            "gold_nums_found": valid[:10],
            "sjc_context": r.text[r.text.find('SJC')-50:r.text.find('SJC')+200] if 'SJC' in r.text else "SJC not found",
        }
    except Exception as e:
        results["giavang_net"] = {"error": str(e)}

    return results

@app.post("/api/alert/trigger-now")
async def trigger_alert_now():
    await job_alert()
    return {"status": "ok", "message": "Alert job executed"}

@app.get("/chat")
def chat_page():
    return FileResponse("static/chat.html")
