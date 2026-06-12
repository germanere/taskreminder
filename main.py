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

BTC_MIN    = float(os.getenv("BTC_MIN",    "60000"))
BTC_MAX    = float(os.getenv("BTC_MAX",    "75000"))
ETH_MIN    = float(os.getenv("ETH_MIN",    "2800"))
ETH_MAX    = float(os.getenv("ETH_MAX",    "4500"))
CHANGE_PCT = float(os.getenv("CHANGE_PCT", "3.0"))
USD_MIN    = float(os.getenv("USD_MIN",    "24000"))
USD_MAX    = float(os.getenv("USD_MAX",    "26500"))
GOLD_MIN   = float(os.getenv("GOLD_MIN",   "80000000"))
GOLD_MAX   = float(os.getenv("GOLD_MAX",   "120000000"))

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
    """Format giá trị với đơn vị đúng vị trí.
    $ → $103,456  |  đ/... → 103,456 đ/lượng  |  blank → 103,456
    """
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
    """
    Lấy giá từ Bybit (linear → spot fallback) rồi Binance.
    Raise ValueError nếu tất cả đều thất bại — tránh trả về 0 gây alert sai.
    """
    # Bybit v5 — thử linear trước, rồi spot
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
        price = float(r.json().get("price", 0))
        if price > 0:
            log.info(f"Binance fallback price {symbol_binance}: {price}")
            return price
    except Exception as e:
        log.warning(f"Binance fallback price error ({symbol_binance}): {e}")

    raise ValueError(f"Không lấy được giá cho {symbol_bybit} / {symbol_binance}")

# ─────────────────────────────────────────────
# GOLD PRICE FETCH — SJC textContent → BTMC fallback
# PriceService.ashx bị block 403 từ Render US IP
# ─────────────────────────────────────────────

async def fetch_gold_price() -> float | None:
    """
    Lấy giá vàng SJC (VND/lượng).
    Endpoint 1: sjc.com.vn/giavang/textContent.aspx
    Endpoint 2: api.btmc.vn (Bảo Tín Minh Châu) — public, không block Render
    """
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:

        # ── Endpoint 1: SJC textContent ──────────────────────
        try:
            r = await client.get(
                "https://sjc.com.vn/giavang/textContent.aspx",
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://sjc.com.vn/"},
            )
            if r.status_code == 200 and r.text.strip():
                # Tìm số dạng 1xx.xxx.xxx hoặc xx.xxx.xxx (giá vàng ~80-130 triệu)
                nums = re.findall(r"\b\d{2,3}[.,]\d{3}[.,]\d{3}\b", r.text)
                for n in nums:
                    val = float(n.replace(",", "").replace(".", ""))
                    if 70_000_000 < val < 200_000_000:
                        log.info(f"SJC textContent gold price: {val}")
                        return val
            else:
                log.warning(f"SJC textContent status: {r.status_code}")
        except Exception as e:
            log.warning(f"SJC textContent error: {e}")

        # ── Endpoint 2: BTMC API ──────────────────────────────
        try:
            r = await client.get(
                "https://api.btmc.vn/api/BTMCAPI/getpricebtmc?key=3kd8ub1llcg9t45hnoh8hmn7t5kc2v",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            data = r.json()
            rows = data.get("DataList", {}).get("Data", [])

            # Ưu tiên row có tên chứa "SJC"
            for row in rows:
                name = str(row.get("n_1", "") + row.get("@rowid", "")).upper()
                try:
                    sell = float(str(row.get("pb_1", 0)).replace(",", "")) * 1000
                    if "SJC" in name and sell > 70_000_000:
                        log.info(f"BTMC SJC gold price: {sell}")
                        return sell
                except (ValueError, TypeError):
                    continue

            # Fallback: row đầu tiên hợp lệ
            for row in rows:
                try:
                    sell = float(str(row.get("pb_1", 0)).replace(",", "")) * 1000
                    if sell > 70_000_000:
                        log.info(f"BTMC fallback gold price: {sell}")
                        return sell
                except (ValueError, TypeError):
                    continue

        except Exception as e:
            log.warning(f"BTMC API error: {e}")

    log.error("fetch_gold_price: tất cả endpoint thất bại")
    return None

# ─────────────────────────────────────────────
# SCHEDULER JOB
# ─────────────────────────────────────────────

async def job_alert():
    all_alerts: list[str] = []

    # BTC — Bybit primary, Binance fallback
    try:
        btc = await fetch_price("BTCUSDT", "BTCUSDT")
        all_alerts += check_alert("BTC", btc, BTC_MIN, BTC_MAX, "BTC/USDT", "$")
    except Exception as e:
        log.error(f"BTC alert error: {e}")

    # ETH — Bybit primary, Binance fallback
    try:
        eth = await fetch_price("ETHUSDT", "ETHUSDT")
        all_alerts += check_alert("ETH", eth, ETH_MIN, ETH_MAX, "ETH/USDT", "$")
    except Exception as e:
        log.error(f"ETH alert error: {e}")

    # USD/VND — Vietcombank
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

    # GOLD — SJC textContent → BTMC fallback
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
# Bybit interval mapping: 1m→1, 3m→3, 5m→5, 15m→15, 30m→30,
#                         1h→60, 2h→120, 4h→240, 1d→D, 1w→W
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
# ─────────────────────────────────────────────

@app.get("/api/klines")
async def get_klines(symbol: str = "BTCUSDT", interval: str = "1h", limit: int = 200):
    bybit_symbol   = to_bybit_symbol(symbol)
    bybit_interval = BYBIT_INTERVAL_MAP.get(interval, "60")

    # ── Bybit ──────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.bybit.com/v5/market/kline",
                params={
                    "category": "linear",
                    "symbol":   bybit_symbol,
                    "interval": bybit_interval,
                    "limit":    limit,
                },
            )
        result = r.json()
        if result.get("retCode") == 0:
            raw = result["result"]["list"][::-1]
            return [{
                "time":   int(k[0]) // 1000,
                "open":   float(k[1]),
                "high":   float(k[2]),
                "low":    float(k[3]),
                "close":  float(k[4]),
                "volume": float(k[5]),
            } for k in raw]
        log.warning(f"Bybit kline non-zero retCode: {result.get('retMsg')}")
    except Exception as e:
        log.warning(f"Bybit kline REST error: {e} — falling back to Binance")

    # ── Binance fallback ───────────────────────
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url)
        data = r.json()
    return [{
        "time":   int(k[0]) // 1000,
        "open":   float(k[1]),
        "high":   float(k[2]),
        "low":    float(k[3]),
        "close":  float(k[4]),
        "volume": float(k[5]),
    } for k in data]

# ─────────────────────────────────────────────
# REST — CRYPTO PRICES (CoinGecko) — rate-limited cache
# ─────────────────────────────────────────────

_coingecko_cache: dict = {}
COINGECKO_TTL = 60

async def _coingecko_get(url: str, ttl: int = COINGECKO_TTL):
    cached = _coingecko_cache.get(url)
    if cached and (time.time() - cached[0]) < ttl:
        return cached[1]
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url)
        if r.status_code == 429:
            log.warning("CoinGecko 429 — returning cached data if available")
            return cached[1] if cached else []  # FIX: trả [] thay vì {} để frontend .slice() không crash
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
# PriceService.ashx bị block 403 từ Render US IP
# → dùng textContent.aspx → BTMC fallback
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
async def get_vn_stocks(symbols: str = "VNM.VN,FPT.VN,VCB.VN,HPG.VN,MWG.VN,TCB.VN,VIC.VN,VHM.VN"):
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

    # Test 1: PNJ API
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            r = await client.get(
                "https://www.pnj.com.vn/blog/gia-vang/",
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            )
        results["pnj"] = {"status": r.status_code, "length": len(r.text), "preview": r.text[:300]}
    except Exception as e:
        results["pnj"] = {"error": str(e)}

    # Test 2: giavang.net
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            r = await client.get(
                "https://giavang.net/",
                headers={"User-Agent": "Mozilla/5.0"},
            )
        results["giavang_net"] = {"status": r.status_code, "length": len(r.text), "preview": r.text[:300]}
    except Exception as e:
        results["giavang_net"] = {"error": str(e)}

    # Test 3: DOJI API
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://dojigroup.vn/api/product/gold-price",
                headers={"User-Agent": "Mozilla/5.0"},
            )
        results["doji"] = {"status": r.status_code, "preview": r.text[:300]}
    except Exception as e:
        results["doji"] = {"error": str(e)}

    # Test 4: Vietcombank (đã hoạt động, dùng làm baseline)
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

@app.post("/api/alert/trigger-now")
async def trigger_alert_now():
    await job_alert()
    return {"status": "ok", "message": "Alert job executed"}
