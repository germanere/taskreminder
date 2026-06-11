"""
Market Research Hub — Backend
==============================
- FastAPI server
- WebSocket proxy: Binance → client (with auto-reconnect)
- REST APIs: CoinGecko, Yahoo Finance, TCBS, SJC, Vietcombank
- Telegram alerts: BTC, ETH, USD/VND, Gold (SJC)
- Serve static files
"""

import os, json, logging, asyncio, time
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
    """Gửi Telegram bất đồng bộ — không block event loop."""
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

    # Dưới ngưỡng min
    if min_val and value < min_val:
        if _should_alert(key_min):
            _mark_alerted(key_min)
            alerts.append(
                f"🔴 *{label} XUỐNG NGƯỠNG*\n"
                f"💰 {value:,.0f}{unit} < {min_val:,.0f}{unit}\n🕐 {now}"
            )
    else:
        _clear_alert(key_min)

    # Vượt ngưỡng max
    if max_val and value > max_val:
        if _should_alert(key_max):
            _mark_alerted(key_max)
            alerts.append(
                f"🟢 *{label} VƯỢT NGƯỠNG*\n"
                f"💰 {value:,.0f}{unit} > {max_val:,.0f}{unit}\n🕐 {now}"
            )
    else:
        _clear_alert(key_max)

    # Biến động % mạnh so với lần check trước
    prev = _prev.get(key)
    if prev and prev > 0:
        pct = (value - prev) / prev * 100
        if abs(pct) >= CHANGE_PCT and _should_alert(key_pct):
            _mark_alerted(key_pct)
            icon = "📈" if pct > 0 else "📉"
            alerts.append(
                f"{icon} *{label} BIẾN ĐỘNG MẠNH*\n"
                f"{pct:+.2f}% | {prev:,.0f}{unit} → {value:,.0f}{unit}\n🕐 {now}"
            )
        elif abs(pct) < CHANGE_PCT * 0.5:
            _clear_alert(key_pct)

    _prev[key] = value
    return alerts

# ─────────────────────────────────────────────
# SJC PARSER
# ─────────────────────────────────────────────

def parse_sjc_sell(data) -> float | None:
    """
    SJC trả về list dạng:
      [{"khu_vuc": "TP.HCM", "ten_loai": "SJC 1L, 10C, 1KG", "gia_mua": ..., "gia_ban": ...}, ...]
    Lấy giá bán (gia_ban) của loại SJC 1L tại TP.HCM.
    Fallback: phần tử đầu tiên có gia_ban > 0.
    """
    if not isinstance(data, list):
        # Một số version API trả về {"data": [...]}
        data = data.get("data", []) if isinstance(data, dict) else []

    for item in data:
        name = str(item.get("ten_loai", "")).upper()
        region = str(item.get("khu_vuc", "")).upper()
        sell = item.get("gia_ban", 0)
        try:
            sell = float(str(sell).replace(",", "").replace(".", ""))
        except (ValueError, TypeError):
            continue
        if sell > 0 and "SJC" in name and "HCM" in region:
            return sell

    # Fallback: bất kỳ mục nào có gia_ban hợp lệ
    for item in data:
        sell = item.get("gia_ban", 0)
        try:
            sell = float(str(sell).replace(",", "").replace(".", ""))
            if sell > 0:
                return sell
        except (ValueError, TypeError):
            continue
    return None

# ─────────────────────────────────────────────
# SCHEDULER JOB
# ─────────────────────────────────────────────

async def job_alert():
    """Chạy mỗi 5 phút — fetch giá BTC, ETH, USD/VND, Gold và check alert."""
    all_alerts: list[str] = []

    # BTC
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
        btc = float(r.json().get("price", 0))
        all_alerts += check_alert("BTC", btc, BTC_MIN, BTC_MAX, "BTC/USDT", "$")
    except Exception as e:
        log.error(f"BTC alert error: {e}")

    # ETH
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get("https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT")
        eth = float(r.json().get("price", 0))
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
                break
    except Exception as e:
        log.error(f"USD alert error: {e}")

    # GOLD — SJC
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://sjc.com.vn/GoldPrice/Services/PriceService.ashx",
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://sjc.com.vn/"},
            )
        gold_price = parse_sjc_sell(r.json())
        if gold_price:
            all_alerts += check_alert("GOLD", gold_price, GOLD_MIN, GOLD_MAX, "Vàng SJC", "đ/lượng")
        else:
            log.warning("SJC: không parse được giá bán")
    except Exception as e:
        log.error(f"Gold alert error: {e}")

    # Gửi Telegram
    for alert in all_alerts:
        await send_telegram_async(alert)

    log.info(f"Alert check done — {len(all_alerts)} alert(s) sent")

# ─────────────────────────────────────────────
# LIFESPAN (thay thế @app.on_event deprecated)
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

# ─────────────────────────────────────────────
# SERVE STATIC
# ─────────────────────────────────────────────

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def index():
    return FileResponse("static/index.html")

# ─────────────────────────────────────────────
# WEBSOCKET — BINANCE KLINE (with auto-reconnect)
# ─────────────────────────────────────────────

@app.websocket("/ws/kline")
async def ws_kline(ws: WebSocket, symbol: str = "btcusdt", interval: str = "1h"):
    await ws.accept()
    binance_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{interval}"
    RECONNECT_DELAY = 3   # giây chờ trước khi reconnect
    MAX_RETRIES     = 10

    for attempt in range(MAX_RETRIES):
        try:
            async with websockets.connect(
                binance_url,
                ping_interval=20,
                ping_timeout=10,
            ) as binance_ws:
                log.info(f"Binance kline connected: {symbol} {interval}")
                while True:
                    try:
                        msg = await asyncio.wait_for(binance_ws.recv(), timeout=35)
                    except asyncio.TimeoutError:
                        # Gửi heartbeat về client để giữ kết nối
                        await ws.send_json({"ping": True})
                        continue

                    data = json.loads(msg)
                    k = data.get("k", {})
                    await ws.send_json({
                        "time":      k.get("t", 0) // 1000,
                        "open":      float(k.get("o", 0)),
                        "high":      float(k.get("h", 0)),
                        "low":       float(k.get("l", 0)),
                        "close":     float(k.get("c", 0)),
                        "volume":    float(k.get("v", 0)),
                        "is_closed": k.get("x", False),
                    })

        except WebSocketDisconnect:
            log.info("Client disconnected from kline WS")
            return
        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"Binance kline closed (attempt {attempt+1}): {e} — retry in {RECONNECT_DELAY}s")
        except Exception as e:
            log.error(f"Binance kline error (attempt {attempt+1}): {e}")

        # Thử reconnect — nhưng dừng nếu client đã ngắt
        try:
            await ws.send_json({"reconnecting": True, "attempt": attempt + 1})
        except Exception:
            return  # client đã đóng

        await asyncio.sleep(RECONNECT_DELAY)
        RECONNECT_DELAY = min(RECONNECT_DELAY * 2, 60)  # exponential backoff, tối đa 60s

    log.error(f"Binance kline: max retries reached for {symbol}")

# ─────────────────────────────────────────────
# WEBSOCKET — BINANCE ORDERBOOK (with auto-reconnect)
# ─────────────────────────────────────────────

@app.websocket("/ws/orderbook")
async def ws_orderbook(ws: WebSocket, symbol: str = "btcusdt"):
    await ws.accept()
    binance_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth20@100ms"
    RECONNECT_DELAY = 3
    MAX_RETRIES     = 10

    for attempt in range(MAX_RETRIES):
        try:
            async with websockets.connect(binance_url, ping_interval=20, ping_timeout=10) as binance_ws:
                log.info(f"Binance orderbook connected: {symbol}")
                while True:
                    msg  = await binance_ws.recv()
                    data = json.loads(msg)
                    await ws.send_json({
                        "bids": [[float(p), float(q)] for p, q in data.get("bids", [])[:10]],
                        "asks": [[float(p), float(q)] for p, q in data.get("asks", [])[:10]],
                    })

        except WebSocketDisconnect:
            log.info("Client disconnected from orderbook WS")
            return
        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"Binance orderbook closed (attempt {attempt+1}): {e}")
        except Exception as e:
            log.error(f"Binance orderbook error (attempt {attempt+1}): {e}")

        try:
            await ws.send_json({"reconnecting": True, "attempt": attempt + 1})
        except Exception:
            return

        await asyncio.sleep(RECONNECT_DELAY)
        RECONNECT_DELAY = min(RECONNECT_DELAY * 2, 60)

    log.error(f"Binance orderbook: max retries reached for {symbol}")

# ─────────────────────────────────────────────
# REST — HISTORICAL KLINES (Binance)
# ─────────────────────────────────────────────

@app.get("/api/klines")
async def get_klines(symbol: str = "BTCUSDT", interval: str = "1h", limit: int = 200):
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
# REST — CRYPTO PRICES (CoinGecko)
# ─────────────────────────────────────────────

@app.get("/api/crypto/prices")
async def get_crypto_prices(ids: str = "bitcoin,ethereum,solana,binancecoin,ripple"):
    url = (
        f"https://api.coingecko.com/api/v3/simple/price"
        f"?ids={ids}&vs_currencies=usd&include_24hr_change=true&include_market_cap=true"
    )
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url)
        return r.json()

@app.get("/api/crypto/top200")
async def get_top200(page: int = 1):
    url = (
        f"https://api.coingecko.com/api/v3/coins/markets"
        f"?vs_currency=usd&order=market_cap_desc&per_page=100&page={page}"
        f"&sparkline=false&price_change_percentage=24h,7d"
    )
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url)
        return r.json()

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
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://sjc.com.vn/GoldPrice/Services/PriceService.ashx",
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://sjc.com.vn/"},
            )
        return r.json()
    except Exception as e:
        log.error(f"Gold endpoint error: {e}")
        return {"error": str(e)}

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

@app.post("/api/alert/trigger-now")
async def trigger_alert_now():
    """Chạy job alert ngay lập tức (dùng để test)."""
    await job_alert()
    return {"status": "ok", "message": "Alert job executed"}
