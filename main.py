"""
Market Research Hub — Backend
==============================
- FastAPI server
- WebSocket proxy: Binance → client
- REST APIs: CoinGecko, Yahoo Finance, TCBS, SJC, Vietcombank
- Serve static files
"""

import os, json, logging, asyncio, time
from datetime import datetime, timedelta
import httpx
import pytz
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import websockets

ICT = pytz.timezone("Asia/Ho_Chi_Minh")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(title="Market Research Hub")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─────────────────────────────────────────────
# SERVE STATIC
# ─────────────────────────────────────────────

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def index():
    return FileResponse("static/index.html")

# ─────────────────────────────────────────────
# WEBSOCKET — BINANCE PROXY
# Nhận kline từ Binance rồi forward đến browser
# ─────────────────────────────────────────────

@app.websocket("/ws/kline")
async def ws_kline(ws: WebSocket, symbol: str = "btcusdt", interval: str = "1h"):
    await ws.accept()
    binance_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{interval}"
    try:
        async with websockets.connect(binance_url) as binance_ws:
            while True:
                try:
                    msg = await asyncio.wait_for(binance_ws.recv(), timeout=30)
                    data = json.loads(msg)
                    k = data.get("k", {})
                    await ws.send_json({
                        "time":  k.get("t", 0) // 1000,
                        "open":  float(k.get("o", 0)),
                        "high":  float(k.get("h", 0)),
                        "low":   float(k.get("l", 0)),
                        "close": float(k.get("c", 0)),
                        "volume": float(k.get("v", 0)),
                        "is_closed": k.get("x", False),
                    })
                except asyncio.TimeoutError:
                    await ws.send_json({"ping": True})
    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.error(f"WS error: {e}")

# ─────────────────────────────────────────────
# WEBSOCKET — BINANCE ORDERBOOK
# ─────────────────────────────────────────────

@app.websocket("/ws/orderbook")
async def ws_orderbook(ws: WebSocket, symbol: str = "btcusdt"):
    await ws.accept()
    binance_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth20@100ms"
    try:
        async with websockets.connect(binance_url) as binance_ws:
            while True:
                msg = await binance_ws.recv()
                data = json.loads(msg)
                await ws.send_json({
                    "bids": [[float(p), float(q)] for p, q in data.get("bids", [])[:10]],
                    "asks": [[float(p), float(q)] for p, q in data.get("asks", [])[:10]],
                })
    except (WebSocketDisconnect, Exception) as e:
        log.error(f"OB WS: {e}")

# ─────────────────────────────────────────────
# REST — HISTORICAL KLINES (Binance)
# ─────────────────────────────────────────────

@app.get("/api/klines")
async def get_klines(symbol: str = "BTCUSDT", interval: str = "1h", limit: int = 200):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    async with httpx.AsyncClient() as client:
        r = await client.get(url, timeout=10)
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
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd&include_24hr_change=true&include_market_cap=true"
    async with httpx.AsyncClient() as client:
        r = await client.get(url, timeout=10)
        return r.json()

@app.get("/api/crypto/top200")
async def get_top200(page: int = 1):
    url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page={page}&sparkline=false&price_change_percentage=24h,7d"
    async with httpx.AsyncClient() as client:
        r = await client.get(url, timeout=15)
        return r.json()

# ─────────────────────────────────────────────
# REST — FEAR & GREED
# ─────────────────────────────────────────────

@app.get("/api/fear-greed")
async def get_fear_greed():
    async with httpx.AsyncClient() as client:
        r = await client.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        return r.json()

# ─────────────────────────────────────────────
# REST — TỶ GIÁ (Vietcombank)
# ─────────────────────────────────────────────

@app.get("/api/forex/vnd")
async def get_forex_vnd():
    try:
        import xml.etree.ElementTree as ET
        async with httpx.AsyncClient() as client:
            r = await client.get(
                "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx?b=10",
                headers={"User-Agent": "Mozilla/5.0"}, timeout=10
            )
        root = ET.fromstring(r.text)
        rates = {}
        for ex in root.findall(".//Exrate"):
            code = ex.get("CurrencyCode","")
            sell = ex.get("Sell","0").replace(",","")
            buy  = ex.get("Buy","0").replace(",","")
            if code in ["USD","EUR","JPY","CNY","GBP"]:
                rates[code] = {"sell": float(sell) if sell else 0, "buy": float(buy) if buy else 0}
        return rates
    except Exception as e:
        return {"error": str(e)}

# ─────────────────────────────────────────────
# REST — GIÁ VÀNG SJC
# ─────────────────────────────────────────────

@app.get("/api/gold")
async def get_gold():
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                "https://sjc.com.vn/GoldPrice/Services/PriceService.ashx",
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://sjc.com.vn/"},
                timeout=10
            )
        return r.json()
    except Exception as e:
        return {"error": str(e)}

# ─────────────────────────────────────────────
# REST — VN STOCK (Yahoo Finance)
# ─────────────────────────────────────────────

@app.get("/api/vn/stocks")
async def get_vn_stocks(symbols: str = "VNM.VN,FPT.VN,VCB.VN,HPG.VN,MWG.VN,TCB.VN,VIC.VN,VHM.VN"):
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols}"
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        data = r.json()
    quotes = data.get("quoteResponse", {}).get("result", [])
    return [{
        "symbol": q.get("symbol","").replace(".VN",""),
        "price":  q.get("regularMarketPrice", 0),
        "change": q.get("regularMarketChangePercent", 0),
        "volume": q.get("regularMarketVolume", 0),
        "pe":     q.get("trailingPE", 0),
        "market_cap": q.get("marketCap", 0),
    } for q in quotes]

# ─────────────────────────────────────────────
# REST — ECONOMIC CALENDAR (Investing.com public)
# ─────────────────────────────────────────────

@app.get("/api/calendar")
async def get_calendar():
    # Trả về dữ liệu mẫu — production dùng Investing.com API hoặc Finnhub
    now = datetime.now(ICT)
    return [
        {"date": (now + timedelta(days=1)).strftime("%d/%m/%Y"), "time": "19:30", "event": "US CPI MoM", "impact": "high", "prev": "0.3%", "forecast": "0.2%"},
        {"date": (now + timedelta(days=2)).strftime("%d/%m/%Y"), "time": "02:00", "event": "FED Rate Decision", "impact": "high", "prev": "5.50%", "forecast": "5.50%"},
        {"date": (now + timedelta(days=3)).strftime("%d/%m/%Y"), "time": "08:00", "event": "BTC Options Expiry", "impact": "medium", "prev": "$1.8B", "forecast": "$2.1B"},
        {"date": (now + timedelta(days=5)).strftime("%d/%m/%Y"), "time": "21:30", "event": "US NFP", "impact": "high", "prev": "175K", "forecast": "180K"},
    ]

# ─────────────────────────────────────────────
# REST — LIQUIDATION DATA (Coinglass public)
# ─────────────────────────────────────────────

@app.get("/api/liquidations")
async def get_liquidations(symbol: str = "BTC"):
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"https://open-api.coinglass.com/public/v2/liquidation_history?symbol={symbol}&timeType=0",
                headers={"coinglassSecret": ""},
                timeout=10
            )
        return r.json()
    except:
        return {"data": []}

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(ICT).isoformat()}

# ─────────────────────────────────────────────
# TELEGRAM ALERTS
# ─────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# Ngưỡng cảnh báo — đổi qua env var
BTC_MIN   = float(os.getenv("BTC_MIN", "60000"))
BTC_MAX   = float(os.getenv("BTC_MAX", "75000"))
ETH_MIN   = float(os.getenv("ETH_MIN", "2800"))
ETH_MAX   = float(os.getenv("ETH_MAX", "4500"))
CHANGE_PCT = float(os.getenv("CHANGE_PCT", "3.0"))   # % biến động mạnh
USD_MIN   = float(os.getenv("USD_MIN", "24000"))
USD_MAX   = float(os.getenv("USD_MAX", "26500"))
GOLD_MIN  = float(os.getenv("GOLD_MIN", "80000000"))
GOLD_MAX  = float(os.getenv("GOLD_MAX", "120000000"))

# Lưu giá trước để tính % thay đổi
_prev = {}
# Lưu trạng thái đã alert để không spam
_alerted = {}

def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10
        )
        log.info(f"Telegram sent: {text[:60]}...")
    except Exception as e:
        log.error(f"Telegram error: {e}")

def check_alert(key: str, value: float, min_val: float, max_val: float, label: str, unit: str = ""):
    alerts = []
    now = datetime.now(ICT).strftime("%H:%M %d/%m")

    # Vượt ngưỡng
    key_min = f"{key}_min"
    key_max = f"{key}_max"

    if min_val and value < min_val and not _alerted.get(key_min):
        _alerted[key_min] = True
        alerts.append(f"🔴 *{label} XUỐNG NGƯỠNG*\n💰 {value:,.0f}{unit} < {min_val:,.0f}{unit}\n🕐 {now}")
    elif value >= min_val:
        _alerted.pop(key_min, None)

    if max_val and value > max_val and not _alerted.get(key_max):
        _alerted[key_max] = True
        alerts.append(f"🟢 *{label} VƯỢT NGƯỠNG*\n💰 {value:,.0f}{unit} > {max_val:,.0f}{unit}\n🕐 {now}")
    elif value <= max_val:
        _alerted.pop(key_max, None)

    # Biến động % mạnh
    prev = _prev.get(key)
    if prev:
        pct = (value - prev) / prev * 100
        key_pct = f"{key}_pct"
        if abs(pct) >= CHANGE_PCT and not _alerted.get(key_pct):
            _alerted[key_pct] = True
            icon = "📈" if pct > 0 else "📉"
            alerts.append(f"{icon} *{label} BIẾN ĐỘNG MẠNH*\n{pct:+.2f}% | {prev:,.0f} → {value:,.0f}{unit}\n🕐 {now}")
        elif abs(pct) < CHANGE_PCT * 0.5:
            _alerted.pop(key_pct, None)

    _prev[key] = value
    return alerts


async def job_alert():
    """Chạy mỗi 5 phút — fetch giá và check alert."""
    all_alerts = []

    # BTC
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", timeout=8)
            btc = float(r.json().get("price", 0))
        all_alerts += check_alert("BTC", btc, BTC_MIN, BTC_MAX, "BTC/USDT", "$")
    except Exception as e:
        log.error(f"BTC alert: {e}")

    # ETH
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get("https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT", timeout=8)
            eth = float(r.json().get("price", 0))
        all_alerts += check_alert("ETH", eth, ETH_MIN, ETH_MAX, "ETH/USDT", "$")
    except Exception as e:
        log.error(f"ETH alert: {e}")

    # USD/VND
    try:
        import xml.etree.ElementTree as ET
        async with httpx.AsyncClient() as client:
            r = await client.get(
                "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx?b=10",
                headers={"User-Agent": "Mozilla/5.0"}, timeout=10
            )
        root = ET.fromstring(r.text)
        for ex in root.findall(".//Exrate"):
            if ex.get("CurrencyCode") == "USD":
                usd = float(ex.get("Sell", "0").replace(",", ""))
                all_alerts += check_alert("USD", usd, USD_MIN, USD_MAX, "USD/VND", "đ")
    except Exception as e:
        log.error(f"USD alert: {e}")

    # Gửi tất cả alerts
    for alert in all_alerts:
        send_telegram(alert)

    if not all_alerts:
        log.info("Alert check: no alerts triggered")


# ─────────────────────────────────────────────
# SCHEDULER — thêm alert job
# ─────────────────────────────────────────────

from apscheduler.schedulers.asyncio import AsyncIOScheduler

alert_scheduler = AsyncIOScheduler(timezone=ICT)
alert_scheduler.add_job(job_alert, "interval", minutes=5, id="alert")

@app.on_event("startup")
async def startup():
    alert_scheduler.start()
    log.info("Alert scheduler started — checking every 5 minutes")

@app.get("/api/alert/test")
async def test_alert():
    """Test gửi Telegram thủ công."""
    send_telegram("✅ *Market Hub Alert Test*\nBot đang hoạt động bình thường!\n🕐 " + datetime.now(ICT).strftime("%H:%M %d/%m/%Y"))
    return {"status": "sent"}

@app.get("/api/alert/config")
def alert_config():
    return {
        "BTC": {"min": BTC_MIN, "max": BTC_MAX},
        "ETH": {"min": ETH_MIN, "max": ETH_MAX},
        "USD_VND": {"min": USD_MIN, "max": USD_MAX},
        "change_pct_threshold": CHANGE_PCT,
    }
