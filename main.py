"""
Market Research Hub — Backend
==============================
- FastAPI server
- WebSocket proxy: Bybit → client
- REST APIs: Bybit klines, CoinGecko, Yahoo Finance, TCBS, SJC, Vietcombank
- HOSE Top 50 endpoint
- Telegram alerts: BTC, ETH, USD/VND, Gold (SJC)
- Serve static files
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

# ─────────────────────────────────────────────
# ALERT STATE
# ─────────────────────────────────────────────

_prev: dict    = {}
_alerted: dict = {}
ALERT_COOLDOWN = 3600

def _should_alert(key: str) -> bool:
    last = _alerted.get(key, 0)
    return (time.time() - last) >= ALERT_COOLDOWN

def _mark_alerted(key: str):
    _alerted[key] = time.time()

def _clear_alert(key: str):
    _alerted.pop(key, None)

# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────

async def send_telegram_async(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set — skipping")
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


def check_alert(key, value, min_val, max_val, label, unit=""):
    alerts = []
    now = datetime.now(ICT).strftime("%H:%M %d/%m")
    key_min, key_max, key_pct = f"{key}_min", f"{key}_max", f"{key}_pct"
    v_str, min_str, max_str = _fmt(value, unit), _fmt(min_val, unit), _fmt(max_val, unit)

    if min_val and value < min_val:
        if _should_alert(key_min):
            _mark_alerted(key_min)
            alerts.append(f"🔴 *{label} XUỐNG NGƯỠNG*\n💰 {v_str} < {min_str}\n🕐 {now}")
    else:
        _clear_alert(key_min)

    if max_val and value > max_val:
        if _should_alert(key_max):
            _mark_alerted(key_max)
            alerts.append(f"🟢 *{label} VƯỢT NGƯỠNG*\n💰 {v_str} > {max_str}\n🕐 {now}")
    else:
        _clear_alert(key_max)

    prev = _prev.get(key)
    if prev and prev > 0:
        pct = (value - prev) / prev * 100
        if abs(pct) >= CHANGE_PCT and _should_alert(key_pct):
            _mark_alerted(key_pct)
            icon = "📈" if pct > 0 else "📉"
            alerts.append(f"{icon} *{label} BIẾN ĐỘNG MẠNH*\n{pct:+.2f}% | {_fmt(prev,unit)} → {v_str}\n🕐 {now}")
        elif abs(pct) < CHANGE_PCT * 0.5:
            _clear_alert(key_pct)

    _prev[key] = value
    return alerts

# ─────────────────────────────────────────────
# PRICE FETCH HELPERS
# ─────────────────────────────────────────────

async def fetch_price(symbol_bybit: str, symbol_binance: str) -> float:
    for category in ("linear", "spot"):
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                r = await client.get(
                    "https://api.bybit.com/v5/market/tickers",
                    params={"category": category, "symbol": symbol_bybit},
                )
            lst = r.json().get("result", {}).get("list", [])
            if lst:
                price = float(lst[0]["lastPrice"])
                if price > 0:
                    return price
        except Exception as e:
            log.warning(f"Bybit {category} price error ({symbol_bybit}): {e}")

    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol_binance}")
        data = r.json()
        if not isinstance(data, dict):
            raise ValueError(f"Binance unexpected response: {type(data)}")
        price = float(data.get("price", 0))
        if price > 0:
            return price
    except Exception as e:
        log.warning(f"Binance fallback price error ({symbol_binance}): {e}")

    raise ValueError(f"Cannot fetch price for {symbol_bybit}/{symbol_binance}")

# ─────────────────────────────────────────────
# GOLD PRICE
# ─────────────────────────────────────────────

async def fetch_gold_price() -> float | None:
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
                return round(price / 100_000) * 100_000
    except Exception as e:
        log.error(f"fetch_gold_price error: {e}")
    return None

# ─────────────────────────────────────────────
# SCHEDULER JOB
# ─────────────────────────────────────────────

async def job_alert():
    all_alerts = []

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
                break
    except Exception as e:
        log.error(f"USD alert error: {e}")

    try:
        gold_price = await fetch_gold_price()
        if gold_price and gold_price > 0:
            all_alerts += check_alert("GOLD", gold_price, GOLD_MIN, GOLD_MAX, "Vàng SJC", "đ/lượng")
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
    log.info("Alert scheduler started")
    yield
    alert_scheduler.shutdown(wait=False)

# ─────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────

app = FastAPI(title="Market Research Hub", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def index():
    return FileResponse("static/index.html")

@app.get("/chat")
def chat_page():
    return FileResponse("static/chat.html")

# ─────────────────────────────────────────────
# BYBIT SYMBOL / INTERVAL HELPERS
# ─────────────────────────────────────────────

def to_bybit_symbol(symbol: str) -> str:
    return symbol.upper()

BYBIT_INTERVAL_MAP = {
    "1m": "1",  "3m": "3",   "5m": "5",   "15m": "15",  "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360",  "12h": "720",
    "1d": "D",  "1w": "W",   "1M": "M",
}

# ─────────────────────────────────────────────
# WEBSOCKET — BYBIT KLINE
# ─────────────────────────────────────────────

@app.websocket("/ws/kline")
async def ws_kline(ws: WebSocket, symbol: str = "btcusdt", interval: str = "1h"):
    await ws.accept()
    bybit_symbol   = to_bybit_symbol(symbol)
    bybit_interval = BYBIT_INTERVAL_MAP.get(interval, "60")
    bybit_url      = "wss://stream.bybit.com/v5/public/linear"
    subscribe_msg  = json.dumps({"op": "subscribe", "args": [f"kline.{bybit_interval}.{bybit_symbol}"]})
    RECONNECT_DELAY = 3

    for attempt in range(10):
        try:
            async with websockets.connect(bybit_url, ping_interval=20, ping_timeout=10) as bybit_ws:
                await bybit_ws.send(subscribe_msg)
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
                            "time": int(k.get("start", 0)) // 1000,
                            "open": float(k.get("open", 0)),
                            "high": float(k.get("high", 0)),
                            "low":  float(k.get("low", 0)),
                            "close": float(k.get("close", 0)),
                            "volume": float(k.get("volume", 0)),
                            "is_closed": k.get("confirm", False),
                        })
        except WebSocketDisconnect:
            return
        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"Bybit kline closed (attempt {attempt+1}): {e}")
        except Exception as e:
            log.error(f"Bybit kline error (attempt {attempt+1}): {e}")

        try:
            await ws.send_json({"reconnecting": True, "attempt": attempt + 1})
        except Exception:
            return
        await asyncio.sleep(RECONNECT_DELAY)
        RECONNECT_DELAY = min(RECONNECT_DELAY * 2, 60)

# ─────────────────────────────────────────────
# WEBSOCKET — BYBIT ORDERBOOK
# ─────────────────────────────────────────────

@app.websocket("/ws/orderbook")
async def ws_orderbook(ws: WebSocket, symbol: str = "btcusdt"):
    await ws.accept()
    bybit_symbol  = to_bybit_symbol(symbol)
    bybit_url     = "wss://stream.bybit.com/v5/public/linear"
    subscribe_msg = json.dumps({"op": "subscribe", "args": [f"orderbook.50.{bybit_symbol}"]})
    RECONNECT_DELAY = 3

    local_bids: dict = {}
    local_asks: dict = {}

    def apply_delta(book, entries):
        for price, qty in entries:
            if float(qty) == 0:
                book.pop(price, None)
            else:
                book[price] = float(qty)

    for attempt in range(10):
        try:
            local_bids.clear()
            local_asks.clear()
            async with websockets.connect(bybit_url, ping_interval=20, ping_timeout=10) as bybit_ws:
                await bybit_ws.send(subscribe_msg)
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

# ─────────────────────────────────────────────
# REST — HISTORICAL KLINES
# ─────────────────────────────────────────────

@app.get("/api/klines")
async def get_klines(symbol: str = "BTCUSDT", interval: str = "1h", limit: int = 200):
    bybit_symbol   = to_bybit_symbol(symbol)
    bybit_interval = BYBIT_INTERVAL_MAP.get(interval, "60")

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.bybit.com/v5/market/kline",
                params={"category": "linear", "symbol": bybit_symbol, "interval": bybit_interval, "limit": limit},
            )
        result = r.json()
        if result.get("retCode") == 0:
            raw = result["result"]["list"]
            if isinstance(raw, list) and len(raw) > 0:
                raw = raw[::-1]
                return [{"time": int(k[0])//1000, "open": float(k[1]), "high": float(k[2]),
                         "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])} for k in raw]
    except Exception as e:
        log.warning(f"Bybit kline REST error: {e}")

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}&interval={interval}&limit={limit}"
            )
        data = r.json()
        if not isinstance(data, list):
            return JSONResponse(status_code=503, content={"error": "Kline fetch failed"})
        return [{"time": int(k[0])//1000, "open": float(k[1]), "high": float(k[2]),
                 "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])} for k in data]
    except Exception as e:
        return JSONResponse(status_code=503, content={"error": str(e)})

# ─────────────────────────────────────────────
# REST — CRYPTO PRICES (CoinGecko)
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
            return cached[1] if cached else []
        data = r.json()
    _coingecko_cache[url] = (time.time(), data)
    return data

@app.get("/api/crypto/prices")
async def get_crypto_prices(ids: str = "bitcoin,ethereum,solana,binancecoin,ripple"):
    url = (f"https://api.coingecko.com/api/v3/simple/price"
           f"?ids={ids}&vs_currencies=usd&include_24hr_change=true&include_market_cap=true")
    return await _coingecko_get(url)

@app.get("/api/crypto/top200")
async def get_top200(page: int = 1):
    url = (f"https://api.coingecko.com/api/v3/coins/markets"
           f"?vs_currency=usd&order=market_cap_desc&per_page=100&page={page}"
           f"&sparkline=false&price_change_percentage=24h,7d")
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
                rates[code] = {"sell": float(sell) if sell else 0, "buy": float(buy) if buy else 0}
        return rates
    except Exception as e:
        return {"error": str(e)}

# ─────────────────────────────────────────────
# REST — GIÁ VÀNG
# ─────────────────────────────────────────────

@app.get("/api/gold")
async def get_gold():
    try:
        price = await fetch_gold_price()
        if price:
            return {"price": price, "unit": "VND/lượng", "source": "Yahoo+VCB"}
        return JSONResponse(status_code=503, content={"error": "Không lấy được giá vàng"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ─────────────────────────────────────────────
# REST — VN STOCKS (Yahoo Finance) — generic endpoint
# ─────────────────────────────────────────────

YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finance.yahoo.com/",
    "Origin": "https://finance.yahoo.com",
}

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

@app.get("/api/vn/stocks")
async def get_vn_stocks(
    symbols: str = "VNM.VN,FPT.VN,VCB.VN,HPG.VN,MWG.VN,TCB.VN,VIC.VN,VHM.VN,BID.VN,CTG.VN"
                   ",VPB.VN,MBB.VN,ACB.VN,STB.VN,HDB.VN,VIB.VN,SSI.VN,VND.VN,HCM.VN,MSN.VN"
                   ",VRE.VN,PDR.VN,DXG.VN,NVL.VN,KDH.VN,GVR.VN,SAB.VN,GAS.VN,PLX.VN,POW.VN"
):
    sym_list = [s.strip() for s in symbols.split(",")]
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=10) as client:
        results = await asyncio.gather(*[_fetch_yahoo_stock(client, s) for s in sym_list], return_exceptions=True)
    return [r for r in results if isinstance(r, dict)]

# ─────────────────────────────────────────────
# REST — HOSE TOP 50 (vốn hóa lớn nhất)
# Lấy từ TCBS API — public, không cần auth
# Fallback: Yahoo Finance với danh sách cố định
# ─────────────────────────────────────────────

# 50 mã HOSE vốn hóa lớn nhất (tháng 6/2025)
HOSE_TOP50 = [
    "VCB","BID","VIC","VHM","CTG","GAS","VNM","SAB","MSN","TCB",
    "MBB","FPT","ACB","PLX","HPG","VPB","STB","HDB","GVR","POW",
    "MWG","PNJ","REE","SSI","VND","HCM","DPM","DCM","VEA","KDH",
    "NVL","PDR","DXG","PVD","HSG","NKG","PHR","DRC","IDC","KBC",
    "NTC","LHG","EIB","EVF","CMG","VGI","FRT","DGW","GEX","VRE",
]

HOSE_INFO = {
    "VCB":  {"name": "Vietcombank",          "sector": "Ngân hàng"},
    "BID":  {"name": "BIDV",                 "sector": "Ngân hàng"},
    "VIC":  {"name": "Vingroup",             "sector": "Bất động sản"},
    "VHM":  {"name": "Vinhomes",             "sector": "Bất động sản"},
    "CTG":  {"name": "VietinBank",           "sector": "Ngân hàng"},
    "GAS":  {"name": "PV Gas",               "sector": "Năng lượng"},
    "VNM":  {"name": "Vinamilk",             "sector": "Tiêu dùng"},
    "SAB":  {"name": "Sabeco",               "sector": "Tiêu dùng"},
    "MSN":  {"name": "Masan Group",          "sector": "Tiêu dùng"},
    "TCB":  {"name": "Techcombank",          "sector": "Ngân hàng"},
    "MBB":  {"name": "MB Bank",              "sector": "Ngân hàng"},
    "FPT":  {"name": "FPT Corp",             "sector": "Công nghệ"},
    "ACB":  {"name": "ACB",                  "sector": "Ngân hàng"},
    "PLX":  {"name": "Petrolimex",           "sector": "Năng lượng"},
    "HPG":  {"name": "Hòa Phát Group",       "sector": "Vật liệu"},
    "VPB":  {"name": "VPBank",               "sector": "Ngân hàng"},
    "STB":  {"name": "Sacombank",            "sector": "Ngân hàng"},
    "HDB":  {"name": "HDBank",               "sector": "Ngân hàng"},
    "GVR":  {"name": "VRG",                  "sector": "Công nghiệp"},
    "POW":  {"name": "PV Power",             "sector": "Năng lượng"},
    "MWG":  {"name": "Thế Giới Di Động",     "sector": "Tiêu dùng"},
    "PNJ":  {"name": "PNJ",                  "sector": "Tiêu dùng"},
    "REE":  {"name": "Cơ Điện Lạnh REE",     "sector": "Công nghiệp"},
    "SSI":  {"name": "SSI Securities",       "sector": "Chứng khoán"},
    "VND":  {"name": "VNDirect",             "sector": "Chứng khoán"},
    "HCM":  {"name": "HSC",                  "sector": "Chứng khoán"},
    "DPM":  {"name": "Đạm Phú Mỹ",          "sector": "Hóa chất"},
    "DCM":  {"name": "Đạm Cà Mau",          "sector": "Hóa chất"},
    "VEA":  {"name": "VEAM",                 "sector": "Công nghiệp"},
    "KDH":  {"name": "Khang Điền",           "sector": "Bất động sản"},
    "NVL":  {"name": "Novaland",             "sector": "Bất động sản"},
    "PDR":  {"name": "Phát Đạt",             "sector": "Bất động sản"},
    "DXG":  {"name": "Đất Xanh Group",       "sector": "Bất động sản"},
    "PVD":  {"name": "PV Drilling",          "sector": "Năng lượng"},
    "HSG":  {"name": "Hoa Sen Group",        "sector": "Vật liệu"},
    "NKG":  {"name": "Nam Kim Steel",        "sector": "Vật liệu"},
    "PHR":  {"name": "Cao su Phước Hòa",     "sector": "Vật liệu"},
    "DRC":  {"name": "Cao su Đà Nẵng",       "sector": "Vật liệu"},
    "IDC":  {"name": "IDICO",               "sector": "Bất động sản"},
    "KBC":  {"name": "Kinh Bắc City",        "sector": "Bất động sản"},
    "NTC":  {"name": "Nam Tân Uyên",         "sector": "Bất động sản"},
    "LHG":  {"name": "Long Hậu",             "sector": "Bất động sản"},
    "EIB":  {"name": "Eximbank",             "sector": "Ngân hàng"},
    "EVF":  {"name": "EVNFinance",           "sector": "Ngân hàng"},
    "CMG":  {"name": "CMC Corp",             "sector": "Công nghệ"},
    "VGI":  {"name": "Viettel Global",       "sector": "Công nghệ"},
    "FRT":  {"name": "FPT Retail",           "sector": "Tiêu dùng"},
    "DGW":  {"name": "Digiworld",            "sector": "Tiêu dùng"},
    "GEX":  {"name": "Gelex Group",          "sector": "Công nghiệp"},
    "VRE":  {"name": "Vincom Retail",        "sector": "Bất động sản"},
}

_hose_cache: dict = {}
HOSE_TTL = 60  # giây

@app.get("/api/vn/hose-top50")
async def get_hose_top50():
    """
    Lấy giá thực 50 mã HOSE từ TCBS API (public).
    TCBS endpoint: https://apipublic.tcbs.com.vn/stock-insight/v1/stock/price?tickers=VCB,BID,...
    Fallback: Yahoo Finance .VN symbols
    Cache 60s để tránh spam API.
    """
    now = time.time()
    cached = _hose_cache.get("top50")
    if cached and (now - cached["ts"]) < HOSE_TTL:
        return cached["data"]

    results = []

    # ── TCBS public API ──────────────────────
    try:
        tickers_str = ",".join(HOSE_TOP50)
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                "https://apipublic.tcbs.com.vn/stock-insight/v1/stock/price",
                params={"tickers": tickers_str},
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json",
                    "Referer": "https://tcinvest.tcbs.com.vn/",
                },
            )
        data = r.json()
        price_map = {}

        # TCBS trả về list hoặc dict tùy version
        items = data if isinstance(data, list) else data.get("data", [])
        for item in items:
            ticker = item.get("ticker") or item.get("symbol") or ""
            ticker = ticker.upper()
            if not ticker:
                continue
            price  = float(item.get("close") or item.get("price") or item.get("lastPrice") or 0)
            prev   = float(item.get("referencePrice") or item.get("prevClose") or item.get("ref") or 0)
            change = round((price - prev) / prev * 100, 2) if prev > 0 else 0
            volume = int(item.get("volume") or item.get("totalVolume") or 0)
            price_map[ticker] = {"price": price, "change": change, "volume": volume}

        if price_map:
            for i, sym in enumerate(HOSE_TOP50):
                info = HOSE_INFO.get(sym, {"name": sym, "sector": "Khác"})
                p    = price_map.get(sym, {})
                results.append({
                    "rank":    i + 1,
                    "symbol":  sym,
                    "name":    info["name"],
                    "sector":  info["sector"],
                    "price":   p.get("price",  0),
                    "change":  p.get("change", 0),
                    "volume":  p.get("volume", 0),
                    "source":  "TCBS",
                })
            _hose_cache["top50"] = {"ts": now, "data": results}
            log.info(f"HOSE top50: TCBS OK — {len(price_map)} tickers")
            return results

        log.warning(f"TCBS returned empty price_map. Raw: {str(data)[:300]}")

    except Exception as e:
        log.warning(f"TCBS API error: {e} — falling back to Yahoo Finance")

    # ── Yahoo Finance fallback ───────────────
    try:
        sym_list = [f"{s}.VN" for s in HOSE_TOP50]
        async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=15) as client:
            tasks = [_fetch_yahoo_stock(client, s) for s in sym_list]
            yahoo_results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, (sym, res) in enumerate(zip(HOSE_TOP50, yahoo_results)):
            info = HOSE_INFO.get(sym, {"name": sym, "sector": "Khác"})
            p    = res if isinstance(res, dict) else {}
            results.append({
                "rank":   i + 1,
                "symbol": sym,
                "name":   info["name"],
                "sector": info["sector"],
                "price":  p.get("price",  0),
                "change": p.get("change", 0),
                "volume": p.get("volume", 0),
                "source": "Yahoo",
            })

        _hose_cache["top50"] = {"ts": now, "data": results}
        log.info("HOSE top50: Yahoo Finance fallback OK")
        return results

    except Exception as e:
        log.error(f"Yahoo fallback error: {e}")
        return JSONResponse(status_code=503, content={"error": f"Không lấy được dữ liệu HOSE: {e}"})

# ─────────────────────────────────────────────
# REST — TCBS HISTORICAL (1D, 1W, 1M, 1Q, 1Y)
# ─────────────────────────────────────────────

@app.get("/api/vn/history")
async def get_vn_history(symbol: str = "VCB", period: str = "1M"):
    """
    Lấy lịch sử giá 1 mã HOSE từ TCBS.
    period: 1D | 1W | 1M | 3M | 6M | 1Y | 3Y
    """
    period_map = {
        "1D": ("1", "day"),   "1W": ("5", "day"),
        "1M": ("1", "month"), "3M": ("3", "month"),
        "6M": ("6", "month"), "1Y": ("1", "year"),
        "3Y": ("3", "year"),
    }
    count, unit = period_map.get(period.upper(), ("1", "month"))
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://apipublic.tcbs.com.vn/stock-insight/v1/stock/bars-long-term",
                params={"ticker": symbol.upper(), "type": unit, "count": count},
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://tcinvest.tcbs.com.vn/"},
            )
        data = r.json()
        bars = data if isinstance(data, list) else data.get("data", [])
        return [{
            "time":   b.get("tradingDate") or b.get("date", ""),
            "open":   float(b.get("open", 0)),
            "high":   float(b.get("high", 0)),
            "low":    float(b.get("low", 0)),
            "close":  float(b.get("close", 0)),
            "volume": int(b.get("volume", 0)),
        } for b in bars]
    except Exception as e:
        return JSONResponse(status_code=503, content={"error": str(e)})

# ─────────────────────────────────────────────
# REST — MULTI-TF STRENGTH (HOSE)
# Tính % mã đang uptrend trên mỗi timeframe dựa vào MA
# ─────────────────────────────────────────────

_multitf_cache: dict = {}
MULTITF_TTL = 300  # 5 phút

@app.get("/api/vn/multitf")
async def get_multitf():
    """
    Tính % bullish cho từng timeframe bằng cách kiểm tra
    giá hiện tại > MA20 trên từng khung thời gian.
    Dùng Yahoo Finance với interval khác nhau.
    """
    now = time.time()
    cached = _multitf_cache.get("multitf")
    if cached and (now - cached["ts"]) < MULTITF_TTL:
        return cached["data"]

    # Lấy top 20 mã để tính (giới hạn để tránh quá nhiều request)
    sample = HOSE_TOP50[:20]
    timeframes = [
        {"key": "1H",  "interval": "60m",  "range": "5d",   "ma": 20},
        {"key": "4H",  "interval": "1h",   "range": "30d",  "ma": 20},
        {"key": "1D",  "interval": "1d",   "range": "90d",  "ma": 20},
        {"key": "1W",  "interval": "1wk",  "range": "2y",   "ma": 20},
        {"key": "1Q",  "interval": "3mo",  "range": "10y",  "ma": 4},
        {"key": "1Y",  "interval": "1mo",  "range": "20y",  "ma": 12},
    ]

    results = {}
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=15) as client:
        for tf in timeframes:
            bullish = 0
            total   = 0
            tasks   = [
                client.get(
                    f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}.VN",
                    params={"interval": tf["interval"], "range": tf["range"]},
                )
                for sym in sample
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            for resp in responses:
                try:
                    if isinstance(resp, Exception):
                        continue
                    closes = resp.json()["chart"]["result"][0]["indicators"]["quote"][0].get("close", [])
                    closes = [c for c in closes if c is not None]
                    if len(closes) < tf["ma"] + 1:
                        continue
                    ma = sum(closes[-tf["ma"]:]) / tf["ma"]
                    if closes[-1] > ma:
                        bullish += 1
                    total += 1
                except Exception:
                    continue
            pct = round(bullish / total * 100) if total > 0 else 50
            results[tf["key"]] = pct

    _multitf_cache["multitf"] = {"ts": now, "data": results}
    return results

# ─────────────────────────────────────────────
# REST — ECONOMIC CALENDAR
# ─────────────────────────────────────────────

@app.get("/api/calendar")
async def get_calendar():
    now = datetime.now(ICT)
    return [
        {"date": (now + timedelta(days=1)).strftime("%d/%m/%Y"), "time": "19:30", "event": "US CPI MoM",       "impact": "high",   "prev": "0.3%",  "forecast": "0.2%"},
        {"date": (now + timedelta(days=2)).strftime("%d/%m/%Y"), "time": "02:00", "event": "FED Rate Decision", "impact": "high",   "prev": "5.50%", "forecast": "5.50%"},
        {"date": (now + timedelta(days=3)).strftime("%d/%m/%Y"), "time": "08:00", "event": "BTC Options Expiry","impact": "medium", "prev": "$1.8B", "forecast": "$2.1B"},
        {"date": (now + timedelta(days=5)).strftime("%d/%m/%Y"), "time": "21:30", "event": "US NFP",            "impact": "high",   "prev": "175K",  "forecast": "180K"},
    ]

# ─────────────────────────────────────────────
# REST — LIQUIDATION DATA
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


# ─────────────────────────────────────────────
# REST — HOSE TOP 50
# ─────────────────────────────────────────────

@app.get("/api/vn/hose-top50")
async def get_hose_top50():
    now = time.time()
    cached = _hose_cache.get("top50")
    if cached and (now - cached["ts"]) < HOSE_TTL:
        return cached["data"]

    results = []

    # TCBS primary
    try:
        tickers_str = ",".join(HOSE_TOP50)
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                "https://apipublic.tcbs.com.vn/stock-insight/v1/stock/price",
                params={"tickers": tickers_str},
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://tcinvest.tcbs.com.vn/"},
            )
        data  = r.json()
        items = data if isinstance(data, list) else data.get("data", [])
        price_map = {}
        for item in items:
            ticker = (item.get("ticker") or item.get("symbol") or "").upper()
            if not ticker:
                continue
            price  = float(item.get("close") or item.get("price") or item.get("lastPrice") or 0)
            prev   = float(item.get("referencePrice") or item.get("prevClose") or item.get("ref") or 0)
            change = round((price - prev) / prev * 100, 2) if prev > 0 else 0
            volume = int(item.get("volume") or item.get("totalVolume") or 0)
            price_map[ticker] = {"price": price, "change": change, "volume": volume}

        if price_map:
            for i, sym in enumerate(HOSE_TOP50):
                info = HOSE_INFO.get(sym, {"name": sym, "sector": "Khác"})
                p    = price_map.get(sym, {})
                results.append({
                    "rank": i + 1, "symbol": sym,
                    "name": info["name"], "sector": info["sector"],
                    "price": p.get("price", 0), "change": p.get("change", 0),
                    "volume": p.get("volume", 0), "source": "TCBS",
                })
            _hose_cache["top50"] = {"ts": now, "data": results}
            return results

        log.warning(f"TCBS empty. Raw: {str(data)[:200]}")
    except Exception as e:
        log.warning(f"TCBS error: {e} — fallback Yahoo")

    # Yahoo Finance fallback
    try:
        sym_list = [f"{s}.VN" for s in HOSE_TOP50]
        async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=15) as client:
            yahoo_results = await asyncio.gather(
                *[_fetch_yahoo_stock(client, s) for s in sym_list],
                return_exceptions=True
            )
        for i, (sym, res) in enumerate(zip(HOSE_TOP50, yahoo_results)):
            info = HOSE_INFO.get(sym, {"name": sym, "sector": "Khác"})
            p    = res if isinstance(res, dict) else {}
            results.append({
                "rank": i + 1, "symbol": sym,
                "name": info["name"], "sector": info["sector"],
                "price": p.get("price", 0), "change": p.get("change", 0),
                "volume": p.get("volume", 0), "source": "Yahoo",
            })
        _hose_cache["top50"] = {"ts": now, "data": results}
        return results
    except Exception as e:
        return JSONResponse(status_code=503, content={"error": str(e)})


# ─────────────────────────────────────────────
# REST — TCBS HISTORICAL
# ─────────────────────────────────────────────

@app.get("/api/vn/history")
async def get_vn_history(symbol: str = "VCB", period: str = "1M"):
    period_map = {
        "1D": ("1", "day"),  "1W": ("5", "day"),
        "1M": ("1", "month"),"3M": ("3", "month"),
        "6M": ("6", "month"),"1Y": ("1", "year"),
        "3Y": ("3", "year"),
    }
    count, unit = period_map.get(period.upper(), ("1", "month"))
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://apipublic.tcbs.com.vn/stock-insight/v1/stock/bars-long-term",
                params={"ticker": symbol.upper(), "type": unit, "count": count},
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://tcinvest.tcbs.com.vn/"},
            )
        data = r.json()
        bars = data if isinstance(data, list) else data.get("data", [])
        return [{
            "time":   b.get("tradingDate") or b.get("date", ""),
            "open":   float(b.get("open",   0)),
            "high":   float(b.get("high",   0)),
            "low":    float(b.get("low",    0)),
            "close":  float(b.get("close",  0)),
            "volume": int(b.get("volume",   0)),
        } for b in bars]
    except Exception as e:
        return JSONResponse(status_code=503, content={"error": str(e)})


# ─────────────────────────────────────────────
# REST — MULTI-TF STRENGTH
# ─────────────────────────────────────────────

@app.get("/api/vn/multitf")
async def get_multitf():
    now = time.time()
    cached = _multitf_cache.get("multitf")
    if cached and (now - cached["ts"]) < MULTITF_TTL:
        return cached["data"]

    sample = HOSE_TOP50[:20]
    timeframes = [
        {"key": "1H",  "interval": "60m", "range": "5d",  "ma": 20},
        {"key": "4H",  "interval": "1h",  "range": "30d", "ma": 20},
        {"key": "1D",  "interval": "1d",  "range": "90d", "ma": 20},
        {"key": "1W",  "interval": "1wk", "range": "2y",  "ma": 20},
        {"key": "1Q",  "interval": "3mo", "range": "10y", "ma": 4},
        {"key": "1Y",  "interval": "1mo", "range": "20y", "ma": 12},
    ]

    results = {}
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=15) as client:
        for tf in timeframes:
            bullish, total = 0, 0
            responses = await asyncio.gather(*[
                client.get(
                    f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}.VN",
                    params={"interval": tf["interval"], "range": tf["range"]},
                )
                for sym in sample
            ], return_exceptions=True)

            for resp in responses:
                try:
                    if isinstance(resp, Exception):
                        continue
                    closes = resp.json()["chart"]["result"][0]["indicators"]["quote"][0].get("close", [])
                    closes = [c for c in closes if c is not None]
                    if len(closes) < tf["ma"] + 1:
                        continue
                    ma = sum(closes[-tf["ma"]:]) / tf["ma"]
                    if closes[-1] > ma:
                        bullish += 1
                    total += 1
                except Exception:
                    continue

            results[tf["key"]] = round(bullish / total * 100) if total > 0 else 50

    _multitf_cache["multitf"] = {"ts": now, "data": results}
    return results



@app.get("/health")
def health():
    return {
        "status":    "ok",
        "time":      datetime.now(ICT).isoformat(),
        "scheduler": alert_scheduler.running,
        "next_alert": str(alert_scheduler.get_job("alert_job").next_run_time),
    }

@app.get("/api/alert/test")
async def test_alert():
    msg = "✅ *Market Hub Alert Test*\nBot đang hoạt động bình thường!\n🕐 " + datetime.now(ICT).strftime("%H:%M %d/%m/%Y")
    await send_telegram_async(msg)
    return {"status": "sent", "message": msg}

@app.get("/api/alert/config")
def alert_config():
    return {
        "BTC": {"min": BTC_MIN, "max": BTC_MAX},
        "ETH": {"min": ETH_MIN, "max": ETH_MAX},
        "USD_VND": {"min": USD_MIN, "max": USD_MAX},
        "GOLD_SJC": {"min": GOLD_MIN, "max": GOLD_MAX},
        "change_pct_threshold": CHANGE_PCT,
        "alert_cooldown_sec": ALERT_COOLDOWN,
    }

@app.post("/api/alert/trigger-now")
async def trigger_alert_now():
    await job_alert()
    return {"status": "ok", "message": "Alert job executed"}

@app.get("/chat")
def chat_page():
    return FileResponse("static/chat.html")
