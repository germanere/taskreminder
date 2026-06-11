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
