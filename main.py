"""
Market Research Hub — Backend
==============================
- FastAPI server
- WebSocket proxy: OKX → client (Bybit/Binance bị geo-block trên Render US IP)
- REST APIs: OKX klines, CoinGecko, Yahoo Finance, TCBS, SJC, Vietcombank
- HOSE Top 250 endpoint (TCBS + Yahoo fallback, retry cho lỗi DNS/geo-block)
- Telegram alerts: BTC, ETH, USD/VND, Gold (SJC)
- Serve static files
"""

import os, re, json, logging, asyncio, time, socket
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
import gspread
from google.oauth2.service_account import Credentials

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
# GOOGLE SHEETS — Chat Log Storage
# ─────────────────────────────────────────────

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
CHAT_LOG_SHEET_ID = "12W6K3Y3-Ac2tCE1B8JBOC-ZYAB09QRdFJn-Yv1mmk3w"
CHAT_LOG_SHEET_NAME = "Logs"

_gsheet_client = None
_chat_log_worksheet = None

def _get_chat_log_worksheet():
    """
    Lazy-init gspread client + worksheet.
    Trả về None nếu chưa cấu hình credentials hoặc lỗi kết nối.
    """
    global _gsheet_client, _chat_log_worksheet
    if _chat_log_worksheet is not None:
        return _chat_log_worksheet

    if not GOOGLE_CREDENTIALS_JSON:
        log.warning("GOOGLE_CREDENTIALS_JSON chưa cấu hình — bỏ qua chat log")
        return None

    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        _gsheet_client = gspread.authorize(creds)

        sheet = _gsheet_client.open_by_key(CHAT_LOG_SHEET_ID)
        try:
            ws = sheet.worksheet(CHAT_LOG_SHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sheet.add_worksheet(title=CHAT_LOG_SHEET_NAME, rows=1000, cols=5)
            ws.append_row(["Timestamp", "Session", "Role", "Message"])

        _chat_log_worksheet = ws
        log.info("Chat log: kết nối Google Sheets OK")
        return ws
    except Exception as e:
        log.error(f"Chat log: lỗi kết nối Google Sheets: {e}")
        return None


def log_chat_message(session_id: str, role: str, message: str):
    """
    Ghi 1 dòng log vào Google Sheets. Chạy non-blocking trong background task.
    Lỗi không được làm crash chat response.
    """
    try:
        ws = _get_chat_log_worksheet()
        if ws is None:
            return
        timestamp = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([timestamp, session_id, role, message])
    except Exception as e:
        log.error(f"Chat log write error: {e}")



# 250 mã HOSE vốn hóa lớn nhất + bổ sung (tháng 7/2026)
HOSE_TOP200 = [
    "VCB","BID","VIC","VHM","CTG","GAS","VNM","SAB","MSN","TCB",
    "MBB","FPT","ACB","PLX","HPG","VPB","STB","HDB","GVR","POW",
    "MWG","PNJ","REE","SSI","VND","HCM","DPM","DCM","VEA","KDH",
    "NVL","PDR","DXG","PVD","HSG","NKG","PHR","DRC","IDC","KBC",
    "NTC","LHG","EIB","EVF","CMG","VGI","FRT","DGW","GEX","VRE",
    # 50 mã bổ sung (51-100)
    "BVH","BCM","PC1","PVT","BSR","BMI","DGC","CTD","HDG","HAH",
    "ANV","VHC","DBC","NLG","CII","TCH","HHV","VCG","HT1","PAN",
    "VOS","VTP","VCI","SHB","TPB","OCB","MSB","LPB","BAB","NAB",
    "TLG","SCS","ASM","CTS","FTS","PVS","PVC","TIS","NT2","VSH",
    "BWE","DPR","HAG","HNG","DHC","SBT","SZC","DIG","ITA","TDM",
    # 50 mã bổ sung (101-150)
    "AAA","APH","BFC","BCG","BHN","CAV","CKG","CLL","CMX","CRE",
    "DAH","DBD","DHA","DPG","ELC","EVE","FCN","FIT","FTM","GEG",
    "GIL","GMD","HBC","HCD","HII","HQC","HU1","HVH","IJC","IMP",
    "ITC","KSB","LCG","LDG","LSS","MCP","NHA","NHH","NTL","OGC",
    "PDN","PGD","PGI","PHC","PIT","PLP","PMG","PTB","QCG","RAL",
    # 50 mã bổ sung (151-200)
    "SAM","SBA","SCD","SFG","SGN","SGT","SHA","SHI","SJD","SJS",
    "SMA","SMB","SMC","SRC","SRF","SVC","SVI","TCM","TDC","TDH",
    "TDP","TEG","THG","TLH","TNA","TNI","TNT","TPC","TRA","TSC",
    "TTF","TV2","TVS","UDC","VCF","VDS","VFG","VID","VIP","VIX",
    "VNE","VNG","VPG","VPI","VSC","VTO","YEG","BMP","DXS","NAF",
    # 50 mã bổ sung (201-250)
    "VJC","HVN","VGC","DHG","DBT","PPC","NBB","ABT","ACL","BBC",
    "BTP","C32","CDC","CIG","CLC","COM","CTI","D2D","DAG","DRH",
    "DTL","EVG","FIR","GDT","HAP","HDC","HRC","HTN","ICF","IDI",
    "ILB","JVC","KHP","LAF","LGC","LIX","MHC","NNC","PET","PGC",
    "QNS","RDP","SAV","SC5","SCR","SFC","SFI","SGR","SKG","STK",
]
HOSE_TOP100 = HOSE_TOP200  # alias để tương thích code cũ
HOSE_TOP50  = HOSE_TOP200  # alias để tương thích code cũ


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
    # 50 mã bổ sung
    "BVH":  {"name": "Bảo Việt",           "sector": "Bảo hiểm"},
    "BCM":  {"name": "Becamex IDC",        "sector": "Bất động sản"},
    "PC1":  {"name": "PC1 Group",          "sector": "Công nghiệp"},
    "PVT":  {"name": "PV Trans",           "sector": "Năng lượng"},
    "BSR":  {"name": "Bình Sơn Refinery",  "sector": "Năng lượng"},
    "BMI":  {"name": "Bảo Minh",           "sector": "Bảo hiểm"},
    "DGC":  {"name": "Hóa chất Đức Giang", "sector": "Hóa chất"},
    "CTD":  {"name": "Coteccons",          "sector": "Công nghiệp"},
    "HDG":  {"name": "Hà Đô Group",        "sector": "Bất động sản"},
    "HAH":  {"name": "Hải An Transport",   "sector": "Công nghiệp"},
    "ANV":  {"name": "Nam Việt",           "sector": "Tiêu dùng"},
    "VHC":  {"name": "Vĩnh Hoàn",          "sector": "Tiêu dùng"},
    "DBC":  {"name": "Dabaco",             "sector": "Tiêu dùng"},
    "NLG":  {"name": "Nam Long Group",     "sector": "Bất động sản"},
    "CII":  {"name": "CII",                "sector": "Bất động sản"},
    "TCH":  {"name": "Hòa Phát Hospitality","sector": "Bất động sản"},
    "HHV":  {"name": "Đèo Cả Group",       "sector": "Công nghiệp"},
    "VCG":  {"name": "Vinaconex",          "sector": "Công nghiệp"},
    "HT1":  {"name": "Xi măng Hà Tiên 1",  "sector": "Vật liệu"},
    "PAN":  {"name": "PAN Group",          "sector": "Tiêu dùng"},
    "VOS":  {"name": "Vosco",              "sector": "Công nghiệp"},
    "VTP":  {"name": "Viettel Post",       "sector": "Công nghiệp"},
    "VCI":  {"name": "VietCap Securities", "sector": "Chứng khoán"},
    "SHB":  {"name": "SHB",                "sector": "Ngân hàng"},
    "TPB":  {"name": "TPBank",             "sector": "Ngân hàng"},
    "OCB":  {"name": "OCB",                "sector": "Ngân hàng"},
    "MSB":  {"name": "MSB",                "sector": "Ngân hàng"},
    "LPB":  {"name": "LPBank",             "sector": "Ngân hàng"},
    "BAB":  {"name": "Bắc Á Bank",         "sector": "Ngân hàng"},
    "NAB":  {"name": "Nam A Bank",         "sector": "Ngân hàng"},
    "TLG":  {"name": "Thiên Long Group",   "sector": "Tiêu dùng"},
    "SCS":  {"name": "SCSC",               "sector": "Công nghiệp"},
    "ASM":  {"name": "Sao Mai Group",      "sector": "Bất động sản"},
    "CTS":  {"name": "VietinBank Securities","sector": "Chứng khoán"},
    "FTS":  {"name": "FPT Securities",     "sector": "Chứng khoán"},
    "PVS":  {"name": "PV Service",         "sector": "Năng lượng"},
    "PVC":  {"name": "PV Coating",         "sector": "Năng lượng"},
    "TIS":  {"name": "Gang Thép Thái Nguyên","sector": "Vật liệu"},
    "NT2":  {"name": "Nhơn Trạch 2 Power", "sector": "Năng lượng"},
    "VSH":  {"name": "Vĩnh Sơn-Sông Hinh", "sector": "Năng lượng"},
    "BWE":  {"name": "BIWASE",             "sector": "Công nghiệp"},
    "DPR":  {"name": "Cao su Đồng Phú",    "sector": "Vật liệu"},
    "HAG":  {"name": "Hoàng Anh Gia Lai",  "sector": "Tiêu dùng"},
    "HNG":  {"name": "HAGL Agrico",        "sector": "Tiêu dùng"},
    "DHC":  {"name": "Đông Hải Bến Tre",   "sector": "Vật liệu"},
    "SBT":  {"name": "TTC Sugar",          "sector": "Tiêu dùng"},
    "SZC":  {"name": "Sonadezi Châu Đức",  "sector": "Bất động sản"},
    "DIG":  {"name": "DIC Corp",           "sector": "Bất động sản"},
    "ITA":  {"name": "Tân Tạo Group",      "sector": "Bất động sản"},
    "TDM":  {"name": "Thủ Dầu Một Water",  "sector": "Công nghiệp"},
    # 101-150
    "AAA":  {"name": "An Phát Holdings",   "sector": "Vật liệu"},
    "APH":  {"name": "An Phát Plastic",    "sector": "Vật liệu"},
    "BFC":  {"name": "Phân bón Bình Điền", "sector": "Hóa chất"},
    "BCG":  {"name": "Bamboo Capital",     "sector": "Bất động sản"},
    "BHN":  {"name": "Habeco",             "sector": "Tiêu dùng"},
    "CAV":  {"name": "Dây cáp điện CADIVI","sector": "Công nghiệp"},
    "CKG":  {"name": "Cảng Kiên Giang",    "sector": "Bất động sản"},
    "CLL":  {"name": "Cảng Cát Lái",       "sector": "Công nghiệp"},
    "CMX":  {"name": "Camimex Group",      "sector": "Tiêu dùng"},
    "CRE":  {"name": "Cen Land",           "sector": "Bất động sản"},
    "DAH":  {"name": "Tập đoàn Khách sạn Đông Á","sector": "Tiêu dùng"},
    "DBD":  {"name": "Dược Bidiphar",      "sector": "Y tế"},
    "DHA":  {"name": "Hóa An",             "sector": "Vật liệu"},
    "DPG":  {"name": "Đạt Phương Group",   "sector": "Công nghiệp"},
    "ELC":  {"name": "Elcom",              "sector": "Công nghệ"},
    "EVE":  {"name": "Everpia",            "sector": "Tiêu dùng"},
    "FCN":  {"name": "FECON",              "sector": "Công nghiệp"},
    "FIT":  {"name": "FIT Group",          "sector": "Công nghiệp"},
    "FTM":  {"name": "Đầu tư Phát triển TDT","sector": "Tiêu dùng"},
    "GEG":  {"name": "Gia Lai Electricity","sector": "Năng lượng"},
    "GIL":  {"name": "Bình Thạnh Garment", "sector": "Tiêu dùng"},
    "GMD":  {"name": "Gemadept",           "sector": "Công nghiệp"},
    "HBC":  {"name": "Hòa Bình Construction","sector": "Công nghiệp"},
    "HCD":  {"name": "Hòa Cường",          "sector": "Vật liệu"},
    "HII":  {"name": "An Tiến Industries", "sector": "Vật liệu"},
    "HQC":  {"name": "Hoàng Quân Group",   "sector": "Bất động sản"},
    "HU1":  {"name": "Đầu tư & Phát triển nhà HUD1","sector": "Bất động sản"},
    "HVH":  {"name": "Hồ Việt Holdings",   "sector": "Tiêu dùng"},
    "IJC":  {"name": "Becamex IJC",        "sector": "Bất động sản"},
    "IMP":  {"name": "Imexpharm",          "sector": "Y tế"},
    "ITC":  {"name": "Đầu tư & Kinh doanh Nhà",   "sector": "Bất động sản"},
    "KSB":  {"name": "Khoáng sản Bình Dương","sector": "Vật liệu"},
    "LCG":  {"name": "Licogi 16",          "sector": "Công nghiệp"},
    "LDG":  {"name": "LDG Group",          "sector": "Bất động sản"},
    "LSS":  {"name": "Mía đường Lam Sơn",  "sector": "Tiêu dùng"},
    "MCP":  {"name": "In & Bao bì Mỹ Châu","sector": "Vật liệu"},
    "NHA":  {"name": "Đầu tư Phát triển Nhà & Đô thị Nam Hà Nội","sector": "Bất động sản"},
    "NHH":  {"name": "Nhựa Hà Nội",        "sector": "Vật liệu"},
    "NTL":  {"name": "Đô thị Từ Liêm",     "sector": "Bất động sản"},
    "OGC":  {"name": "Đại Dương Group",    "sector": "Bất động sản"},
    "PDN":  {"name": "Cảng Đồng Nai",      "sector": "Công nghiệp"},
    "PGD":  {"name": "PV Gas City",        "sector": "Năng lượng"},
    "PGI":  {"name": "Bảo hiểm Petrolimex","sector": "Bảo hiểm"},
    "PHC":  {"name": "Xây dựng Phục Hưng Holdings","sector": "Công nghiệp"},
    "PIT":  {"name": "Xuất nhập khẩu Phú Yên","sector": "Tiêu dùng"},
    "PLP":  {"name": "Bao bì Dầu thực vật","sector": "Vật liệu"},
    "PMG":  {"name": "Đầu tư Khí Mê Kông", "sector": "Năng lượng"},
    "PTB":  {"name": "Phú Tài",            "sector": "Vật liệu"},
    "QCG":  {"name": "Quốc Cường Gia Lai", "sector": "Bất động sản"},
    "RAL":  {"name": "Rạng Đông",          "sector": "Công nghiệp"},
    # 151-200
    "SAM":  {"name": "SAM Holdings",       "sector": "Công nghiệp"},
    "SBA":  {"name": "Sông Ba Hydropower", "sector": "Năng lượng"},
    "SCD":  {"name": "Nước giải khát Chương Dương","sector": "Tiêu dùng"},
    "SFG":  {"name": "Phân bón Miền Nam",  "sector": "Hóa chất"},
    "SGN":  {"name": "Phục vụ mặt đất Sài Gòn","sector": "Công nghiệp"},
    "SGT":  {"name": "Công nghệ Viễn thông Sài Gòn","sector": "Công nghệ"},
    "SHA":  {"name": "Sơn Hà SHI Group",   "sector": "Vật liệu"},
    "SHI":  {"name": "Quốc tế Sơn Hà",     "sector": "Vật liệu"},
    "SJD":  {"name": "Thủy điện Cần Đơn",  "sector": "Năng lượng"},
    "SJS":  {"name": "Sudico",             "sector": "Bất động sản"},
    "SMA":  {"name": "Thiết bị Phụ tùng Sài Gòn","sector": "Công nghiệp"},
    "SMB":  {"name": "Bia Sài Gòn Miền Trung","sector": "Tiêu dùng"},
    "SMC":  {"name": "Đầu tư Thương mại SMC","sector": "Vật liệu"},
    "SRC":  {"name": "Cao su Sao Vàng",    "sector": "Vật liệu"},
    "SRF":  {"name": "Kỹ nghệ lạnh SEAREFICO","sector": "Công nghiệp"},
    "SVC":  {"name": "Savico",             "sector": "Tiêu dùng"},
    "SVI":  {"name": "Bao bì Biên Hòa",    "sector": "Vật liệu"},
    "TCM":  {"name": "Dệt may - Đầu tư - Thương mại Thành Công","sector": "Tiêu dùng"},
    "TDC":  {"name": "Kinh doanh & Phát triển Bình Dương","sector": "Bất động sản"},
    "TDH":  {"name": "Thuduc House",       "sector": "Bất động sản"},
    "TDP":  {"name": "Thuận Đức",          "sector": "Vật liệu"},
    "TEG":  {"name": "Trường Tiền Group",  "sector": "Công nghiệp"},
    "THG":  {"name": "Tiền Giang",         "sector": "Công nghiệp"},
    "TLH":  {"name": "Thép Tiến Lên",      "sector": "Vật liệu"},
    "TNA":  {"name": "Thương mại Xuất nhập khẩu Thiên Nam","sector": "Tiêu dùng"},
    "TNI":  {"name": "Tập đoàn Thành Nam", "sector": "Vật liệu"},
    "TNT":  {"name": "Tài Nguyên",         "sector": "Bất động sản"},
    "TPC":  {"name": "Nhựa Tân Đại Hưng",  "sector": "Vật liệu"},
    "TRA":  {"name": "Traphaco",           "sector": "Y tế"},
    "TSC":  {"name": "Vật tư Kỹ thuật Nông nghiệp Cần Thơ","sector": "Tiêu dùng"},
    "TTF":  {"name": "Gỗ Trường Thành",    "sector": "Vật liệu"},
    "TV2":  {"name": "Tư vấn Xây dựng Điện 2","sector": "Công nghiệp"},
    "TVS":  {"name": "Chứng khoán Thiên Việt","sector": "Chứng khoán"},
    "UDC":  {"name": "Xây dựng & Phát triển Đô thị Bà Rịa","sector": "Bất động sản"},
    "VCF":  {"name": "Vinacafé Biên Hòa",  "sector": "Tiêu dùng"},
    "VDS":  {"name": "Chứng khoán Rồng Việt","sector": "Chứng khoán"},
    "VFG":  {"name": "Khử trùng Việt Nam VFG","sector": "Hóa chất"},
    "VID":  {"name": "Đầu tư & Phát triển Thương mại Viễn Đông","sector": "Vật liệu"},
    "VIP":  {"name": "Vận tải Xăng dầu VIPCO","sector": "Công nghiệp"},
    "VIX":  {"name": "Chứng khoán VIX",    "sector": "Chứng khoán"},
    "VNE":  {"name": "Tổng CTCP Xây dựng Điện Việt Nam","sector": "Công nghiệp"},
    "VNG":  {"name": "Du lịch Việt Nam VNG","sector": "Tiêu dùng"},
    "VPG":  {"name": "Đầu tư Thương mại Xuất nhập khẩu Việt Phát","sector": "Vật liệu"},
    "VPI":  {"name": "Đầu tư Văn Phú - Invest","sector": "Bất động sản"},
    "VSC":  {"name": "Container Việt Nam VSC","sector": "Công nghiệp"},
    "VTO":  {"name": "Vận tải Xăng dầu VITACO","sector": "Công nghiệp"},
    "YEG":  {"name": "Yeah1 Group",        "sector": "Công nghệ"},
    "BMP":  {"name": "Nhựa Bình Minh",     "sector": "Vật liệu"},
    "DXS":  {"name": "Đất Xanh Services",  "sector": "Bất động sản"},
    "NAF":  {"name": "Nafoods Group",      "sector": "Tiêu dùng"},
    # 201-250
    "VJC":  {"name": "Vietjet Air",              "sector": "Công nghiệp"},
    "HVN":  {"name": "Vietnam Airlines",         "sector": "Công nghiệp"},
    "VGC":  {"name": "Viglacera",                "sector": "Vật liệu"},
    "DHG":  {"name": "Dược Hậu Giang",           "sector": "Y tế"},
    "DBT":  {"name": "Dược phẩm Bến Tre",        "sector": "Y tế"},
    "PPC":  {"name": "Nhiệt điện Phả Lại",       "sector": "Năng lượng"},
    "NBB":  {"name": "Năm Bảy Bảy",              "sector": "Bất động sản"},
    "ABT":  {"name": "XNK Thủy sản Bến Tre",     "sector": "Tiêu dùng"},
    "ACL":  {"name": "XNK Thủy sản Cửu Long An Giang","sector": "Tiêu dùng"},
    "BBC":  {"name": "Bibica",                   "sector": "Tiêu dùng"},
    "BTP":  {"name": "Nhiệt điện Bà Rịa",        "sector": "Năng lượng"},
    "C32":  {"name": "Xây dựng Số 32",           "sector": "Công nghiệp"},
    "CDC":  {"name": "Chương Dương",             "sector": "Bất động sản"},
    "CIG":  {"name": "COMA18",                   "sector": "Bất động sản"},
    "CLC":  {"name": "Cát Lợi",                  "sector": "Vật liệu"},
    "COM":  {"name": "Vật tư Xăng dầu COMECO",   "sector": "Năng lượng"},
    "CTI":  {"name": "Cường Thuận IDICO",        "sector": "Công nghiệp"},
    "D2D":  {"name": "PT Đô thị Công nghiệp Số 2","sector": "Bất động sản"},
    "DAG":  {"name": "Tập đoàn Nhựa Đông Á",     "sector": "Vật liệu"},
    "DRH":  {"name": "DRH Holdings",             "sector": "Bất động sản"},
    "DTL":  {"name": "Đại Thiên Lộc",            "sector": "Vật liệu"},
    "EVG":  {"name": "Everland",                 "sector": "Bất động sản"},
    "FIR":  {"name": "Địa ốc First Real",        "sector": "Bất động sản"},
    "GDT":  {"name": "Chế biến Gỗ Đức Thành",    "sector": "Vật liệu"},
    "HAP":  {"name": "Tập đoàn Hapaco",          "sector": "Vật liệu"},
    "HDC":  {"name": "PT Nhà Bà Rịa - Vũng Tàu", "sector": "Bất động sản"},
    "HRC":  {"name": "Cao su Hòa Bình",          "sector": "Vật liệu"},
    "HTN":  {"name": "Hưng Thịnh Incons",        "sector": "Công nghiệp"},
    "ICF":  {"name": "ĐT Thương mại Thủy sản",   "sector": "Tiêu dùng"},
    "IDI":  {"name": "ĐT & PT Đa Quốc gia IDI",  "sector": "Tiêu dùng"},
    "ILB":  {"name": "Tân Cảng Long Bình",       "sector": "Công nghiệp"},
    "JVC":  {"name": "Thiết bị Y tế Việt Nhật",  "sector": "Y tế"},
    "KHP":  {"name": "Điện lực Khánh Hòa",       "sector": "Năng lượng"},
    "LAF":  {"name": "Chế biến Hàng XK Long An", "sector": "Tiêu dùng"},
    "LGC":  {"name": "Đầu tư Cầu đường CII",     "sector": "Công nghiệp"},
    "LIX":  {"name": "Bột giặt Lix",             "sector": "Tiêu dùng"},
    "MHC":  {"name": "MHC Group",                "sector": "Công nghiệp"},
    "NNC":  {"name": "Đá Núi Nhỏ",               "sector": "Vật liệu"},
    "PET":  {"name": "Dịch vụ Tổng hợp Dầu khí", "sector": "Công nghiệp"},
    "PGC":  {"name": "Gas Petrolimex",           "sector": "Năng lượng"},
    "QNS":  {"name": "Đường Quảng Ngãi",         "sector": "Tiêu dùng"},
    "RDP":  {"name": "Nhựa Rạng Đông",           "sector": "Vật liệu"},
    "SAV":  {"name": "Savimex",                  "sector": "Vật liệu"},
    "SC5":  {"name": "Xây dựng Số 5",            "sector": "Công nghiệp"},
    "SCR":  {"name": "Địa ốc Sài Gòn Thương Tín","sector": "Bất động sản"},
    "SFC":  {"name": "Nhiên liệu Sài Gòn",       "sector": "Năng lượng"},
    "SFI":  {"name": "Đại lý Vận tải SAFI",      "sector": "Công nghiệp"},
    "SGR":  {"name": "Địa ốc Sài Gòn",           "sector": "Bất động sản"},
    "SKG":  {"name": "Superdong Kiên Giang",     "sector": "Công nghiệp"},
    "STK":  {"name": "Sợi Thế Kỷ",               "sector": "Vật liệu"},
}


_hose_cache: dict = {}
HOSE_TTL = 60

_multitf_cache: dict = {}
MULTITF_TTL = 300

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
# PRICE FETCH HELPERS (OKX primary, Binance fallback)
# ─────────────────────────────────────────────

async def fetch_price(symbol_okx: str, symbol_binance: str) -> float:
    """
    symbol_okx ví dụ: 'BTC-USDT'
    """
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(
                "https://www.okx.com/api/v5/market/ticker",
                params={"instId": symbol_okx},
            )
        data = r.json()
        lst  = data.get("data", [])
        if lst:
            price = float(lst[0]["last"])
            if price > 0:
                return price
    except Exception as e:
        log.warning(f"OKX price error ({symbol_okx}): {e}")

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

    raise ValueError(f"Cannot fetch price for {symbol_okx}/{symbol_binance}")

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
        btc = await fetch_price("BTC-USDT", "BTCUSDT")
        all_alerts += check_alert("BTC", btc, BTC_MIN, BTC_MAX, "BTC/USDT", "$")
    except Exception as e:
        log.error(f"BTC alert error: {e}")

    try:
        eth = await fetch_price("ETH-USDT", "ETHUSDT")
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
# OKX SYMBOL / INTERVAL HELPERS
# ─────────────────────────────────────────────

def to_okx_symbol(symbol: str) -> str:
    """
    Chuyển 'BTCUSDT' -> 'BTC-USDT'
    """
    s = symbol.upper()
    if "-" in s:
        return s
    for quote in ("USDT", "USDC", "BTC", "ETH"):
        if s.endswith(quote) and len(s) > len(quote):
            return f"{s[:-len(quote)]}-{quote}"
    return s

OKX_INTERVAL_MAP = {
    "1m": "1m",  "3m": "3m",  "5m": "5m",  "15m": "15m", "30m": "30m",
    "1h": "1H",  "2h": "2H",  "4h": "4H",  "6h": "6H",   "12h": "12H",
    "1d": "1D",  "1w": "1W",  "1M": "1M",
}

# ─────────────────────────────────────────────
# WEBSOCKET — OKX KLINE
# ─────────────────────────────────────────────

@app.websocket("/ws/kline")
async def ws_kline(ws: WebSocket, symbol: str = "btcusdt", interval: str = "1h"):
    await ws.accept()
    okx_symbol   = to_okx_symbol(symbol)
    okx_bar      = OKX_INTERVAL_MAP.get(interval, "1H")
    okx_url      = "wss://ws.okx.com:8443/ws/v5/business"
    subscribe_msg = json.dumps({
        "op": "subscribe",
        "args": [{"channel": f"candle{okx_bar}", "instId": okx_symbol}],
    })
    RECONNECT_DELAY = 3

    for attempt in range(10):
        try:
            async with websockets.connect(okx_url, ping_interval=20, ping_timeout=10) as okx_ws:
                await okx_ws.send(subscribe_msg)
                log.info(f"OKX kline connected: {okx_symbol} {okx_bar}")
                while True:
                    try:
                        msg = await asyncio.wait_for(okx_ws.recv(), timeout=35)
                    except asyncio.TimeoutError:
                        await ws.send_json({"ping": True})
                        try:
                            await okx_ws.send("ping")
                        except Exception:
                            pass
                        continue

                    if msg == "pong":
                        continue

                    data = json.loads(msg)
                    if "data" not in data:
                        continue

                    for k in data["data"]:
                        await ws.send_json({
                            "time":      int(k[0]) // 1000,
                            "open":      float(k[1]),
                            "high":      float(k[2]),
                            "low":       float(k[3]),
                            "close":     float(k[4]),
                            "volume":    float(k[5]),
                            "is_closed": k[8] == "1" if len(k) > 8 else False,
                        })
        except WebSocketDisconnect:
            return
        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"OKX kline closed (attempt {attempt+1}): {e}")
        except Exception as e:
            log.error(f"OKX kline error (attempt {attempt+1}): {e}")

        try:
            await ws.send_json({"reconnecting": True, "attempt": attempt + 1})
        except Exception:
            return
        await asyncio.sleep(RECONNECT_DELAY)
        RECONNECT_DELAY = min(RECONNECT_DELAY * 2, 60)

# ─────────────────────────────────────────────
# WEBSOCKET — OKX ORDERBOOK
# ─────────────────────────────────────────────

@app.websocket("/ws/orderbook")
async def ws_orderbook(ws: WebSocket, symbol: str = "btcusdt"):
    await ws.accept()
    okx_symbol  = to_okx_symbol(symbol)
    okx_url     = "wss://ws.okx.com:8443/ws/v5/public"
    subscribe_msg = json.dumps({
        "op": "subscribe",
        "args": [{"channel": "books5", "instId": okx_symbol}],
    })
    RECONNECT_DELAY = 3

    for attempt in range(10):
        try:
            async with websockets.connect(okx_url, ping_interval=20, ping_timeout=10) as okx_ws:
                await okx_ws.send(subscribe_msg)
                log.info(f"OKX orderbook connected: {okx_symbol}")
                while True:
                    try:
                        msg = await asyncio.wait_for(okx_ws.recv(), timeout=35)
                    except asyncio.TimeoutError:
                        try:
                            await okx_ws.send("ping")
                        except Exception:
                            pass
                        continue

                    if msg == "pong":
                        continue

                    data = json.loads(msg)
                    if "data" not in data:
                        continue

                    for book in data["data"]:
                        bids = book.get("bids", [])[:10]
                        asks = book.get("asks", [])[:10]
                        await ws.send_json({
                            "bids": [[float(p), float(q)] for p, q, *_ in bids],
                            "asks": [[float(p), float(q)] for p, q, *_ in asks],
                        })
        except WebSocketDisconnect:
            return
        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"OKX orderbook closed (attempt {attempt+1}): {e}")
        except Exception as e:
            log.error(f"OKX orderbook error (attempt {attempt+1}): {e}")

        try:
            await ws.send_json({"reconnecting": True, "attempt": attempt + 1})
        except Exception:
            return
        await asyncio.sleep(RECONNECT_DELAY)
        RECONNECT_DELAY = min(RECONNECT_DELAY * 2, 60)

# ─────────────────────────────────────────────
# REST — HISTORICAL KLINES (OKX primary, Binance fallback)
# ─────────────────────────────────────────────

@app.get("/api/klines")
async def get_klines(symbol: str = "BTCUSDT", interval: str = "1h", limit: int = 200):
    okx_symbol = to_okx_symbol(symbol)
    okx_bar    = OKX_INTERVAL_MAP.get(interval, "1H")

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://www.okx.com/api/v5/market/candles",
                params={"instId": okx_symbol, "bar": okx_bar, "limit": min(limit, 300)},
            )
        result = r.json()
        if result.get("code") == "0":
            raw = result.get("data", [])
            if isinstance(raw, list) and len(raw) > 0:
                raw = raw[::-1]
                log.info(f"OKX kline OK: {okx_symbol} {okx_bar} ({len(raw)} bars)")
                return [{
                    "time":   int(k[0]) // 1000,
                    "open":   float(k[1]),
                    "high":   float(k[2]),
                    "low":    float(k[3]),
                    "close":  float(k[4]),
                    "volume": float(k[5]),
                } for k in raw]
        log.warning(f"OKX kline non-zero code: {result.get('msg')}")
    except Exception as e:
        log.warning(f"OKX kline REST error: {e}")

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}&interval={interval}&limit={limit}"
            )
        if r.status_code == 451:
            return JSONResponse(status_code=503, content={"error": "OKX failed, Binance geo-blocked"})
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
# REST — HOSE TOP 250 (TCBS primary + retry, Yahoo Finance fallback)
# ─────────────────────────────────────────────

async def _fetch_tcbs_batch(client: httpx.AsyncClient, batch: list[str], max_retries: int = 2) -> list:
    """Gọi TCBS cho 1 batch mã, retry ngắn khi gặp lỗi DNS/network."""
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = await client.get(
                "https://apipublic.tcbs.com.vn/stock-insight/v1/stock/price",
                params={"tickers": ",".join(batch)},
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://tcinvest.tcbs.com.vn/"},
            )
            data = r.json()
            items = data if isinstance(data, list) else data.get("data", [])
            if items:
                return items
            log.warning(f"TCBS batch {batch[0]}..{batch[-1]} trả rỗng (attempt {attempt})")
        except (httpx.ConnectError, OSError, socket.gaierror) as e:
            last_err = e
            wait = 1.5 * attempt
            log.warning(f"TCBS batch {batch[0]}..{batch[-1]} lỗi DNS/network (attempt {attempt}/{max_retries}): {e} — retry sau {wait}s")
            await asyncio.sleep(wait)
        except Exception as e:
            last_err = e
            log.warning(f"TCBS batch {batch[0]}..{batch[-1]} lỗi khác (attempt {attempt}/{max_retries}): {e}")
            await asyncio.sleep(1)

    if last_err:
        log.error(f"TCBS batch {batch[0]}..{batch[-1]} thất bại sau {max_retries} lần thử: {last_err}")
    return []


async def _yahoo_fallback_fill(missing_symbols: list[str], price_map: dict):
    """
    Với các mã TCBS không trả được giá, thử lấy qua Yahoo Finance
    (Yahoo không bị geo-block trên Render, đã dùng ổn định ở /api/vn/stocks).
    """
    if not missing_symbols:
        return
    CHUNK = 30
    try:
        async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=15) as client:
            for i in range(0, len(missing_symbols), CHUNK):
                chunk = missing_symbols[i:i + CHUNK]
                yahoo_syms = [f"{s}.VN" for s in chunk]
                res = await asyncio.gather(
                    *[_fetch_yahoo_stock(client, s) for s in yahoo_syms],
                    return_exceptions=True,
                )
                for sym, r in zip(chunk, res):
                    if isinstance(r, dict) and r.get("price", 0) > 0:
                        price_map[sym] = {
                            "price": r["price"], "change": r["change"],
                            "volume": r["volume"], "source": "Yahoo",
                        }
                if i + CHUNK < len(missing_symbols):
                    await asyncio.sleep(0.4)
    except Exception as e:
        log.warning(f"Yahoo fallback error: {e}")


@app.get("/api/vn/hose-top50")
async def get_hose_top50():
    """
    Trả về toàn bộ HOSE_TOP200 (250 mã, hardcode) kèm giá thời gian thực.
    Nguồn chính: TCBS (batch 50 mã/lượt, retry khi lỗi DNS/network).
    Nguồn dự phòng: Yahoo Finance cho các mã TCBS không trả được giá
    (phòng trường hợp TCBS bị chặn/không phản hồi từ server hosting).
    Endpoint name giữ "hose-top50" để tương thích frontend cũ.
    """
    now = time.time()
    cached = _hose_cache.get("top250")
    if cached and (now - cached["ts"]) < HOSE_TTL:
        return cached["data"]

    symbols = HOSE_TOP200
    price_map = {}
    BATCH = 50

    try:
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        async with httpx.AsyncClient(timeout=25, limits=limits) as client:
            for i in range(0, len(symbols), BATCH):
                batch = symbols[i:i+BATCH]
                items = await _fetch_tcbs_batch(client, batch)
                for item in items:
                    ticker = (item.get("ticker") or item.get("symbol") or "").upper()
                    if not ticker:
                        continue
                    price  = float(item.get("close") or item.get("price") or item.get("lastPrice") or 0)
                    prev   = float(item.get("referencePrice") or item.get("prevClose") or item.get("ref") or 0)
                    change = round((price - prev) / prev * 100, 2) if prev > 0 else 0
                    volume = int(item.get("volume") or item.get("totalVolume") or 0)
                    if price > 0:
                        price_map[ticker] = {"price": price, "change": change, "volume": volume, "source": "TCBS"}
                if i + BATCH < len(symbols):
                    await asyncio.sleep(0.5)

        # Fallback Yahoo cho các mã TCBS chưa có giá
        missing = [s for s in symbols if s not in price_map]
        if missing:
            log.info(f"TCBS thiếu {len(missing)} mã — fallback sang Yahoo Finance")
            await _yahoo_fallback_fill(missing, price_map)

        results = []
        for i, sym in enumerate(symbols):
            info = HOSE_INFO.get(sym, {"name": sym, "sector": "Khác"})
            p = price_map.get(sym, {})
            results.append({
                "rank": i + 1, "symbol": sym,
                "name": info["name"], "sector": info["sector"],
                "price": p.get("price", 0), "change": p.get("change", 0),
                "volume": p.get("volume", 0),
                "source": p.get("source", "—"),
            })

        _hose_cache["top250"] = {"ts": now, "data": results}
        log.info(f"HOSE prices: {len(price_map)}/{len(symbols)} mã có giá (TCBS + Yahoo fallback)")
        return results

    except Exception as e:
        log.error(f"HOSE prices fatal error: {e}")
        if cached:
            return cached["data"]
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
# REST — PHÂN TÍCH KỸ THUẬT: MA, SUPPORT/RESISTANCE, XU HƯỚNG
# ─────────────────────────────────────────────

def _sma(values: list[float], period: int) -> list[float | None]:
    """Simple moving average, trả None cho các điểm chưa đủ dữ liệu."""
    out = []
    for i in range(len(values)):
        if i + 1 < period:
            out.append(None)
        else:
            out.append(sum(values[i+1-period:i+1]) / period)
    return out


def _find_pivots(highs: list[float], lows: list[float], window: int = 3):
    """
    Tìm swing high/low đơn giản: điểm cao/thấp hơn `window` nến lân cận mỗi bên.
    Trả về list các giá trị pivot high và pivot low.
    """
    pivot_highs, pivot_lows = [], []
    n = len(highs)
    for i in range(window, n - window):
        if highs[i] == max(highs[i-window:i+window+1]):
            pivot_highs.append(highs[i])
        if lows[i] == min(lows[i-window:i+window+1]):
            pivot_lows.append(lows[i])
    return pivot_highs, pivot_lows


def _cluster_levels(levels: list[float], tolerance_pct: float = 0.015) -> list[dict]:
    """
    Gộp các mức giá gần nhau thành 1 vùng (cluster), trả về
    [{"price": giá_trung_bình, "strength": số_lần_chạm}], sort theo strength giảm dần.
    """
    if not levels:
        return []
    levels = sorted(levels)
    clusters = []
    current = [levels[0]]
    for lv in levels[1:]:
        if abs(lv - current[-1]) / current[-1] <= tolerance_pct:
            current.append(lv)
        else:
            clusters.append(current)
            current = [lv]
    clusters.append(current)

    result = [{"price": sum(c)/len(c), "strength": len(c)} for c in clusters]
    result.sort(key=lambda x: x["strength"], reverse=True)
    return result


@app.get("/api/vn/analysis/{symbol}")
async def get_vn_analysis(symbol: str):
    """
    Phân tích kỹ thuật 1 mã HOSE dựa trên dữ liệu 6 tháng (nến ngày):
    - MA20, MA50 và vị trí giá hiện tại so với MA
    - Vùng hỗ trợ / kháng cự từ swing high-low (gộp cluster)
    - Xu hướng tổng quan (uptrend/downtrend/sideway)
    - Gợi ý vùng vào tiền (mua) / vùng thoát (chốt lời) / vùng cắt lỗ
    """
    symbol = symbol.upper()
    try:
        bars = []
        async with httpx.AsyncClient(timeout=15) as client:
            # Thử các params khác nhau của TCBS cho nến ngày
            for params in [
                {"ticker": symbol, "type": "day",   "count": "120"},
                {"ticker": symbol, "type": "daily", "count": "120"},
                {"ticker": symbol, "resolution": "D", "count": "120"},
            ]:
                try:
                    r = await client.get(
                        "https://apipublic.tcbs.com.vn/stock-insight/v1/stock/bars-long-term",
                        params=params,
                        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://tcinvest.tcbs.com.vn/"},
                    )
                    data = r.json()
                    bars = data if isinstance(data, list) else data.get("data", [])
                    if bars and len(bars) >= 25:
                        log.info(f"Analysis {symbol}: TCBS OK với params {params}, {len(bars)} bars")
                        break
                    else:
                        log.warning(f"Analysis {symbol}: params {params} trả {len(bars)} bars")
                except Exception as e:
                    log.warning(f"Analysis {symbol}: params {params} lỗi: {e}")

        if not bars or len(bars) < 10:
            return JSONResponse(status_code=503, content={"error": f"Không đủ dữ liệu lịch sử ({len(bars) if bars else 0} bars)"})

        # Nếu ít hơn 25 vẫn cho phân tích nhưng giảm period MA
        min_bars = len(bars)

        closes = [float(b.get("close", 0)) for b in bars if b.get("close")]
        highs  = [float(b.get("high",  0)) for b in bars if b.get("high")]
        lows   = [float(b.get("low",   0)) for b in bars if b.get("low")]

        if not closes:
            return JSONResponse(status_code=503, content={"error": "Dữ liệu giá không hợp lệ"})

        current_price = closes[-1]
        n = len(closes)
        ma20_period = min(20, max(5, n // 4))
        ma50_period = min(50, max(10, n // 2))

        ma20_series = _sma(closes, ma20_period)
        ma50_series = _sma(closes, ma50_period)
        ma20 = ma20_series[-1]
        ma50 = ma50_series[-1]

        # Xu hướng
        if ma20 is not None and ma50 is not None:
            if current_price > ma20 > ma50:
                trend = "uptrend"
                trend_label = "Xu hướng tăng"
            elif current_price < ma20 < ma50:
                trend = "downtrend"
                trend_label = "Xu hướng giảm"
            else:
                trend = "sideway"
                trend_label = "Tích lũy / Đi ngang"
        elif ma20 is not None:
            if current_price > ma20:
                trend, trend_label = "uptrend", "Xu hướng tăng (ngắn hạn)"
            else:
                trend, trend_label = "downtrend", "Xu hướng giảm (ngắn hạn)"
        else:
            trend, trend_label = "sideway", "Chưa đủ dữ liệu xác định xu hướng"

        # Support / Resistance từ swing pivots
        pivot_highs, pivot_lows = _find_pivots(highs, lows, window=3)
        resistance_clusters = _cluster_levels([p for p in pivot_highs if p > current_price])
        support_clusters    = _cluster_levels([p for p in pivot_lows  if p < current_price])

        nearest_resistance = resistance_clusters[0]["price"] if resistance_clusters else None
        nearest_support    = support_clusters[0]["price"] if support_clusters else None

        # Nếu không tìm được support/resistance từ pivot, fallback dùng min/max gần đây
        recent_high = max(highs[-60:]) if len(highs) >= 60 else max(highs)
        recent_low  = min(lows[-60:])  if len(lows)  >= 60 else min(lows)
        if nearest_resistance is None:
            nearest_resistance = recent_high
        if nearest_support is None:
            nearest_support = recent_low

        # Vùng giao dịch gợi ý
        buy_zone_low  = nearest_support
        buy_zone_high = nearest_support * 1.02  # +2% trên hỗ trợ
        sell_zone_low  = nearest_resistance * 0.98
        sell_zone_high = nearest_resistance
        stop_loss = nearest_support * 0.97  # -3% dưới hỗ trợ

        # Vị trí giá hiện tại trong biên độ support-resistance
        if nearest_resistance > nearest_support:
            position_pct = round((current_price - nearest_support) / (nearest_resistance - nearest_support) * 100, 1)
        else:
            position_pct = 50.0
        position_pct = max(0, min(100, position_pct))

        # Khuyến nghị hành động dựa trên vị trí + xu hướng
        if position_pct <= 25 and trend != "downtrend":
            action = "buy_zone"
            action_label = "Đang gần vùng hỗ trợ — cân nhắc tích lũy"
        elif position_pct >= 80:
            action = "sell_zone"
            action_label = "Đang gần vùng kháng cự — cân nhắc chốt lời / giảm tỷ trọng"
        elif trend == "downtrend":
            action = "wait"
            action_label = "Xu hướng giảm — chờ tín hiệu đảo chiều rõ ràng"
        elif trend == "uptrend":
            action = "hold"
            action_label = "Xu hướng tăng — có thể nắm giữ, theo dõi sát kháng cự"
        else:
            action = "wait"
            action_label = "Đang tích lũy — chờ phá vùng để xác nhận xu hướng"

        return {
            "symbol": symbol,
            "current_price": round(current_price, 2),
            "ma20": round(ma20, 2) if ma20 else None,
            "ma50": round(ma50, 2) if ma50 else None,
            "trend": trend,
            "trend_label": trend_label,
            "support": round(nearest_support, 2),
            "resistance": round(nearest_resistance, 2),
            "position_pct": position_pct,
            "buy_zone":  {"low": round(buy_zone_low, 2),  "high": round(buy_zone_high, 2)},
            "sell_zone": {"low": round(sell_zone_low, 2), "high": round(sell_zone_high, 2)},
            "stop_loss": round(stop_loss, 2),
            "action": action,
            "action_label": action_label,
            "support_levels":    [{"price": round(c["price"],2), "strength": c["strength"]} for c in support_clusters[:3]],
            "resistance_levels": [{"price": round(c["price"],2), "strength": c["strength"]} for c in resistance_clusters[:3]],
            "history": [{"time": b.get("tradingDate") or b.get("date",""), "close": float(b.get("close",0))} for b in bars[-60:]],
        }

    except Exception as e:
        log.error(f"Analysis error [{symbol}]: {e}")
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

# ─────────────────────────────────────────────
# REST — ECONOMIC CALENDAR
# ─────────────────────────────────────────────

@app.get("/api/global-markets")
async def get_global_markets():
    """
    Lấy chỉ số thị trường toàn cầu từ Yahoo Finance:
    S&P500, Nasdaq, Dow Jones, Nikkei, KOSPI, DAX, FTSE, CAC40, giá dầu WTI, Brent
    Cache 5 phút.
    """
    SYMBOLS = {
        "^GSPC":  {"name": "S&P 500",    "region": "🇺🇸 Mỹ"},
        "^IXIC":  {"name": "Nasdaq",     "region": "🇺🇸 Mỹ"},
        "^DJI":   {"name": "Dow Jones",  "region": "🇺🇸 Mỹ"},
        "^N225":  {"name": "Nikkei 225", "region": "🇯🇵 Nhật"},
        "^KS11":  {"name": "KOSPI",      "region": "🇰🇷 Hàn Quốc"},
        "^GDAXI": {"name": "DAX",        "region": "🇩🇪 Đức"},
        "^FTSE":  {"name": "FTSE 100",   "region": "🇬🇧 Anh"},
        "^FCHI":  {"name": "CAC 40",     "region": "🇫🇷 Pháp"},
        "CL=F":   {"name": "Dầu WTI",    "region": "🛢️ Năng lượng"},
        "BZ=F":   {"name": "Dầu Brent",  "region": "🛢️ Năng lượng"},
    }

    now = time.time()
    cached = _coingecko_cache.get("global_markets")
    if cached and (now - cached[0]) < 300:
        return cached[1]

    results = []
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=12) as client:
        tasks = [
            client.get(
                f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}",
                params={"interval": "1d", "range": "2d"},
            )
            for sym in SYMBOLS
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    for sym, resp in zip(SYMBOLS, responses):
        info = SYMBOLS[sym]
        item = {"symbol": sym, "name": info["name"], "region": info["region"],
                "price": 0, "change": 0, "change_abs": 0}
        try:
            if isinstance(resp, Exception):
                raise resp
            if resp.status_code != 200:
                raise ValueError(f"HTTP {resp.status_code}")
            data = resp.json()
            meta  = data["chart"]["result"][0]["meta"]
            prev  = meta.get("previousClose") or meta.get("chartPreviousClose") or 1
            price = meta.get("regularMarketPrice", 0)
            item.update({
                "price":      round(price, 2),
                "change":     round((price - prev) / prev * 100, 2) if prev else 0,
                "change_abs": round(price - prev, 2),
            })
        except Exception as e:
            log.warning(f"global_markets [{sym}]: {e}")
        results.append(item)

    _coingecko_cache["global_markets"] = (now, results)
    return results


@app.get("/api/calendar")
async def get_calendar():
    """
    Fallback tĩnh — giữ để tương thích cũ.
    Frontend nên dùng /api/news-calendar để có dữ liệu thật từ Gemini.
    """
    now = datetime.now(ICT)
    return [
        {"date": (now + timedelta(days=1)).strftime("%d/%m/%Y"), "time": "19:30", "event": "US CPI MoM",       "impact": "high",   "prev": "0.3%",  "forecast": "0.2%"},
        {"date": (now + timedelta(days=2)).strftime("%d/%m/%Y"), "time": "02:00", "event": "FED Rate Decision", "impact": "high",   "prev": "5.50%", "forecast": "5.50%"},
        {"date": (now + timedelta(days=3)).strftime("%d/%m/%Y"), "time": "08:00", "event": "BTC Options Expiry","impact": "medium", "prev": "$1.8B", "forecast": "$2.1B"},
        {"date": (now + timedelta(days=5)).strftime("%d/%m/%Y"), "time": "21:30", "event": "US NFP",            "impact": "high",   "prev": "175K",  "forecast": "180K"},
    ]

# ─────────────────────────────────────────────
# NEWS CALENDAR — Gemini + Google Search grounding
# Tin tức + lịch sự kiện thật: FED, lãi suất, chứng khoán, crypto
# ─────────────────────────────────────────────

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"

_news_cache: dict = {}
NEWS_TTL = 3600  # 1 giờ

@app.get("/api/news-calendar")
async def get_news_calendar():
    """
    Dùng Gemini (Google Search grounding) lấy tin tức + lịch sự kiện
    liên quan FED, lãi suất, chứng khoán VN/Mỹ, crypto trong tuần.
    Trả về list các event để render vào bảng "Lịch sự kiện".
    Cache 1 giờ.
    """
    now = time.time()
    cached = _news_cache.get("calendar")
    if cached and (now - cached["ts"]) < NEWS_TTL:
        return cached["data"]

    if not GEMINI_API_KEY:
        return JSONResponse(status_code=503, content={"error": "GEMINI_API_KEY chưa được cấu hình"})

    today_str = datetime.now(ICT).strftime("%d/%m/%Y")

    prompt = (
        f"Hôm nay là {today_str}. Tìm kiếm và liệt kê các tin tức/sự kiện kinh tế "
        f"QUAN TRỌNG trong 7 ngày tới liên quan đến: FED, lãi suất Mỹ, CPI, NFP, "
        f"thị trường chứng khoán Mỹ (S&P500, Nasdaq), chứng khoán Việt Nam (VN-Index, HOSE), "
        f"và thị trường crypto (Bitcoin, Ethereum, ETF, regulation).\n\n"
        f"Trả về DUY NHẤT một JSON array, không markdown, không giải thích, theo format:\n"
        f'[{{"date": "DD/MM/YYYY", "time": "HH:MM", "event": "Tên sự kiện ngắn gọn tiếng Việt", '
        f'"impact": "high|medium|low", "category": "fed|stock|crypto|macro", '
        f'"summary": "Tóm tắt 1 câu ngắn về sự kiện/dự báo"}}]\n\n'
        f"Tối đa 10 sự kiện, sắp xếp theo ngày gần nhất trước. Chỉ trả JSON, không có markdown code block."
    )

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                GEMINI_URL,
                json={
                    "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                    "tools": [{"google_search": {}}],
                    "generationConfig": {"temperature": 0.3, "maxOutputTokens": 2048},
                },
            )
        data = r.json()
        text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")

        # Strip markdown fences nếu có
        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
            text = re.sub(r"```$", "", text).strip()

        events = json.loads(text)
        if not isinstance(events, list):
            raise ValueError("Gemini response is not a list")

        _news_cache["calendar"] = {"ts": now, "data": events}
        log.info(f"News calendar: Gemini OK — {len(events)} events")
        return events

    except Exception as e:
        log.error(f"News calendar error: {e}")
        # Fallback về calendar tĩnh nếu Gemini lỗi
        if cached:
            return cached["data"]
        now_dt = datetime.now(ICT)
        return [
            {"date": (now_dt + timedelta(days=1)).strftime("%d/%m/%Y"), "time": "19:30", "event": "US CPI MoM", "impact": "high", "category": "macro", "summary": "Chỉ số giá tiêu dùng Mỹ"},
            {"date": (now_dt + timedelta(days=2)).strftime("%d/%m/%Y"), "time": "02:00", "event": "FED Rate Decision", "impact": "high", "category": "fed", "summary": "Quyết định lãi suất FED"},
        ]

# ─────────────────────────────────────────────
# CHATBOT — Gemini proxy (tránh CORS từ frontend)
# ─────────────────────────────────────────────

from pydantic import BaseModel
from fastapi import BackgroundTasks
import uuid

class ChatMessage(BaseModel):
    role: str   # "user" hoặc "model"
    text: str

class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = []
    session_id: str | None = None

@app.post("/api/chat")
async def chat_with_gemini(req: ChatRequest, background_tasks: BackgroundTasks):
    if not GEMINI_API_KEY:
        return JSONResponse(status_code=503, content={"error": "GEMINI_API_KEY chưa được cấu hình trên server"})

    session_id = req.session_id or str(uuid.uuid4())[:8]

    contents = [
        {"role": m.role, "parts": [{"text": m.text}]}
        for m in req.history
    ]
    contents.append({"role": "user", "parts": [{"text": req.message}]})

    # Log câu hỏi của user (background, không block response)
    background_tasks.add_task(log_chat_message, session_id, "user", req.message)

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                GEMINI_URL,
                json={
                    "system_instruction": {
                        "parts": [{"text": "Bạn là trợ lý phân tích thị trường tài chính. Hỗ trợ phân tích crypto, chứng khoán Việt Nam (HOSE), tỷ giá, vàng, FED và lãi suất. Trả lời ngắn gọn, súc tích bằng tiếng Việt."}]
                    },
                    "contents": contents,
                    "generationConfig": {"maxOutputTokens": 1024, "temperature": 0.7},
                },
            )
        data = r.json()

        if "error" in data:
            log.error(f"Gemini chat error: {data['error']}")
            err_msg = data["error"].get("message", "Gemini API error")
            background_tasks.add_task(log_chat_message, session_id, "error", err_msg)
            return JSONResponse(status_code=502, content={"error": err_msg, "session_id": session_id})

        reply = (
            data.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
        )
        if not reply:
            log.warning(f"Gemini empty reply: {str(data)[:300]}")
            reply = "Xin lỗi, không nhận được phản hồi từ AI."

        # Log câu trả lời của bot
        background_tasks.add_task(log_chat_message, session_id, "model", reply)

        return {"reply": reply, "session_id": session_id}

    except Exception as e:
        log.error(f"Chat proxy error: {e}")
        background_tasks.add_task(log_chat_message, session_id, "error", str(e))
        return JSONResponse(status_code=500, content={"error": str(e), "session_id": session_id})

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

@app.get("/analysis.html")
def analysis_page():
    return FileResponse("static/analysis.html")
