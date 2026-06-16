"""
Market Research Hub — Backend
==============================
- FastAPI server
- WebSocket proxy: OKX → client (Bybit/Binance bị geo-block trên Render US IP)
- REST APIs: OKX klines, CoinGecko, Yahoo Finance, TCBS, SJC, Vietcombank
- HOSE Top 50 endpoint
- Telegram alerts: BTC, ETH, USD/VND, Gold (SJC)
- Serve static files

PATCH NOTES (fix bảng HOSE trống + Multi-TF stale):
  • TCBS: thêm headers đầy đủ + domain fallback (apipublic -> apipubaws),
    log rõ status_code/response khi không phải JSON hợp lệ.
  • TCBS: nếu toàn bộ batch thất bại, fallback sang Yahoo Finance cho HOSE
    để bảng không bị trống hoàn toàn.
  • Multi-TF: log rõ exception thay vì nuốt im lặng, thêm timeout ngắn hơn
    và đếm số request thành công để debug dễ hơn.
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
import gspread
from google.oauth2.service_account import Credentials

ICT = pytz.timezone("Asia/Ho_Chi_Minh")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def _safe_json(r: httpx.Response):
    """
    Parse JSON an toàn tuyệt đối — dùng cho MỌI external API call trong file này.
    Trả về None nếu:
    - content-type không phải JSON (trang chặn bot trả HTML, Cloudflare challenge, v.v.)
    - body không parse được dù content-type là JSON
    Không bao giờ raise exception ra ngoài — chỉ trả None để caller tự fallback sang nguồn khác.
    Đây là fix gốc cho lỗi 500 "ValueError: invalid literal for int() with base 10: 'c'"
    (xảy ra khi sàn trả 403/451 với body HTML nhưng code cũ vẫn ép gọi r.json()).
    """
    ctype = r.headers.get("content-type", "")
    if "json" not in ctype:
        return None
    try:
        return r.json()
    except Exception:
        return None


def _parse_json_array_safely(text: str) -> list | None:
    """
    Parse 1 chuỗi text thành JSON array, với khả năng khôi phục khi LLM (Gemini)
    bị cắt giữa dòng do hết maxOutputTokens (lỗi "Unterminated string ...").

    Chiến lược:
    1. Thử parse trực tiếp — trường hợp JSON hoàn chỉnh, nhanh nhất.
    2. Nếu lỗi, tìm vị trí object "}" hợp lệ CUỐI CÙNG trong text, cắt bỏ phần
       dở dang sau đó, đóng lại bằng "]", rồi parse lại.
       Ví dụ: '[{"a":1},{"a":2},{"a":"b' (bị cắt giữa string)
       -> khôi phục thành '[{"a":1},{"a":2}]' (bỏ object dở dang cuối, giữ các object hoàn chỉnh).
    3. Nếu vẫn lỗi, trả None — caller tự fallback.
    """
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        log.warning(f"_parse_json_array_safely: parse trực tiếp lỗi ({e}), thử khôi phục từ phần hoàn chỉnh")

    # Tìm vị trí "}" cuối cùng — đây là điểm kết thúc của object cuối cùng còn nguyên vẹn
    last_brace = text.rfind("}")
    if last_brace == -1:
        return None

    candidate = text[:last_brace + 1]
    # Đảm bảo candidate vẫn mở đầu bằng "[" 
    if not candidate.lstrip().startswith("["):
        return None

    candidate = candidate.rstrip()
    # Bỏ dấu "," dư ở cuối nếu có (trường hợp object cuối bị cắt ngay sau dấu phẩy)
    if candidate.endswith(","):
        candidate = candidate[:-1]
    candidate += "]"

    try:
        result = json.loads(candidate)
        log.info(f"_parse_json_array_safely: khôi phục thành công {len(result)} object từ JSON bị cắt")
        return result
    except json.JSONDecodeError as e:
        log.warning(f"_parse_json_array_safely: khôi phục thất bại ({e}), candidate[-100:]={candidate[-100:]!r}")
        return None


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
GOLD_MIN   = float(os.getenv("GOLD_MIN",   "120000000"))
GOLD_MAX   = float(os.getenv("GOLD_MAX",   "155000000"))

# ─────────────────────────────────────────────
# GOOGLE SHEETS — Chat Log Storage
# ─────────────────────────────────────────────

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
CHAT_LOG_SHEET_ID = "12W6K3Y3-Ac2tCE1B8JBOC-ZYAB09QRdFJn-Yv1mmk3w"
CHAT_LOG_SHEET_NAME = "Logs"

_gsheet_client = None
_chat_log_worksheet = None

def _get_chat_log_worksheet():
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
    try:
        ws = _get_chat_log_worksheet()
        if ws is None:
            return
        timestamp = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([timestamp, session_id, role, message])
    except Exception as e:
        log.error(f"Chat log write error: {e}")


# 200 mã HOSE
HOSE_TOP200 = [
    "VCB","BID","VIC","VHM","CTG","GAS","VNM","SAB","MSN","TCB",
    "MBB","FPT","ACB","PLX","HPG","VPB","STB","HDB","GVR","POW",
    "MWG","PNJ","REE","SSI","VND","HCM","DPM","DCM","VEA","KDH",
    "NVL","PDR","DXG","PVD","HSG","NKG","PHR","DRC","IDC","KBC",
    "NTC","LHG","EIB","EVF","CMG","VGI","FRT","DGW","GEX","VRE",
    "BVH","BCM","PC1","PVT","BSR","BMI","DGC","CTD","HDG","HAH",
    "ANV","VHC","DBC","NLG","CII","TCH","HHV","VCG","HT1","PAN",
    "VOS","VTP","VCI","SHB","TPB","OCB","MSB","LPB","BAB","NAB",
    "TLG","SCS","ASM","CTS","FTS","PVS","PVC","TIS","NT2","VSH",
    "BWE","DPR","HAG","HNG","DHC","SBT","SZC","DIG","ITA","TDM",
    "AAA","APH","BFC","BCG","BHN","CAV","CKG","CLL","CMX","CRE",
    "DAH","DBD","DHA","DPG","ELC","EVE","FCN","FIT","FTM","GEG",
    "GIL","GMD","HBC","HCD","HII","HQC","HU1","HVH","IJC","IMP",
    "ITC","KSB","LCG","LDG","LSS","MCP","NHA","NHH","NTL","OGC",
    "PDN","PGD","PGI","PHC","PIT","PLP","PMG","PTB","QCG","RAL",
    "SAM","SBA","SCD","SFG","SGN","SGT","SHA","SHI","SJD","SJS",
    "SMA","SMB","SMC","SRC","SRF","SVC","SVI","TCM","TDC","TDH",
    "TDP","TEG","THG","TLH","TNA","TNI","TNT","TPC","TRA","TSC",
    "TTF","TV2","TVS","UDC","VCF","VDS","VFG","VID","VIP","VIX",
    "VNE","VNG","VPG","VPI","VSC","VTO","YEG","BMP","DXS","NAF",
]
HOSE_TOP100 = HOSE_TOP200
HOSE_TOP50  = HOSE_TOP200

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
    Chain: OKX -> Bybit -> Binance.
    Mỗi bước dùng _safe_json (không bao giờ raise từ parse JSON lỗi/HTML).
    """
    # ── 1) OKX ─────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(
                "https://www.okx.com/api/v5/market/ticker",
                params={"instId": symbol_okx},
            )
        data = _safe_json(r)
        lst  = (data or {}).get("data", [])
        if lst:
            price = float(lst[0]["last"])
            if price > 0:
                return price
        if data is None:
            log.warning(f"OKX price ({symbol_okx}): HTTP {r.status_code}, non-JSON body")
    except Exception as e:
        log.warning(f"OKX price error ({symbol_okx}): {e}")

    # ── 2) Bybit ───────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(
                "https://api.bybit.com/v5/market/tickers",
                params={"category": "linear", "symbol": symbol_binance},
            )
        data = _safe_json(r)
        lst  = (data or {}).get("result", {}).get("list", [])
        if lst:
            price = float(lst[0]["lastPrice"])
            if price > 0:
                return price
        if data is None:
            log.warning(f"Bybit price ({symbol_binance}): HTTP {r.status_code}, non-JSON body")
    except Exception as e:
        log.warning(f"Bybit price error ({symbol_binance}): {e}")

    # ── 3) Binance fallback ────────────────────
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol_binance}")
        data = _safe_json(r)
        if isinstance(data, dict):
            price = float(data.get("price", 0))
            if price > 0:
                return price
        elif data is None:
            log.warning(f"Binance price ({symbol_binance}): HTTP {r.status_code}, non-JSON body")
    except Exception as e:
        log.warning(f"Binance fallback price error ({symbol_binance}): {e}")

    raise ValueError(f"Cannot fetch price for {symbol_okx}/{symbol_binance} (đã thử OKX, Bybit, Binance)")

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
            gold_data = _safe_json(rg)
            if gold_data is None:
                log.warning(f"fetch_gold_price: Yahoo GC=F trả non-JSON, status={rg.status_code}")
                return None
            try:
                gold_usd_oz = gold_data["chart"]["result"][0]["meta"]["regularMarketPrice"]
            except (KeyError, IndexError, TypeError) as e:
                log.warning(f"fetch_gold_price: Yahoo response thiếu field cần thiết: {e}")
                return None

            import xml.etree.ElementTree as ET
            rv = await client.get(
                "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx?b=10",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            usd_vnd = 0.0
            try:
                root = ET.fromstring(rv.text)
                for ex in root.findall(".//Exrate"):
                    if ex.get("CurrencyCode") == "USD":
                        usd_vnd = float(ex.get("Sell", "0").replace(",", ""))
                        break
            except ET.ParseError as e:
                log.warning(f"fetch_gold_price: VCB XML parse error: {e}")
                return None

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

# Dùng cho fallback Bybit trong /api/klines (chain OKX -> Bybit -> Binance)
BYBIT_INTERVAL_MAP_FALLBACK = {
    "1m": "1",   "3m": "3",   "5m": "5",   "15m": "15",  "30m": "30",
    "1h": "60",  "2h": "120", "4h": "240", "6h": "360",  "12h": "720",
    "1d": "D",   "1w": "W",   "1M": "M",
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

def _parse_kline_rows(raw: list, source: str) -> list[dict] | None:
    """
    Parse list nến thành format chuẩn, bỏ qua an toàn nếu 1 dòng nào hỏng.
    Trả về None nếu raw không phải list-of-list/array hợp lệ.
    """
    if not isinstance(raw, list):
        return None
    out = []
    for k in raw:
        try:
            if not isinstance(k, (list, tuple)) or len(k) < 6:
                continue
            out.append({
                "time":   int(float(k[0])) // 1000,
                "open":   float(k[1]),
                "high":   float(k[2]),
                "low":    float(k[3]),
                "close":  float(k[4]),
                "volume": float(k[5]),
            })
        except (ValueError, TypeError, IndexError) as e:
            log.warning(f"_parse_kline_rows [{source}]: bỏ qua 1 dòng lỗi: {e}")
            continue
    return out if out else None


@app.get("/api/klines")
async def get_klines(symbol: str = "BTCUSDT", interval: str = "1h", limit: int = 200):
    """
    Thứ tự nguồn: OKX -> Bybit -> Binance.
    Mọi bước parse JSON đều an toàn tuyệt đối (không bao giờ raise ra ngoài),
    nên endpoint này KHÔNG BAO GIỜ trả 500 — chỉ trả 503 với thông báo lỗi rõ ràng.
    """
    okx_symbol     = to_okx_symbol(symbol)
    okx_bar        = OKX_INTERVAL_MAP.get(interval, "1H")
    bybit_symbol   = symbol.upper()
    bybit_interval = BYBIT_INTERVAL_MAP_FALLBACK.get(interval, "60")
    errors = []

    # ── 1) OKX ─────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://www.okx.com/api/v5/market/candles",
                params={"instId": okx_symbol, "bar": okx_bar, "limit": min(limit, 300)},
            )
        result = _safe_json(r)
        if result is not None and result.get("code") == "0":
            rows = _parse_kline_rows(result.get("data", []), "OKX")
            if rows:
                rows = rows[::-1]
                log.info(f"OKX kline OK: {okx_symbol} {okx_bar} ({len(rows)} bars)")
                return rows
        msg = (result or {}).get("msg") if result else f"HTTP {r.status_code}, non-JSON body"
        errors.append(f"OKX: {msg}")
        log.warning(f"OKX kline failed: {msg}")
    except Exception as e:
        errors.append(f"OKX: {e}")
        log.warning(f"OKX kline REST error: {e}")

    # ── 2) Bybit ───────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.bybit.com/v5/market/kline",
                params={"category": "linear", "symbol": bybit_symbol, "interval": bybit_interval, "limit": min(limit, 1000)},
            )
        result = _safe_json(r)
        if result is not None and result.get("retCode") == 0:
            rows = _parse_kline_rows(result.get("result", {}).get("list", []), "Bybit")
            if rows:
                rows = rows[::-1]
                log.info(f"Bybit kline OK: {bybit_symbol} {bybit_interval} ({len(rows)} bars)")
                return rows
        msg = (result or {}).get("retMsg") if result else f"HTTP {r.status_code}, non-JSON body"
        errors.append(f"Bybit: {msg}")
        log.warning(f"Bybit kline failed: {msg}")
    except Exception as e:
        errors.append(f"Bybit: {e}")
        log.warning(f"Bybit kline REST error: {e}")

    # ── 3) Binance fallback ────────────────────
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}&interval={interval}&limit={limit}"
            )
        if r.status_code == 451:
            errors.append("Binance: 451 geo-blocked")
        else:
            data = _safe_json(r)
            rows = _parse_kline_rows(data, "Binance") if data is not None else None
            if rows:
                log.info(f"Binance kline OK: {symbol} {interval} ({len(rows)} bars)")
                return rows
            errors.append(f"Binance: HTTP {r.status_code}, không parse được dữ liệu")
    except Exception as e:
        errors.append(f"Binance: {e}")

    # ── Tất cả nguồn đều fail ──────────────────
    log.error(f"get_klines: TẤT CẢ nguồn fail cho {symbol}/{interval} — {' | '.join(errors)}")
    return JSONResponse(status_code=503, content={"error": "Không lấy được dữ liệu nến từ bất kỳ nguồn nào", "details": errors})

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
            log.warning("CoinGecko 429 — trả cache cũ nếu có")
            return cached[1] if cached else []
        data = _safe_json(r)
        if data is None:
            log.warning(f"CoinGecko trả non-JSON, status={r.status_code} — trả cache cũ nếu có")
            return cached[1] if cached else []
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
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://api.alternative.me/fng/?limit=1")
        data = _safe_json(r)
        if data is None:
            return JSONResponse(status_code=503, content={"error": f"Fear&Greed API trả non-JSON, status={r.status_code}"})
        return data
    except Exception as e:
        return JSONResponse(status_code=503, content={"error": str(e)})

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
# REST — HOSE TOP 200
# PATCHED: TCBS headers đầy đủ + domain fallback + log rõ ràng +
#          fallback toàn bộ sang Yahoo nếu TCBS chết hoàn toàn
# ─────────────────────────────────────────────

TCBS_DOMAINS = [
    "https://apipubaws.tcbs.com.vn",
    "https://apipublic.tcbs.com.vn",
]

TCBS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://tcinvest.tcbs.com.vn/",
    "Origin": "https://tcinvest.tcbs.com.vn",
}


async def _fetch_tcbs_batch(client: httpx.AsyncClient, batch: list[str]) -> dict:
    """
    Thử lần lượt các domain TCBS. Trả về {ticker: {price, change, volume}}.
    Log rõ status_code + 200 ký tự đầu response khi không parse được JSON,
    để dễ debug trên Render logs.
    """
    tickers_str = ",".join(batch)
    last_error = None

    for domain in TCBS_DOMAINS:
        url = f"{domain}/stock-insight/v1/stock/price"
        try:
            r = await client.get(
                url,
                params={"tickers": tickers_str},
                headers=TCBS_HEADERS,
            )
            content_type = r.headers.get("content-type", "")
            if "json" not in content_type:
                log.warning(
                    f"TCBS [{domain}] trả về content-type lạ: {content_type} "
                    f"status={r.status_code} body[:150]={r.text[:150]!r}"
                )
                last_error = f"non-json content-type ({content_type}), status {r.status_code}"
                continue

            data  = r.json()
            items = data if isinstance(data, list) else data.get("data", [])

            if not items:
                log.warning(f"TCBS [{domain}] batch trả về rỗng. status={r.status_code} raw[:200]={str(data)[:200]!r}")
                last_error = "empty items"
                continue

            out = {}
            for item in items:
                ticker = (item.get("ticker") or item.get("symbol") or "").upper()
                if not ticker:
                    continue
                price  = float(item.get("close") or item.get("price") or item.get("lastPrice") or 0)
                prev   = float(item.get("referencePrice") or item.get("prevClose") or item.get("ref") or 0)
                change = round((price - prev) / prev * 100, 2) if prev > 0 else 0
                volume = int(item.get("volume") or item.get("totalVolume") or 0)
                out[ticker] = {"price": price, "change": change, "volume": volume}

            if out:
                log.info(f"TCBS [{domain}] OK — {len(out)}/{len(batch)} mã có giá")
                return out
            last_error = "parsed items nhưng không có ticker hợp lệ"

        except json.JSONDecodeError as e:
            log.warning(f"TCBS [{domain}] JSONDecodeError: {e} — status={getattr(r,'status_code','?')} body[:150]={getattr(r,'text','')[:150]!r}")
            last_error = f"JSONDecodeError: {e}"
        except Exception as e:
            log.warning(f"TCBS [{domain}] lỗi request: {e}")
            last_error = str(e)

    log.error(f"TCBS: TẤT CẢ domain đều fail cho batch {tickers_str[:60]}... — lỗi cuối: {last_error}")
    return {}


async def _fetch_yahoo_hose_batch(symbols: list[str]) -> dict:
    """
    Fallback toàn bộ sang Yahoo Finance khi TCBS chết hoàn toàn.
    Trả về {ticker: {price, change, volume}}.
    """
    out = {}
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=10) as client:
        results = await asyncio.gather(
            *[_fetch_yahoo_stock(client, f"{s}.VN") for s in symbols],
            return_exceptions=True,
        )
    for sym, res in zip(symbols, results):
        if isinstance(res, dict) and res.get("price", 0) > 0:
            out[sym] = {"price": res["price"], "change": res["change"], "volume": res["volume"]}
    log.info(f"Yahoo HOSE fallback: {len(out)}/{len(symbols)} mã có giá")
    return out


@app.get("/api/vn/hose-top50")
async def get_hose_top50():
    """
    Trả về toàn bộ HOSE_TOP200 kèm giá thời gian thực.
    Thứ tự nguồn: TCBS (apipubaws -> apipublic) theo batch 50 mã.
    Nếu TCBS hoàn toàn không trả được giá cho TOÀN BỘ danh sách,
    fallback sang Yahoo Finance cho 50 mã đầu (tránh bảng trống 100%).
    """
    now = time.time()
    cached = _hose_cache.get("top250")
    if cached and (now - cached["ts"]) < HOSE_TTL:
        return cached["data"]

    symbols = HOSE_TOP100
    results = []
    price_map = {}
    BATCH = 50

    try:
        async with httpx.AsyncClient(timeout=25) as client:
            for i in range(0, len(symbols), BATCH):
                batch = symbols[i:i+BATCH]
                batch_prices = await _fetch_tcbs_batch(client, batch)
                price_map.update(batch_prices)
                if i + BATCH < len(symbols):
                    await asyncio.sleep(0.3)

        # Nếu TCBS chết hoàn toàn (0 mã có giá), fallback Yahoo cho 50 mã đầu
        if not price_map:
            log.error("TCBS hoàn toàn không trả được giá — fallback sang Yahoo Finance cho 50 mã đầu")
            yahoo_prices = await _fetch_yahoo_hose_batch(symbols[:50])
            price_map.update(yahoo_prices)
            source_label = "Yahoo"
        else:
            source_label = "TCBS"

        for i, sym in enumerate(symbols):
            info = HOSE_INFO.get(sym, {"name": sym, "sector": "Khác"})
            p = price_map.get(sym, {})
            results.append({
                "rank": i + 1, "symbol": sym,
                "name": info["name"], "sector": info["sector"],
                "price": p.get("price", 0), "change": p.get("change", 0),
                "volume": p.get("volume", 0),
                "source": source_label if sym in price_map else "—",
            })

        _hose_cache["top250"] = {"ts": now, "data": results}
        log.info(f"HOSE prices: {source_label} — {len(price_map)}/{len(symbols)} mã có giá")
        return results

    except Exception as e:
        log.error(f"HOSE prices fatal error: {e}")
        if cached:
            return cached["data"]
        return JSONResponse(status_code=503, content={"error": str(e)})


# ─────────────────────────────────────────────
# REST — TCBS HISTORICAL
# PATCHED: dùng cùng helper domain fallback + headers
# ─────────────────────────────────────────────

async def _fetch_tcbs_url(client: httpx.AsyncClient, path: str, params: dict):
    """Thử lần lượt các domain TCBS cho 1 GET request, trả (data, domain_used) hoặc (None, last_error)."""
    last_error = None
    for domain in TCBS_DOMAINS:
        try:
            r = await client.get(f"{domain}{path}", params=params, headers=TCBS_HEADERS)
            if "json" not in r.headers.get("content-type", ""):
                last_error = f"non-json from {domain}, status {r.status_code}"
                continue
            return r.json(), domain
        except Exception as e:
            last_error = str(e)
    return None, last_error


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
            data, src = await _fetch_tcbs_url(
                client,
                "/stock-insight/v1/stock/bars-long-term",
                {"ticker": symbol.upper(), "type": unit, "count": count},
            )
        if data is None:
            log.error(f"TCBS history fetch failed for {symbol}: {src}")
            return JSONResponse(status_code=503, content={"error": f"TCBS không phản hồi: {src}"})

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
    out = []
    for i in range(len(values)):
        if i + 1 < period:
            out.append(None)
        else:
            out.append(sum(values[i+1-period:i+1]) / period)
    return out


def _find_pivots(highs: list[float], lows: list[float], window: int = 3):
    pivot_highs, pivot_lows = [], []
    n = len(highs)
    for i in range(window, n - window):
        if highs[i] == max(highs[i-window:i+window+1]):
            pivot_highs.append(highs[i])
        if lows[i] == min(lows[i-window:i+window+1]):
            pivot_lows.append(lows[i])
    return pivot_highs, pivot_lows


def _cluster_levels(levels: list[float], tolerance_pct: float = 0.015) -> list[dict]:
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
    symbol = symbol.upper()
    try:
        async with httpx.AsyncClient(timeout=15) as client:
             bars = []
        for params in [
            {"ticker": symbol, "type": "day",   "count": "120"},
            {"ticker": symbol, "type": "daily", "count": "120"},
            {"ticker": symbol, "type": "month", "count": "24"},
        ]:
            data, src = await _fetch_tcbs_url(
                client,
                "/stock-insight/v1/stock/bars-long-term",
                params,
            )
            if data is not None:
                candidate = data if isinstance(data, list) else data.get("data", [])
                if candidate and len(candidate) >= 10:
                    bars = candidate
                    log.info(f"Analysis {symbol}: {len(bars)} bars via params={params}")
                    break

        if not bars:
            return JSONResponse(status_code=503, content={"error": f"TCBS không trả được dữ liệu: {src}"})

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

        pivot_highs, pivot_lows = _find_pivots(highs, lows, window=3)
        resistance_clusters = _cluster_levels([p for p in pivot_highs if p > current_price])
        support_clusters    = _cluster_levels([p for p in pivot_lows  if p < current_price])

        nearest_resistance = resistance_clusters[0]["price"] if resistance_clusters else None
        nearest_support    = support_clusters[0]["price"] if support_clusters else None

        recent_high = max(highs[-60:]) if len(highs) >= 60 else max(highs)
        recent_low  = min(lows[-60:])  if len(lows)  >= 60 else min(lows)
        if nearest_resistance is None:
            nearest_resistance = recent_high
        if nearest_support is None:
            nearest_support = recent_low

        buy_zone_low  = nearest_support
        buy_zone_high = nearest_support * 1.02
        sell_zone_low  = nearest_resistance * 0.98
        sell_zone_high = nearest_resistance
        stop_loss = nearest_support * 0.97

        if nearest_resistance > nearest_support:
            position_pct = round((current_price - nearest_support) / (nearest_resistance - nearest_support) * 100, 1)
        else:
            position_pct = 50.0
        position_pct = max(0, min(100, position_pct))

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
# PATCHED: log lỗi rõ ràng, đếm success/fail, timeout ngắn hơn
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
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=10) as client:
        for tf in timeframes:
            bullish, total, failed = 0, 0, 0
            responses = await asyncio.gather(*[
                client.get(
                    f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}.VN",
                    params={"interval": tf["interval"], "range": tf["range"]},
                )
                for sym in sample
            ], return_exceptions=True)

            for sym, resp in zip(sample, responses):
                try:
                    if isinstance(resp, Exception):
                        log.warning(f"Multi-TF [{tf['key']}] {sym}: request exception {resp}")
                        failed += 1
                        continue
                    if resp.status_code != 200:
                        log.warning(f"Multi-TF [{tf['key']}] {sym}: HTTP {resp.status_code}")
                        failed += 1
                        continue
                    closes = resp.json()["chart"]["result"][0]["indicators"]["quote"][0].get("close", [])
                    closes = [c for c in closes if c is not None]
                    if len(closes) < tf["ma"] + 1:
                        failed += 1
                        continue
                    ma = sum(closes[-tf["ma"]:]) / tf["ma"]
                    if closes[-1] > ma:
                        bullish += 1
                    total += 1
                except Exception as e:
                    log.warning(f"Multi-TF [{tf['key']}] {sym}: parse error {e}")
                    failed += 1
                    continue

            if total == 0:
                log.error(f"Multi-TF [{tf['key']}]: TẤT CẢ {len(sample)} request thất bại (failed={failed}) — Yahoo có thể đang chặn IP server")
                results[tf["key"]] = (cached["data"].get(tf["key"], 50) if cached else 50)
            else:
                results[tf["key"]] = round(bullish / total * 100)
                log.info(f"Multi-TF [{tf['key']}]: {total} OK / {failed} fail — bullish={bullish}/{total}")

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
# NEWS CALENDAR — Gemini + Google Search grounding
# ─────────────────────────────────────────────

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"

_news_cache: dict = {}
NEWS_TTL = 3600

@app.get("/api/news-calendar")
async def get_news_calendar():
    now = time.time()
    cached = _news_cache.get("calendar")
    if cached and (now - cached["ts"]) < NEWS_TTL:
        return cached["data"]

    if not GEMINI_API_KEY:
        return JSONResponse(status_code=503, content={"error": "GEMINI_API_KEY chưa được cấu hình"})

    today_str = datetime.now(ICT).strftime("%d/%m/%Y")

    prompt = (
        f"Hôm nay là {today_str}. Tìm kiếm và liệt kê các tin tức/sự kiện kinh tế và thị trường "
        f"QUAN TRỌNG trong 7 ngày qua và 7 ngày tới, chia đều theo 4 nhóm sau "
        f"(ưu tiên ít nhất 2-3 tin mỗi nhóm nếu có, không để 1 nhóm chiếm hết danh sách):\n\n"
        f"1. FED/Vĩ mô Mỹ: lãi suất FED, CPI, NFP, các phát biểu của FED.\n"
        f"2. Chứng khoán Mỹ: S&P500, Nasdaq, các sự kiện lớn ảnh hưởng thị trường Mỹ.\n"
        f"3. Chứng khoán Việt Nam: VN-Index, dòng tiền khối ngoại, chính sách (room ngoại, "
        f"thuế, nâng hạng thị trường), VÀ tin tức cụ thể của các doanh nghiệp niêm yết lớn trên HOSE "
        f"(kết quả kinh doanh quý/năm, chia cổ tức, phát hành thêm, M&A, thay đổi nhân sự cấp cao, "
        f"biến động giá cổ phiếu đáng chú ý) — ưu tiên các mã vốn hóa lớn như VCB, BID, VIC, VHM, "
        f"CTG, GAS, VNM, FPT, HPG, MWG, MSN, TCB, VPB, MBB, SSI, VND, GVR, VRE và các mã đang có tin "
        f"nóng trong tuần.\n"
        f"4. Crypto: Bitcoin, Ethereum, ETF, quy định pháp lý.\n\n"
        f"Trả về DUY NHẤT một JSON array, không markdown, không giải thích, theo format:\n"
        f'[{{"date": "DD/MM/YYYY", "time": "HH:MM", "event": "Tên sự kiện ngắn gọn tiếng Việt", '
        f'"impact": "high|medium|low", "category": "fed|stock|vn_stock|crypto|macro", '
        f'"summary": "Tóm tắt 1 câu ngắn về sự kiện/dự báo, nêu rõ mã cổ phiếu nếu có"}}]\n\n'
        f"Dùng category \"vn_stock\" riêng cho tin chứng khoán Việt Nam (cả vĩ mô VN-Index và tin "
        f"doanh nghiệp cụ thể), KHÔNG dùng \"stock\" cho tin Việt Nam — \"stock\" chỉ dùng cho thị "
        f"trường Mỹ.\n"
        f"Tối đa 14 sự kiện, sắp xếp theo ngày gần nhất trước. Chỉ trả JSON, không có markdown code block."
    )

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                GEMINI_URL,
                json={
                    "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                    "tools": [{"google_search": {}}],
                    "generationConfig": {"temperature": 0.3, "maxOutputTokens": 4096},
                },
            )
        data = _safe_json(r)
        if data is None:
            raise ValueError(f"Gemini trả non-JSON, status={r.status_code}, body[:200]={r.text[:200]!r}")

        candidate    = (data.get("candidates") or [{}])[0]
        finish_reason = candidate.get("finishReason", "")
        text = candidate.get("content", {}).get("parts", [{}])[0].get("text", "")

        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
            text = re.sub(r"```$", "", text).strip()

        if finish_reason == "MAX_TOKENS":
            log.warning("News calendar: Gemini bị cắt do MAX_TOKENS — sẽ thử khôi phục JSON từ phần đã có")

        events = _parse_json_array_safely(text)
        if events is None:
            raise ValueError(f"Không parse được JSON (finish_reason={finish_reason}). raw_text[:300]={text[:300]!r}")
        if not isinstance(events, list) or not events:
            raise ValueError(f"Gemini response không phải list hợp lệ hoặc rỗng: {str(events)[:200]}")

        _news_cache["calendar"] = {"ts": now, "data": events}
        log.info(f"News calendar: Gemini OK — {len(events)} events")
        return events

    except Exception as e:
        log.error(f"News calendar error: {e}")
        if cached:
            return cached["data"]
        now_dt = datetime.now(ICT)
        return [
            {"date": (now_dt + timedelta(days=1)).strftime("%d/%m/%Y"), "time": "19:30", "event": "US CPI MoM", "impact": "high", "category": "macro", "summary": "Chỉ số giá tiêu dùng Mỹ"},
            {"date": (now_dt + timedelta(days=2)).strftime("%d/%m/%Y"), "time": "02:00", "event": "FED Rate Decision", "impact": "high", "category": "fed", "summary": "Quyết định lãi suất FED"},
            {"date": now_dt.strftime("%d/%m/%Y"), "time": "--:--", "event": "VN-Index biến động", "impact": "medium", "category": "vn_stock", "summary": "Theo dõi diễn biến VN-Index và dòng tiền khối ngoại trong phiên"},
        ]

# ─────────────────────────────────────────────
# CHATBOT — Gemini proxy
# ─────────────────────────────────────────────

from pydantic import BaseModel
from fastapi import BackgroundTasks
import uuid

class ChatMessage(BaseModel):
    role: str
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
