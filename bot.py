import os
import time
import json
import threading
from datetime import datetime, timedelta
import telebot
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
import requests
from dotenv import load_dotenv
import pytz
import psycopg2
from flask import Flask, request, abort

load_dotenv()
TOKEN        = (os.getenv('TELEGRAM_TOKEN') or '').strip()
RENDER_URL   = (os.getenv('RENDER_URL') or '').strip()
DATABASE_URL = (os.getenv('DATABASE_URL') or '').strip()
PORT         = int(os.getenv('PORT', 10000))

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

# Her chat için aktif işlem iptali
# { chat_id: { "optimize": threading.Event, "check": threading.Event } }
_cancel_flags = {}

def get_cancel_flag(chat_id, op):
    chat_id = str(chat_id)
    if chat_id not in _cancel_flags:
        _cancel_flags[chat_id] = {}
    if op not in _cancel_flags[chat_id]:
        _cancel_flags[chat_id][op] = threading.Event()
    return _cancel_flags[chat_id][op]

def reset_cancel_flag(chat_id, op):
    flag = get_cancel_flag(chat_id, op)
    flag.clear()
    return flag

def is_cancelled(chat_id, op):
    return get_cancel_flag(chat_id, op).is_set()

TD_DELAY = 1.5   # saniye – Yahoo Finance için yeterli

BIST_FALLBACK = [
    "THYAO","GARAN","ASELS","KCHOL","EREGL","AKBNK","YKBNK","SISE","TUPRS","SAHOL",
    "FROTO","TOASO","PETKM","HALKB","VAKBN","TTKOM","BIMAS","AKSEN","ENKAI","KOZAL",
    "ISCTR","ARCLK","PGSUS","TAVHL","TCELL","DOHOL","OYAKC","OTKAR","GUBRF","MGROS",
    "SOKM","ULKER","CCOLA","AGHOL","EKGYO","LOGO","NETAS","VESTL","TSKB","ALARK",
    "ALFAS","AEFES","KRDMD","GOLTS","SODA","KONYA","EGEEN","TKFEN","KERVT","HEKTS",
    "GOODY","DURDO","ADEL","ADANA","AKMGY","ANACM","ANHYT","ANSGR","ASTOR","AYGAZ",
    "BAGFS","BAKAB","BANVT","BASCM","BERA","BFREN","BIOEN","BIZIM","BJKAS","BLCYT",
    "BMSTL","BNTAS","BOSSA","BRISA","BRKSN","BRYAT","BSOKE","BTCIM","BUCIM","BURCE",
    "BURVA","BVSAN","CATES","CEMTS","CIMSA","CLEBI","CRDFA","CRFSA","CUSAN","CVKMD",
    "DAGHL","DARDL","DENGE","DERHL","DERIM","DESAS","DESPC","DEVA","DGATE","DITAS",
    "DMSAS","DNISI","DOGUB","DORTS","DPENS","DYOBY","DZGYO","ECILC","ECZYT","EDIP",
    "EGGUB","EGPRO","EGSER","EKIZ","EMKEL","EMNIS","ENDL","EPLAS","ERSU","ESCOM",
    "ESEN","ETYAT","EUHOL","FADE","FENER","FONET","FORTE","GARFA","GEDIK","GEDZA",
    "GENIL","GENTS","GEREL","GESAN","GIMAT","GLBMD","GLRYH","GLYHO","GMTAS","GNDUZ",
    "GRSEL","GRTRK","GSDDE","GSDHO","GSRAY","GWIND","HEDEF","HZNDR","IDEAS","IDGYO",
    "IEYHO","IHEVA","IHGZT","IHLAS","IHLGM","IHYAY","INDES","INFO","INGRM","INVEO",
    "IPEKE","ISATR","ISBIR","ITTFK","IZFAS","IZMDC","JANTS","KAPLM","KAREL","KARSN",
    "KARTN","KATMR","KBORU","KENT","KERVN","KLKIM","KLMSN","KONKA","KONTR","KOPOL",
    "KORDS","KOZAA","KRDMA","KRDMB","KRONT","KRSAN","KSTUR","KUYAS","LIDER","LKMNH",
    "LUKSK","MACKO","MAKIM","MANAS","MARTI","MAVI","MEDTR","MEPET","MERCN","MERKO",
    "METRO","MIPAZ","MNDRS","MNVRL","MOBTL","MOGAN","MPARK","MRDIN","MRSHL","MSGYO",
    "MTRKS","NATEN","NBORU","NTGAZ","NTHOL","NUGYO","NUHCM","OBASE","ODAS","ORGE",
    "ORMA","OSTIM","PAMEL","PAPIL","PARSN","PASEU","PENGD","PENTA","PETUN","PINSU",
    "PKART","PLTUR","POLHO","POLTK","PRZMA","QNBFB","QNBFL","RAYSG","RHEAG","RNPOL",
    "RUBNS","SAFKR","SAMAT","SANEL","SANFM","SANKO","SARKY","SAYAS","SDTTR","SEGYO",
    "SEKFK","SEKUR","SELEC","SELGD","SELVA","SEYKM","SILVR","SKBNK","SMART","SMILE",
    "SNPAM","SONME","SUWEN","TARKM","TATEN","TATGD","TBORG","TEKTU","TGSAS","TIRE",
    "TMSN","TRCAS","TRILC","TSPOR","TUCLK","TUREX","TURSG","TUYAP","ULUUN","USAK",
    "VAKFN","VAKKO","VANGD","VBTYZ","VERUS","VKGYO","YAPRK","YATAS","YBTAS","YESIL",
    "YUNSA","ZOREN"
]

# ═══════════════════════════════════════════════
# VERİTABANI
# ═══════════════════════════════════════════════
def db_connect():
    if not DATABASE_URL:
        return None
    try:
        return psycopg2.connect(DATABASE_URL, sslmode='require')
    except Exception as e:
        print(f"DB hata: {e}")
        return None

def db_init():
    conn = db_connect()
    if not conn:
        print("DB yok – in-memory mod.")
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS store (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS price_cache (
                    ticker     TEXT PRIMARY KEY,
                    fetched_at DATE NOT NULL,
                    data       TEXT NOT NULL
                )
            """)
        conn.commit()
        print("DB hazir.")
    except Exception as e:
        print(f"DB init hata: {e}")
    finally:
        conn.close()

def db_get(key, default=None):
    conn = db_connect()
    if not conn:
        return default
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM store WHERE key=%s", (key,))
            row = cur.fetchone()
            return json.loads(row[0]) if row else default
    except Exception:
        return default
    finally:
        conn.close()

def db_set(key, value):
    conn = db_connect()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO store(key,value) VALUES(%s,%s)
                ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
            """, (key, json.dumps(value)))
        conn.commit()
    except Exception as e:
        print(f"DB set hata: {e}")
    finally:
        conn.close()

def pc_save(ticker, df):
    conn = db_connect()
    if not conn:
        return
    try:
        data = df.reset_index()
        data['datetime'] = data['datetime'].astype(str)
        payload = data.to_json(orient='records')
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO price_cache(ticker, fetched_at, data)
                VALUES(%s, CURRENT_DATE, %s)
                ON CONFLICT(ticker) DO UPDATE
                    SET fetched_at=CURRENT_DATE, data=EXCLUDED.data
            """, (ticker, payload))
        conn.commit()
    except Exception as e:
        print(f"pc_save hata {ticker}: {e}")
    finally:
        conn.close()

def pc_load(ticker):
    conn = db_connect()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT data, fetched_at FROM price_cache
                WHERE ticker=%s
            """, (ticker,))
            row = cur.fetchone()
            if not row:
                return None
            fetched_at = row[1]
            today      = datetime.now(pytz.timezone('Europe/Istanbul')).date()
            if fetched_at < today:
                return None
            records = json.loads(row[0])
            df = pd.DataFrame(records)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.set_index('datetime').sort_index()
            for col in ['Open','High','Low','Close','Volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
    except Exception as e:
        print(f"pc_load hata {ticker}: {e}")
        return None
    finally:
        conn.close()

def pc_count_today():
    conn = db_connect()
    if not conn:
        return 0
    try:
        today = datetime.now(pytz.timezone('Europe/Istanbul')).date()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM price_cache WHERE fetched_at=%s", (today,))
            return cur.fetchone()[0]
    except Exception:
        return 0
    finally:
        conn.close()

# ─── Watchlist ───────────────────────────────
_mem_watchlist = {}
_mem_emas      = {}

def wl_get(chat_id):
    return db_get(f"wl:{chat_id}", []) if DATABASE_URL else _mem_watchlist.get(str(chat_id), [])

def wl_set(chat_id, tickers):
    if DATABASE_URL:
        db_set(f"wl:{chat_id}", tickers)
    else:
        _mem_watchlist[str(chat_id)] = tickers

def wl_all_ids():
    if not DATABASE_URL:
        return list(_mem_watchlist.keys())
    conn = db_connect()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT key FROM store WHERE key LIKE 'wl:%'")
            return [r[0].replace('wl:','') for r in cur.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()

def ema_get(ticker):
    val = db_get(f"ema:{ticker}") if DATABASE_URL else _mem_emas.get(ticker)
    if isinstance(val, dict):
        return {
            "daily":  tuple(val.get("daily",  [9, 21])),
            "weekly": tuple(val.get("weekly", [9, 21])),
        }
    if isinstance(val, (list, tuple)) and len(val) == 2:
        return {"daily": tuple(val), "weekly": tuple(val)}
    return {"daily": (9, 21), "weekly": (9, 21)}

def ema_set(ticker, pairs_dict):
    payload = {
        "daily":  list(pairs_dict.get("daily",  (9,21))),
        "weekly": list(pairs_dict.get("weekly", (9,21))),
    }
    if DATABASE_URL:
        db_set(f"ema:{ticker}", payload)
    else:
        _mem_emas[ticker] = payload

# ═══════════════════════════════════════════════
# Flask – Webhook
# ═══════════════════════════════════════════════
@app.route('/')
def home():
    return "BIST Bot calisiyor.", 200

@app.route('/health')
def health():
    return "OK", 200

@app.route(f'/webhook/{TOKEN}', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        update = telebot.types.Update.de_json(request.get_data(as_text=True))
        threading.Thread(target=bot.process_new_updates, args=([update],), daemon=True).start()
        return 'OK', 200
    abort(403)

def set_webhook():
    if not RENDER_URL:
        print("HATA: RENDER_URL eksik!")
        return
    bot.remove_webhook()
    time.sleep(1)
    print(f"Webhook: {bot.set_webhook(url=f'{RENDER_URL}/webhook/{TOKEN}')}")

def keep_alive():
    if not RENDER_URL:
        return
    while True:
        try:
            requests.get(f"{RENDER_URL}/health", timeout=10)
        except Exception:
            pass
        time.sleep(14 * 60)

# ═══════════════════════════════════════════════
# Hesaplamalar
# ═══════════════════════════════════════════════
def calc_rsi(series, length=14):
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=length-1, min_periods=length).mean()
    avg_loss = loss.ewm(com=length-1, min_periods=length).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def detect_divergence(df, window=60, min_bars=5, max_bars=40):
    """
    RSI Pozitif (Bullish) ve Negatif (Bearish) uyumsuzluk tespiti.

    Bullish : Fiyat düşük dip  + RSI yüksek dip  → Yukarı dönüş sinyali
    Bearish : Fiyat yüksek tepe + RSI düşük tepe → Aşağı dönüş sinyali

    Döner: (divergence_type: str, message: str) veya (None, None)
    """
    try:
        if "RSI" not in df.columns:
            df = df.copy()
            df["RSI"] = calc_rsi(df["Close"], 14)

        df = df.dropna(subset=["Close", "RSI"]).tail(window)

        if len(df) < min_bars + 5:
            return None, None

        closes = df["Close"].values
        rsis   = df["RSI"].values
        last_idx = len(closes) - 1

        # ── Dip ve tepe noktaları ──
        price_lows,  _ = find_peaks(-closes, distance=min_bars)
        rsi_lows,    _ = find_peaks(-rsis,   distance=min_bars)
        price_highs, _ = find_peaks(closes,  distance=min_bars)
        rsi_highs,   _ = find_peaks(rsis,    distance=min_bars)

        # ── BULLISH DIVERGENCE ──
        recent_price_lows = price_lows[price_lows >= last_idx - max_bars]
        recent_rsi_lows   = rsi_lows[rsi_lows     >= last_idx - max_bars]

        if len(recent_price_lows) >= 2 and len(recent_rsi_lows) >= 2:
            p1, p2 = recent_price_lows[-2], recent_price_lows[-1]
            r1, r2 = recent_rsi_lows[-2],   recent_rsi_lows[-1]

            price_lower_low = closes[p2] < closes[p1]
            rsi_higher_low  = rsis[r2]   > rsis[r1]

            if price_lower_low and rsi_higher_low:
                strength = "GÜÇLÜ 🔥" if rsis[r2] < 40 else "ORTA"
                return (
                    "Bullish Diverjans",
                    f"📈 POZİTİF UYUMSUZLUK [{strength}]\n"
                    f"   Fiyat dip: {closes[p1]:.2f} → {closes[p2]:.2f} ↘\n"
                    f"   RSI  dip: {rsis[r1]:.1f} → {rsis[r2]:.1f} ↗\n"
                    f"   ⚡ Yukarı dönüş sinyali!"
                )

        # Tek dip – zayıf bullish
        elif len(recent_price_lows) >= 1 and len(recent_rsi_lows) >= 1:
            p2 = recent_price_lows[-1]
            r2 = recent_rsi_lows[-1]
            if p2 >= last_idx - 10 and rsis[r2] < 35:
                return (
                    "Bullish Diverjans",
                    f"📈 POZİTİF UYUMSUZLUK [ZAYIF]\n"
                    f"   RSI dip: {rsis[r2]:.1f} — Aşırı Satım bölgesi\n"
                    f"   ⚡ Toparlanma ihtimali"
                )

        # ── BEARISH DIVERGENCE ──
        recent_price_highs = price_highs[price_highs >= last_idx - max_bars]
        recent_rsi_highs   = rsi_highs[rsi_highs     >= last_idx - max_bars]

        if len(recent_price_highs) >= 2 and len(recent_rsi_highs) >= 2:
            p1, p2 = recent_price_highs[-2], recent_price_highs[-1]
            r1, r2 = recent_rsi_highs[-2],   recent_rsi_highs[-1]

            price_higher_high = closes[p2] > closes[p1]
            rsi_lower_high    = rsis[r2]   < rsis[r1]

            if price_higher_high and rsi_lower_high:
                strength = "GÜÇLÜ 🔥" if rsis[r2] > 60 else "ORTA"
                return (
                    "Bearish Diverjans",
                    f"📉 NEGATİF UYUMSUZLUK [{strength}]\n"
                    f"   Fiyat tepe: {closes[p1]:.2f} → {closes[p2]:.2f} ↗\n"
                    f"   RSI  tepe: {rsis[r1]:.1f} → {rsis[r2]:.1f} ↘\n"
                    f"   ⚡ Aşağı dönüş sinyali!"
                )

        # Tek tepe – zayıf bearish
        elif len(recent_price_highs) >= 1 and len(recent_rsi_highs) >= 1:
            p2 = recent_price_highs[-1]
            r2 = recent_rsi_highs[-1]
            if p2 >= last_idx - 10 and rsis[r2] > 65:
                return (
                    "Bearish Diverjans",
                    f"📉 NEGATİF UYUMSUZLUK [ZAYIF]\n"
                    f"   RSI tepe: {rsis[r2]:.1f} — Aşırı Alım bölgesi\n"
                    f"   ⚡ Düzeltme ihtimali"
                )

    except Exception as e:
        print(f"Diverjans hata: {e}")

    return None, None


# ═══════════════════════════════════════════════
# TwelveData – kredi sayacı farkında
# ═══════════════════════════════════════════════
def get_all_bist_tickers():
    return BIST_FALLBACK

def fetch_yahoo_direct(ticker, interval="1d", range_="2y"):
    try:
        url     = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}.IS?interval={interval}&range={range_}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json,text/html,*/*",
            "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://finance.yahoo.com/",
        }
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            return pd.DataFrame()
        data   = resp.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return pd.DataFrame()
        r          = result[0]
        timestamps = r.get("timestamp", [])
        ohlcv      = r.get("indicators", {}).get("quote", [{}])[0]
        if not timestamps:
            return pd.DataFrame()
        df = pd.DataFrame({
            "datetime": pd.to_datetime(timestamps, unit="s"),
            "Open":     ohlcv.get("open",   []),
            "High":     ohlcv.get("high",   []),
            "Low":      ohlcv.get("low",    []),
            "Close":    ohlcv.get("close",  []),
            "Volume":   ohlcv.get("volume", []),
        })
        df = df.set_index("datetime").sort_index()
        df.index = df.index.tz_localize(None)
        for col in ["Open","High","Low","Close","Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.dropna(subset=["Close"])
    except Exception as e:
        print(f"Yahoo direct hata {ticker}: {e}")
        return pd.DataFrame()

def fetch_alphavantage(ticker):
    av_key = (os.getenv("ALPHA_VANTAGE_KEY") or "").strip()
    if not av_key:
        return pd.DataFrame()
    try:
        url  = (
            f"https://www.alphavantage.co/query"
            f"?function=TIME_SERIES_DAILY"
            f"&symbol={ticker}.IS"
            f"&outputsize=full"
            f"&apikey={av_key}"
        )
        resp = requests.get(url, timeout=20).json()
        ts   = resp.get("Time Series (Daily)", {})
        if not ts:
            print(f"AV bos {ticker}: {list(resp.keys())}")
            return pd.DataFrame()
        rows = []
        for date_str, vals in ts.items():
            rows.append({
                "datetime": pd.to_datetime(date_str),
                "Open":   float(vals.get("1. open",  0) or 0),
                "High":   float(vals.get("2. high",  0) or 0),
                "Low":    float(vals.get("3. low",   0) or 0),
                "Close":  float(vals.get("4. close", 0) or 0),
                "Volume": float(vals.get("5. volume",0) or 0),
            })
        df = pd.DataFrame(rows).set_index("datetime").sort_index()
        df.index.name = "datetime"
        return df.dropna(subset=["Close"])
    except Exception as e:
        print(f"AlphaVantage hata {ticker}: {e}")
        return pd.DataFrame()

def resample_weekly(df_daily):
    if df_daily.empty:
        return pd.DataFrame()
    df = df_daily.resample("W").agg({
        "Open":   "first",
        "High":   "max",
        "Low":    "min",
        "Close":  "last",
        "Volume": "sum"
    }).dropna(subset=["Close"])
    df.index.name = "datetime"
    return df

def get_data(ticker):
    df_daily  = pd.DataFrame()
    df_weekly = pd.DataFrame()

    if DATABASE_URL:
        c = pc_load(f"d:{ticker}")
        if c is not None and not c.empty:
            df_daily = c
        c = pc_load(f"w:{ticker}")
        if c is not None and not c.empty:
            df_weekly = c

    if df_daily.empty:
        r = fetch_yahoo_direct(ticker, interval="1d", range_="2y")
        if isinstance(r, pd.DataFrame) and not r.empty:
            df_daily = r
            if DATABASE_URL: pc_save(f"d:{ticker}", r)

    if df_daily.empty:
        r = fetch_alphavantage(ticker)
        if isinstance(r, pd.DataFrame) and not r.empty:
            df_daily = r
            if DATABASE_URL: pc_save(f"d:{ticker}", r)

    if df_weekly.empty:
        r = fetch_yahoo_direct(ticker, interval="1wk", range_="5y")
        if isinstance(r, pd.DataFrame) and not r.empty:
            df_weekly = r
            if DATABASE_URL: pc_save(f"w:{ticker}", r)

    if df_weekly.empty and not df_daily.empty:
        df_weekly = resample_weekly(df_daily)

    return df_daily, df_weekly


def find_best_ema_pair(ticker, chat_id=None):
    df_daily, df_weekly = get_data(ticker)
    pairs  = [(3,8),(5,13),(8,21),(9,21),(12,26),(20,50),(10,30)]
    result = {}

    for label, df in [("daily", df_daily), ("weekly", df_weekly)]:
        if chat_id and is_cancelled(chat_id, "optimize"):
            return None
        min_bars = 50 if label == "weekly" else 100
        if not isinstance(df, pd.DataFrame) or df.empty or len(df) < min_bars:
            result[label] = (9, 21)
            continue
        best_profit, best_pair = -999.0, (9, 21)
        for short, long_ in pairs:
            if chat_id and is_cancelled(chat_id, "optimize"):
                return None
            tmp = df.copy()
            tmp["es"] = calc_ema(tmp["Close"], short)
            tmp["el"] = calc_ema(tmp["Close"], long_)
            tmp = tmp.dropna(subset=["es","el"])
            profit = 0.0; in_pos = False; entry = 0.0
            for i in range(1, len(tmp)):
                ep = tmp["es"].iloc[i-1]; lp = tmp["el"].iloc[i-1]
                ec = tmp["es"].iloc[i];   lc = tmp["el"].iloc[i]
                if not in_pos and ec > lc and ep <= lp:
                    in_pos = True; entry = tmp["Close"].iloc[i]
                elif in_pos and ec < lc and ep >= lp:
                    profit += (tmp["Close"].iloc[i] - entry) / entry * 100
                    in_pos = False
            if in_pos:
                profit += (tmp["Close"].iloc[-1] - entry) / entry * 100
            if profit > best_profit:
                best_profit, best_pair = profit, (short, long_)
        result[label] = best_pair

    return result


def send_long_message(chat_id, text):
    if len(text) <= 4000:
        bot.send_message(chat_id, text, parse_mode='Markdown'); return
    parts=[]; current=""
    for block in text.split('\n\n'):
        if len(current)+len(block)+2>4000:
            parts.append(current.strip()); current=block
        else:
            current+=block+'\n\n'
    if current:
        parts.append(current.strip())
    for part in parts:
        bot.send_message(chat_id, part, parse_mode='Markdown')
        time.sleep(0.5)

def scan_all_stocks(chat_id, limit=None, ticker_list=None):
    chat_id = str(chat_id)

    if ticker_list:
        tickers = ticker_list
    else:
        tickers = wl_get(chat_id)
        if not tickers:
            bot.send_message(chat_id, "📭 Liste boş! /addall yaz."); return
        if limit and limit < len(tickers):
            import random
            tickers = random.sample(tickers, limit)
            bot.send_message(chat_id, f"ℹ️ {limit} rastgele hisse taranacak.")

    total = len(tickers)

    cached  = [t for t in tickers if _db_cached_today(t)]
    missing = [t for t in tickers if t not in cached]
    to_fetch = missing

    bot.send_message(chat_id,
        f"🔍 *{total} hisse taranacak*\n"
        f"💾 Cache: {len(cached)} hazır | 📡 İndirilecek: {len(to_fetch)}\n"
        f"{'⏱ Tahmini: ~'+str(int(len(to_fetch)*TD_DELAY//60))+' dk' if to_fetch else '⚡ Anında basliyor!'}\n"
        f"🚫 İptal için: /iptal check"
    )

    fetched = 0
    for i, ticker in enumerate(to_fetch):
        if is_cancelled(chat_id, "check"):
            bot.send_message(chat_id, "İndirme iptal edildi."); return
        df_d, df_w = get_data(ticker)
        if (isinstance(df_d, pd.DataFrame) and not df_d.empty) or \
           (isinstance(df_w, pd.DataFrame) and not df_w.empty):
            fetched += 1
        if (i+1) % 20 == 0:
            bot.send_message(chat_id, f"📥 İndiriliyor: {i+1}/{len(to_fetch)} ({fetched} başarılı)")
        time.sleep(TD_DELAY)

    bot.send_message(chat_id, "✅ Veri hazır, analiz başlıyor...")

    messages = []; no_data = 0

    for ticker in tickers:
        if is_cancelled(chat_id, "check"):
            bot.send_message(chat_id, f"🚫 Tarama iptal edildi. ({len(messages)} sinyal bulunmuştu)")
            if messages:
                send_long_message(chat_id, "\n\n".join(messages))
            return
        try:
            df_d, df_w = get_data(ticker)

            has_daily  = isinstance(df_d, pd.DataFrame) and not df_d.empty and len(df_d) >= 10
            has_weekly = isinstance(df_w, pd.DataFrame) and not df_w.empty and len(df_w) >= 10
            if not has_daily and not has_weekly:
                no_data += 1; continue

            ep   = ema_get(ticker)
            ep_d = ep["daily"]
            ep_w = ep["weekly"]
            signals   = []
            rsi_lines = []

            # ══════════════════════════════
            # GÜNLÜK ANALİZ
            # ══════════════════════════════
            if has_daily:
                d = df_d.copy()

                # RSI — her zaman göster
                rsi_series = calc_rsi(d["Close"], 14).dropna()
                rsi_d = float(rsi_series.iloc[-1]) if len(rsi_series) > 0 else None

                if rsi_d is not None:
                    if rsi_d >= 70:
                        rsi_lines.append(f"RSI-G: {rsi_d:.1f} ⚠️ ASIRI ALIM")
                    elif rsi_d <= 30:
                        rsi_lines.append(f"RSI-G: {rsi_d:.1f} 💡 ASIRI SATIM")
                    else:
                        rsi_lines.append(f"RSI-G: {rsi_d:.1f}")

                # EMA — kesişim veya mevcut trend
                ema_s = calc_ema(d["Close"], ep_d[0])
                ema_l = calc_ema(d["Close"], ep_d[1])
                ef = pd.DataFrame({"s": ema_s, "l": ema_l}).dropna()

                if len(ef) >= 2:
                    cross_up   = ef["s"].iloc[-2] <= ef["l"].iloc[-2] and ef["s"].iloc[-1] > ef["l"].iloc[-1]
                    cross_down = ef["s"].iloc[-2] >= ef["l"].iloc[-2] and ef["s"].iloc[-1] < ef["l"].iloc[-1]
                    trend_up   = ef["s"].iloc[-1] > ef["l"].iloc[-1]
                    trend_down = ef["s"].iloc[-1] < ef["l"].iloc[-1]

                    if cross_up:
                        signals.append(f"🟢↑ Günlük AL — EMA Taze Kesişim ({ep_d[0]}/{ep_d[1]})")
                    elif cross_down:
                        signals.append(f"🔴↓ Günlük SAT — EMA Taze Kesişim ({ep_d[0]}/{ep_d[1]})")
                    elif trend_up and rsi_d and 40 <= rsi_d <= 65:
                        signals.append(f"📈 Günlük YUKARI TREND — EMA({ep_d[0]}>{ep_d[1]}) RSI Uyumlu")
                    elif trend_down and rsi_d and 35 <= rsi_d <= 60:
                        signals.append(f"📉 Günlük AŞAĞI TREND — EMA({ep_d[0]}<{ep_d[1]})")

                # Diverjans
                d["RSI"] = calc_rsi(d["Close"], 14)
                dt, dm = detect_divergence(d)
                if dt:
                    signals.append(dm)

            # ══════════════════════════════
            # HAFTALIK ANALİZ
            # ══════════════════════════════
            if has_weekly:
                w = df_w.copy()

                # RSI — her zaman göster
                rsi_series_w = calc_rsi(w["Close"], 14).dropna()
                rsi_w = float(rsi_series_w.iloc[-1]) if len(rsi_series_w) > 0 else None

                ema_sw = calc_ema(w["Close"], ep_w[0])
                ema_lw = calc_ema(w["Close"], ep_w[1])
                wf = pd.DataFrame({"s": ema_sw, "l": ema_lw}).dropna()
                trend_str = "YUKARI" if (len(wf) > 0 and wf["s"].iloc[-1] > wf["l"].iloc[-1]) else "ASAGI"

                if rsi_w is not None:
                    if rsi_w >= 70:
                        rsi_lines.append(f"RSI-H: {rsi_w:.1f} ⚠️ ASIRI ALIM | Trend:{trend_str}")
                    elif rsi_w <= 30:
                        rsi_lines.append(f"RSI-H: {rsi_w:.1f} 💡 ASIRI SATIM | Trend:{trend_str}")
                    else:
                        rsi_lines.append(f"RSI-H: {rsi_w:.1f} | Trend:{trend_str}")

                # EMA — kesişim veya mevcut trend
                if len(wf) >= 2:
                    cross_up_w   = wf["s"].iloc[-2] <= wf["l"].iloc[-2] and wf["s"].iloc[-1] > wf["l"].iloc[-1]
                    cross_down_w = wf["s"].iloc[-2] >= wf["l"].iloc[-2] and wf["s"].iloc[-1] < wf["l"].iloc[-1]
                    trend_up_w   = wf["s"].iloc[-1] > wf["l"].iloc[-1]
                    trend_down_w = wf["s"].iloc[-1] < wf["l"].iloc[-1]

                    if cross_up_w:
                        signals.append(f"🟢🟢↑↑ Haftalık AL — Taze Kesişim GÜÇLÜ ({ep_w[0]}/{ep_w[1]})")
                    elif cross_down_w:
                        signals.append(f"🔴🔴↓↓ Haftalık SAT — Taze Kesişim GÜÇLÜ ({ep_w[0]}/{ep_w[1]})")
                    elif trend_up_w and rsi_w and 40 <= rsi_w <= 65:
                        signals.append(f"📈📈 Haftalık YUKARI TREND — EMA Uyumlu (GÜÇLÜ)")
                    elif trend_down_w and rsi_w and 35 <= rsi_w <= 60:
                        signals.append(f"📉📉 Haftalık AŞAĞI TREND — EMA Uyumlu (GÜÇLÜ)")

                # Diverjans — haftalık için geniş pencere
                w["RSI"] = calc_rsi(w["Close"], 14)
                dt_w, dm_w = detect_divergence(w, window=80, min_bars=3, max_bars=60)
                if dt_w:
                    signals.append(f"[HAFTALIK] {dm_w}")

            # ══════════════════════════════
            # GÖSTER / GİZLE KOŞULU
            # ══════════════════════════════
            rsi_extreme = any("ASIRI" in r for r in rsi_lines)
            has_signal  = bool(signals)
            # Tek hisse sorgusunda (checksingle/check THYAO) her zaman göster
            force_show  = ticker_list is not None

            show = has_signal or rsi_extreme or force_show

            if show:
                vol = df_d["Close"].pct_change().std() * 100 if has_daily else 0
                msg_lines = [f"{'🔥' if vol>2 else '📌'} *{ticker}* {'(Yüksek Vol)' if vol>2 else ''}".strip()]
                if signals:
                    msg_lines += signals
                msg_lines += rsi_lines
                msg_lines.append(f"📐 EMA G:{ep_d[0]}-{ep_d[1]} | H:{ep_w[0]}-{ep_w[1]}")
                msg_lines.append(f"📈 [TradingView — {ticker}](https://tr.tradingview.com/chart/?symbol=BIST:{ticker})")
                messages.append("\n".join(msg_lines))

        except Exception as e:
            print(f"Scan hata {ticker}: {e}")
            continue

    if messages:
        send_long_message(chat_id, "\n\n".join(messages))
    else:
        bot.send_message(chat_id,
            f"✅ Tarama bitti. {total} hisse tarandı ({no_data} veri yok).\n"
            f"📭 Sinyal bulunamadı.")


def _db_cached_today(ticker):
    if not DATABASE_URL:
        return False
    conn = db_connect()
    if not conn:
        return False
    try:
        today = datetime.now(pytz.timezone('Europe/Istanbul')).date()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM price_cache WHERE ticker IN (%s, %s) AND fetched_at=%s LIMIT 1",
                (f"d:{ticker}", f"w:{ticker}", today)
            )
            return cur.fetchone() is not None
    except Exception:
        return False
    finally:
        conn.close()


def _run_optimize(chat_id, ticker):
    reset_cancel_flag(chat_id, "optimize")
    try:
        pairs = find_best_ema_pair(ticker, chat_id=chat_id)
        if pairs is None:
            bot.send_message(chat_id, f"{ticker} optimize iptal edildi.")
            return
        if not pairs or (pairs["daily"] == (9,21) and pairs["weekly"] == (9,21)):
            bot.send_message(chat_id, f"{ticker}: Yeterli veri yok, varsayilan 9-21 kullanildi.")
        else:
            ema_set(ticker, pairs)
            d = pairs["daily"]; w = pairs["weekly"]
            bot.send_message(chat_id,
                f"✅ *{ticker}* optimize tamamlandı:\n"
                f"📅 Günlük EMA: {d[0]}-{d[1]}\n"
                f"📆 Haftalık EMA: {w[0]}-{w[1]}\n"
                f"{'DB ye kaydedildi.' if DATABASE_URL else 'In-memory kaydedildi.'}")
    except Exception as e:
        bot.send_message(chat_id, f"Hata: {e}")

# ═══════════════════════════════════════════════
# Bot Komutları
# ═══════════════════════════════════════════════
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.reply_to(message,
        "📊 *BIST Teknik Analiz Botu*\n\n"
        "/check — Tüm listeyi tara\n"
        "/check THYAO — Tek hisse analiz\n"
        "/check 50 — Rastgele 50 hisse\n"
        "/checksingle THYAO — Detaylı debug çıktısı\n"
        "/optimize THYAO — EMA optimizasyonu\n"
        "/watchlist — İzleme listeni gör\n"
        "/addall — Tüm BIST hisselerini ekle\n"
        "/iptal optimize — Optimize iptal\n"
        "/iptal check — Tarama iptal"
    )

@bot.message_handler(commands=['iptal'])
def iptal(message):
    chat_id = str(message.chat.id)
    parts   = message.text.strip().split()
    op      = parts[1].lower() if len(parts) > 1 else "check"
    get_cancel_flag(chat_id, op).set()
    bot.reply_to(message, f"🚫 '{op}' işlemi iptal sinyali gönderildi.")

@bot.message_handler(commands=['addall'])
def add_all(message):
    chat_id = str(message.chat.id)
    bot.reply_to(message, "📋 Tüm BIST hisseleri listeye ekleniyor...")
    tickers = get_all_bist_tickers()
    wl_set(chat_id, tickers)
    bot.send_message(chat_id, f"✅ {len(tickers)} hisse eklendi.")

@bot.message_handler(commands=['add'])
def add_ticker(message):
    chat_id = str(message.chat.id)
    parts   = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(message, "Kullanim: /add THYAO"); return
    ticker  = parts[1].upper().replace(".IS","")
    lst     = wl_get(chat_id)
    if ticker not in lst:
        lst.append(ticker)
        wl_set(chat_id, lst)
        bot.reply_to(message, f"✅ {ticker} eklendi. ({len(lst)} hisse)")
    else:
        bot.reply_to(message, f"ℹ️ {ticker} zaten listede.")

@bot.message_handler(commands=['remove'])
def remove_ticker(message):
    chat_id = str(message.chat.id)
    parts   = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(message, "Kullanim: /remove THYAO"); return
    ticker  = parts[1].upper().replace(".IS","")
    lst     = wl_get(chat_id)
    if ticker in lst:
        lst.remove(ticker)
        wl_set(chat_id, lst)
        bot.reply_to(message, f"🗑 {ticker} silindi. ({len(lst)} hisse)")
    else:
        bot.reply_to(message, f"ℹ️ {ticker} listede yok.")

@bot.message_handler(commands=['optimize'])
def optimize(message):
    try:
        ticker  = message.text.split()[1].upper().replace('.IS','')
        chat_id = str(message.chat.id)
        bot.reply_to(message,
            f"⚙️ *{ticker}* optimize başlıyor...\n"
            f"🚫 İptal için: /iptal optimize")
        threading.Thread(target=_run_optimize, args=(chat_id, ticker), daemon=True).start()
    except IndexError:
        bot.reply_to(message, "Kullanim: /optimize THYAO")
    except Exception as e:
        bot.reply_to(message, f"Hata: {e}")

@bot.message_handler(commands=['checksingle'])
def check_single(message):
    try:
        ticker = message.text.split()[1].upper().replace('.IS','')
    except IndexError:
        bot.reply_to(message, "Kullanim: /checksingle THYAO"); return

    bot.reply_to(message, f"🔍 {ticker} analiz ediliyor...")
    df_d, df_w = get_data(ticker)
    lines = []

    if isinstance(df_d, pd.DataFrame) and not df_d.empty:
        lines.append(f"📅 Günlük veri: {len(df_d)} bar")
        rsi_s = calc_rsi(df_d["Close"], 14).dropna()
        lines.append(f"RSI serisi uzunluk: {len(rsi_s)}")
        if len(rsi_s) > 0:
            lines.append(f"RSI-G son değer: {rsi_s.iloc[-1]:.2f}")
        else:
            lines.append("RSI-G: HESAPLANAMADI")
        lines.append(f"Close son: {df_d['Close'].iloc[-1]:.2f}")

        # Diverjans testi
        df_d["RSI"] = calc_rsi(df_d["Close"], 14)
        dt, dm = detect_divergence(df_d)
        if dt:
            lines.append(f"--- Günlük Diverjans ---")
            lines.append(dm)
        else:
            lines.append("Günlük Diverjans: YOK")
    else:
        lines.append("📅 Günlük veri: ❌ YOK")

    if isinstance(df_w, pd.DataFrame) and not df_w.empty:
        lines.append(f"📆 Haftalık veri: {len(df_w)} bar")
        rsi_sw = calc_rsi(df_w["Close"], 14).dropna()
        lines.append(f"RSI-H serisi uzunluk: {len(rsi_sw)}")
        if len(rsi_sw) > 0:
            lines.append(f"RSI-H son değer: {rsi_sw.iloc[-1]:.2f}")
        else:
            lines.append("RSI-H: HESAPLANAMADI")

        # Haftalık diverjans testi
        df_w["RSI"] = calc_rsi(df_w["Close"], 14)
        dt_w, dm_w = detect_divergence(df_w, window=80, min_bars=3, max_bars=60)
        if dt_w:
            lines.append(f"--- Haftalık Diverjans ---")
            lines.append(dm_w)
        else:
            lines.append("Haftalık Diverjans: YOK")
    else:
        lines.append("📆 Haftalık veri: ❌ YOK")

    ep = ema_get(ticker)
    lines.append(f"EMA G:{ep['daily']} H:{ep['weekly']}")

    bot.send_message(str(message.chat.id), "\n".join(lines))

@bot.message_handler(commands=['check'])
def manual_check(message):
    chat_id = str(message.chat.id)
    parts   = message.text.strip().split()

    if len(parts) > 1 and not parts[1].isdigit():
        ticker = parts[1].upper().replace(".IS","")
        reset_cancel_flag(chat_id, "check")
        threading.Thread(target=scan_all_stocks, args=(chat_id, None, [ticker]), daemon=True).start()
        return

    if not wl_get(chat_id):
        bot.reply_to(message, "📭 Liste boş! Önce /addall yaz."); return

    limit = None
    if len(parts) > 1:
        try:
            limit = int(parts[1])
        except ValueError:
            bot.reply_to(message, "Kullanım: /check | /check 50 | /check THYAO"); return

    reset_cancel_flag(chat_id, "check")
    threading.Thread(target=scan_all_stocks, args=(chat_id, limit), daemon=True).start()

@bot.message_handler(commands=['watchlist'])
def show_list(message):
    chat_id = str(message.chat.id)
    lst = wl_get(chat_id)
    if lst:
        send_long_message(chat_id, f"📋 *İzleme Listen* ({len(lst)} hisse):\n" + "\n".join(lst))
    else:
        bot.reply_to(message, "📭 Liste boş — /addall yaz")

# ═══════════════════════════════════════════════
# Otomatik tarama – her gün 18:05
# ═══════════════════════════════════════════════
def auto_scan():
    tr_tz = pytz.timezone('Europe/Istanbul')
    scanned_date = None
    while True:
        now = datetime.now(tr_tz)
        if now.hour == 18 and now.minute >= 5 and now.minute < 10 and scanned_date != now.date():
            scanned_date = now.date()
            for chat_id in wl_all_ids():
                scan_all_stocks(chat_id)
        time.sleep(60)

if __name__ == "__main__":
    print(f"BIST Bot baslatiliyor - PORT={PORT}")
    db_init()
    set_webhook()
    threading.Thread(target=keep_alive, daemon=True).start()
    threading.Thread(target=auto_scan,  daemon=True).start()
    app.run(host='0.0.0.0', port=PORT)
