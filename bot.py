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

# ~550 hisse – Borsa İstanbul Pay Piyasası tam liste (2025)
_BIST_RAW = [
    # ── A ──
    "A1CAP","A1YEN","ACSEL","ADEL","ADANA","ADESE","ADGYO","AEFES","AFYON","AGESA",
    "AGHOL","AGROT","AHGAZ","AHSGY","AKENR","AKBNK","AKCNS","AKFGY","AKFIN","AKFIS",
    "AKFYE","AKGRT","AKHAN","AKMGY","AKPAZ","AKSA","AKSEN","AKSGY","AKSUE","AKTIF",
    "AKYHO","ALARK","ALBRK","ALCAR","ALCTL","ALFAS","ALGYO","ALKA","ALKIM","ALKLC",
    "ALMAD","ALNTF","ALPAY","ALTES","ALTIN","ALTNY","ALVES","AMBRA","AMTEK","ANACM",
    "ANELE","ANGEN","ANHYT","ANSGR","ANTKS","ARASE","ARCLK","ARDYZ","ARENA","ARFYE",
    "ARFYO","ARMGD","ARSAN","ARTMS","ARZUM","ASCEL","ASGYO","ASELS","ASKLR","ASPILSAN",
    "ASRNC","ASTOR","ATAGY","ATAKP","ATATP","ATATR","ATEKS","ATLAS","ATPET","ATSYH",
    "AVGYO","AVHOL","AVOD","AVPGY","AVTUR","AYCES","AYDEM","AYEN","AYNES","AYES",
    "AYGAZ","AZTEK","ASUZU",
    # ── B ──
    "BAGFS","BAHKM","BAKAB","BAKLK","BALAT","BALSU","BANBO","BANVT","BARMA","BASCM",
    "BASGZ","BATISOKE","BAYRK","BEGYO","BERA","BESLR","BESTE","BEYAZ","BFREN","BIMAS","BIOEN","BIRTO","BJKAS","BKFIN",
    "BLCYT","BMELK","BMSTL","BNTAS","BOSSA","BOYP","BOZK","BRLSM","BRISA","BRKSN",
    "BRSAN","BRSHP","BRYAT","BRYYH","BSOKE","BTCIM","BUCIM","BURCE","BURVA","BVSAN",
    "BYDNR","BYME","BYNDR","BIGGS",
    # ── C ──
    "CAFER","CANTE","CARFA","CARSA","CASA","CATES","CCOLA","CEMAS","CEMTS","CEPHE",
    "CEREN","CGCAM","CGINR","CGMYO","CHEFS","CIMSA","CLEBI","COFOR","CRDFA","CRFSA",
    "CRPCA","CUSAN","CVKMD","CWENE",
    # ── D ──
    "DAGHL","DARDL","DATNS","DENGE","DERI","DERHL","DERIM","DESA","DESAS","DESPC",
    "DEVA","DGATE","DIRIT","DITAS","DMSAS","DNISI","DOAS","DOBUR","DOGUB","DOHOL","DOKTA",
    "DORAY","DORTS","DPENS","DRTST","DURDO","DYOBY","DZGYO",
    # ── E ──
    "EAPRO","EBEBK","ECILC","ECZYT","EDIP","EFOR","EGGUB","EGEEN","EGPRO","EGSER",
    "EKGYO","EKIZ","EKOS","ELITE","EMKEL","EMNIS","EMPA","ENCTR","ENDL","ENERY",
    "ENJSA","ENKAI","EPLAS","EREGL",
    "ERSU","ESCOM","ESEN","ESKYP","ETYAT","EUHOL","EUPWR","EYGYO","EZRMK",
    # ── F ──
    "FADE","FENER","FLAP","FMIZP","FONET","FORTE","FRIGO","FROTO","FZLGY",
    # ── G ──
    "GARAN","GARFA","GBOOK","GEDIK","GEDZA","GENIL","GENTS","GEREL","GESAN","GIMAT",
    "GLBMD","GLRYH","GLYHO","GMTAS","GNDUZ","GOKNR","GOLDS","GOLTS","GOODY","GRSEL",
    "GRTRK","GRTHO","GSDDE","GSDHO","GSRAY","GUBRF","GWIND","GZNMI",
    # ── H ──
    "HALKB","HATEK","HEDEF","HEKTS","HKTM","HLGYO","HOROZ","HRKET","HTTBT","HUBVC",
    "HURGZ","HZNDR",
    # ── I ──
    "ICBCT","IDEAS","IDGYO","IEYHO","IHEVA","IHGZT","IHLAS","IHLGM","IHYAY","IMASM",
    "IMBAT","INDES","INFO","INGRM","INTEM","INVEO","IPEKE","ISATR","ISBIR","ISDMR",
    "ISFIN","ISGSY","ISGYO","ISCTR","ISMEN","ISYAT","ITTFK","IZFAS","IZMDC","IZTAR",
    # ── J ──
    "JANTS",
    # ── K ──
    "KAPLM","KAREL","KARSN","KARTN","KATMR","KAYSE","KBORU","KCAER","KENT","KERVN",
    "KERVT","KCHOL","KIMMR","KLKIM","KLMSN","KLNMA","KNFRT","KONKA","KONTR","KOPOL",
    "KONYA","KORDS","KOZAA","KOZAL","KRDMA","KRDMB","KRDMD","KRONT","KRPLS","KRSAN",
    "KSTUR","KTLEV","KUYAS","KZBGY",
    # ── L ──
    "LIDER","LIDFA","LKMNH","LMKDC","LOGO","LUKSK",
    # ── M ──
    "MACKO","MAGEN","MAKIM","MANAS","MARTI","MAVI","MEDTR","MEPET","MERCN","MERKO",
    "METRO","METUR","MGROS","MIATK","MIPAZ","MNDRS","MNVRL","MOBTL","MOGAN","MPARK",
    "MRGYO","MRDIN","MRSHL","MSGYO","MTRKS","MZHLD",
    # ── N ──
    "NATEN","NBORU","NETAS","NTGAZ","NTHOL","NTTUR","NUGYO","NUHCM",
    # ── O ──
    "OBAMS","OBASE","ODAS","ODEAB","OFSYM","ONCSM","ORCAY","ORGE","ORMA","OSMEN","OSTIM",
    "OTKAR","OTTO","OYAKC","OYAYO","OZKGY",
    # ── P ──
    "PAMEL","PAPIL","PARSN","PASEU","PCILT","PEKGY","PENGD","PENTA","PETKM","PETUN",
    "PGSUS","PINSU","PKART","PLTUR","PNLSN","POLHO","POLTK","PRZMA","PSDTC",
    # ── Q ──
    "QNBFB","QNBFL",
    # ── R ──
    "RALYH","RAYSG","RCAST","REEDR","RHEAG","RNPOL","RODRG","ROYAL","RUBNS","RYGYO",
    "RYSAS",
    # ── S ──
    "SAFKR","SAHOL","SAMAT","SANEL","SANFM","SANKO","SARKY","SASA","SAYAS","SDTTR",
    "SEGYO","SEKFK","SEKUR","SELEC","SELGD","SELVA","SEYKM","SILVR","SISE","SKBNK",
    "SMART","SMRTG","SMILE","SNPAM","SODA","SOKM","SONME","SUWEN",
    # ── T ──
    "TABGD","TARKM","TATEN","TATGD","TAVHL","TBORG","TCELL","TDGYO","TEDU","TEKTU",
    "TEZOL","TFAC","TGSAS","THYAO","TIRE","TKFEN","TMSN","TOASO","TPVST","TRALT",
    "TRCAS","TRENJ","TRMET","TRILC","TSGYO","TSKB","TSPOR","TTKOM","TTRAK","TUCLK",
    "TUPRS","TUREX","TURGG","TURSG","TUYAP",
    # ── U ──
    "UCAK","ULKER","ULAS","ULUSE","ULUFA","ULUUN","UNLU","USAK","USDMR","UZERB",
    # ── V ──
    "VAKBN","VAKFN","VAKKO","VANGD","VBTYZ","VESTL","VERUS","VERTU","VKFYO","VKGYO",
    # ── W ──
    "WISD","WNDYR",
    # ── Y ──
    "YAPRK","YKBNK","YATAS","YBTAS","YESIL","YEOTK","YGGYO","YGYO","YKSLN","YUNSA",
    "YYAPI",
    # ── Z ──
    "ZEDUR","ZOREN","ZRGYO",
]
BIST_FALLBACK = sorted(list(set(_BIST_RAW)))




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
    _set_bot_commands()

def _set_bot_commands():
    """Telegram '/' menüsündeki komut listesini günceller."""
    commands = [
        telebot.types.BotCommand("check",        "Tara: /check all | /check 50 | /check THYAO"),
        telebot.types.BotCommand("checksingle",  "Detaylı debug analizi"),
        telebot.types.BotCommand("addall",        "BIST hisselerini ekle"),
        telebot.types.BotCommand("refreshlist",  "Yahoo'dan güncel listeyi çek"),
        telebot.types.BotCommand("add",          "Tek hisse ekle: /add THYAO"),
        telebot.types.BotCommand("remove",       "Tek hisse çıkar: /remove THYAO"),
        telebot.types.BotCommand("watchlist",    "İzleme listesini gör"),
        telebot.types.BotCommand("optimize",     "Tek hisse EMA optimize"),
        telebot.types.BotCommand("optimizeall",  "Tüm listeyi optimize et"),
        telebot.types.BotCommand("backup",       "Veriyi JSON olarak yedekle"),
        telebot.types.BotCommand("loadbackup",   "JSON yedekten geri yükle"),
        telebot.types.BotCommand("status",       "Bot sağlık durumu"),
        telebot.types.BotCommand("iptal",        "İşlemi durdur: /iptal check"),
        telebot.types.BotCommand("help",         "Tüm komutları göster"),
    ]
    try:
        bot.set_my_commands(commands)
        print("Bot komut listesi güncellendi.")
    except Exception as e:
        print(f"Komut listesi güncellenemedi: {e}")

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

def calc_volume_momentum(df, vol_period=20, mom_period_fast=5, mom_period_slow=10):
    """
    Hacim ve momentum analizi.

    Döner dict:
      vol_ratio     : son bar hacmi / 20 bar ortalama hacim
      vol_trend     : 'YUKARI' | 'ASAGI' | 'NOTR'  (son 5 barın hacim eğilimi)
      buy_pressure  : son 10 barda yükselen günlerin hacim oranı (0-1)
      mom_fast      : 5 günlük fiyat değişim % (ROC)
      mom_slow      : 10 günlük fiyat değişim % (ROC)
      obv_trend     : 'YUKARI' | 'ASAGI' | 'NOTR'  (OBV eğilimi)
      confirm_buy   : True/False  (AL sinyalini destekliyor mu?)
      confirm_sell  : True/False  (SAT sinyalini destekliyor mu?)
      summary       : okunabilir metin
    """
    result = {
        "vol_ratio": None, "vol_trend": "NOTR",
        "buy_pressure": None, "mom_fast": None, "mom_slow": None,
        "obv_trend": "NOTR", "confirm_buy": False, "confirm_sell": False,
        "summary": "❓ Hacim verisi yok"
    }
    try:
        if "Volume" not in df.columns or len(df) < vol_period + 2:
            return result

        close  = df["Close"]
        volume = df["Volume"].replace(0, np.nan)

        # ── Hacim oranı ──────────────────────────────
        avg_vol   = volume.iloc[-(vol_period+1):-1].mean()
        last_vol  = volume.iloc[-1]
        vol_ratio = last_vol / avg_vol if (avg_vol and avg_vol > 0) else None
        result["vol_ratio"] = vol_ratio

        # ── Hacim trendi (son 5 bar lineer regresyon eğimi) ──
        recent_vol = volume.dropna().iloc[-5:]
        if len(recent_vol) >= 3:
            x = np.arange(len(recent_vol))
            slope = np.polyfit(x, recent_vol.values, 1)[0]
            norm  = recent_vol.mean()
            if norm > 0:
                slope_pct = slope / norm * 100
                if slope_pct > 5:
                    result["vol_trend"] = "YUKARI"
                elif slope_pct < -5:
                    result["vol_trend"] = "ASAGI"

        # ── Alım/Satım baskısı (son 10 barda yukarı kapanan günlerin hacim ağırlığı) ──
        last10 = df.iloc[-10:].copy()
        last10["up"] = last10["Close"] > last10["Close"].shift(1)
        up_vol   = last10.loc[last10["up"], "Volume"].sum()
        down_vol = last10.loc[~last10["up"], "Volume"].sum()
        total_vol = up_vol + down_vol
        if total_vol > 0:
            result["buy_pressure"] = up_vol / total_vol

        # ── Momentum (ROC) ──────────────────────────
        if len(close) > mom_period_slow + 1:
            mom_fast = (close.iloc[-1] / close.iloc[-mom_period_fast] - 1) * 100
            mom_slow = (close.iloc[-1] / close.iloc[-mom_period_slow] - 1) * 100
            result["mom_fast"] = round(mom_fast, 2)
            result["mom_slow"] = round(mom_slow, 2)

        # ── OBV trendi ──────────────────────────────
        obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
        obv_recent = obv.iloc[-10:]
        if len(obv_recent) >= 5:
            x = np.arange(len(obv_recent))
            obv_slope = np.polyfit(x, obv_recent.values, 1)[0]
            obv_mean  = abs(obv_recent.mean())
            if obv_mean > 0:
                obv_slope_pct = obv_slope / obv_mean * 100
                if obv_slope_pct > 2:
                    result["obv_trend"] = "YUKARI"
                elif obv_slope_pct < -2:
                    result["obv_trend"] = "ASAGI"

        # ── Onay mantığı ──────────────────────────────
        vr    = vol_ratio or 0
        bp    = result["buy_pressure"] or 0.5
        mf    = result["mom_fast"] or 0
        ms    = result["mom_slow"] or 0
        obv_t = result["obv_trend"]
        v_t   = result["vol_trend"]

        # AL onayı: hacim ortalama üstünde + alım baskısı > %55 + momentum pozitif + OBV yukarı
        buy_score  = (
            (1 if vr >= 1.2 else 0) +
            (1 if bp >= 0.55 else 0) +
            (1 if mf > 0 else 0) +
            (1 if ms > 0 else 0) +
            (1 if obv_t == "YUKARI" else 0)
        )
        # SAT onayı: hacim ortalama üstünde + satım baskısı > %55 + momentum negatif + OBV aşağı
        sell_score = (
            (1 if vr >= 1.2 else 0) +
            (1 if bp <= 0.45 else 0) +
            (1 if mf < 0 else 0) +
            (1 if ms < 0 else 0) +
            (1 if obv_t == "ASAGI" else 0)
        )
        result["confirm_buy"]  = buy_score  >= 3
        result["confirm_sell"] = sell_score >= 3

        # ── Özet metin ──────────────────────────────
        vol_str = f"{vr:.1f}x ort" if vr else "?"
        vol_icon = "🔥" if vr and vr >= 1.5 else ("📊" if vr and vr >= 0.8 else "🔇")
        bp_str  = f"%{bp*100:.0f} alım" if result["buy_pressure"] is not None else "?"
        mf_str  = f"{mf:+.1f}%" if result["mom_fast"] is not None else "?"
        ms_str  = f"{ms:+.1f}%" if result["mom_slow"] is not None else "?"
        obv_str = "📈OBV" if obv_t == "YUKARI" else ("📉OBV" if obv_t == "ASAGI" else "➡️OBV")

        result["summary"] = (
            f"{vol_icon} Hacim:{vol_str} | {bp_str} | "
            f"Mom:{mf_str}(5g)/{ms_str}(10g) | {obv_str}"
        )

    except Exception as e:
        result["summary"] = f"❓ Hacim/momentum hata: {e}"

    return result

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
def fetch_bist_tickers_yahoo():
    """
    Yahoo Finance query1 + toplu quote doğrulama.
    Render IP'leri screener'ı bloklasa da quote endpoint'i genellikle çalışır.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8",
        "Referer": "https://finance.yahoo.com/",
    }
    results = set()

    # ── Yöntem 1: query1 + .IS suffix araması ──
    letters = "ABCDEFGHIJKLMNOPRSTUVYZ"
    for letter in letters:
        try:
            url = (
                f"https://query1.finance.yahoo.com/v1/finance/search"
                f"?q={letter}.IS&lang=en-US&region=TR"
                f"&quotesCount=20&newsCount=0&enableFuzzyQuery=false"
            )
            resp = requests.get(url, headers=headers, timeout=8)
            if resp.status_code == 200:
                quotes = resp.json().get("quotes", [])
                batch = [
                    q["symbol"].replace(".IS", "")
                    for q in quotes
                    if q.get("symbol", "").endswith(".IS")
                    and q.get("quoteType") == "EQUITY"
                ]
                results.update(batch)
        except Exception:
            pass
        time.sleep(0.2)

    # ── Yöntem 2: Toplu quote doğrulama (en güvenilir yöntem) ──
    # Tüm bilinen listeyi 50'şer gruplarla sorgular — çalışanları onaylar
    try:
        chunk_size = 50
        all_known = sorted(list(set(list(results) + BIST_FALLBACK)))
        for i in range(0, len(all_known), chunk_size):
            chunk   = all_known[i:i+chunk_size]
            symbols = ",".join([f"{t}.IS" for t in chunk])
            url2    = (
                f"https://query1.finance.yahoo.com/v7/finance/quote"
                f"?symbols={symbols}&lang=en-US&region=TR"
            )
            resp2 = requests.get(url2, headers=headers, timeout=12)
            if resp2.status_code == 200:
                qr = resp2.json().get("quoteResponse", {}).get("result", [])
                batch = [q["symbol"].replace(".IS", "") for q in qr
                         if q.get("symbol", "").endswith(".IS")]
                results.update(batch)
            time.sleep(0.3)
    except Exception as e:
        print(f"Yahoo quote bulk hata: {e}")

    return sorted(results)


def get_all_bist_tickers():
    """
    1. DB'de kayıtlı güncel liste varsa onu döner
    2. Yoksa Yahoo Finance'den çekmeye çalışır
    3. O da başarısızsa BIST_FALLBACK'e döner
    """
    # DB'den kayıtlı master liste var mı?
    if DATABASE_URL:
        saved = db_get("master_ticker_list")
        if saved and isinstance(saved, list) and len(saved) > 100:
            return saved

    live = fetch_bist_tickers_yahoo()
    if len(live) > 100:
        if DATABASE_URL:
            db_set("master_ticker_list", live)
        return live

    return BIST_FALLBACK


def _run_refreshlist(chat_id):
    """
    Yahoo Finance'den güncel BIST listesini çekip DB'ye kaydeder.
    Mevcut watchlist'e eksik olanları ekler.
    """
    bot.send_message(chat_id,
        "🔄 *Liste Yenileniyor...*\n"
        "3 farklı kaynak deneniyor:\n"
        "1️⃣ Yahoo Finance query1\n"
        "2️⃣ Yahoo Finance toplu quote\n"
        "3️⃣ KAP API\n"
        "⏱ ~30-45 saniye sürebilir..."
    )
    try:
        live = fetch_bist_tickers_yahoo()

        # Yahoo sonuçlarını her zaman fallback ile birleştir
        merged_master = sorted(list(set(live) | set(BIST_FALLBACK)))

        if len(live) < 50:
            bot.send_message(chat_id,
                f"⚠️ Yahoo Finance az sonuç döndürdü ({len(live)}).\n"
                f"📋 Dahili liste ile birleştirildi: *{len(merged_master)} hisse*"
            )
        else:
            if DATABASE_URL:
                db_set("master_ticker_list", merged_master)
            bot.send_message(chat_id,
                f"✅ Yahoo Finance: *{len(live)} hisse* bulundu.\n"
                f"📋 Dahili liste ile birleştirildi: *{len(merged_master)} hisse*"
            )

        # Mevcut watchlist ile karşılaştır
        current     = wl_get(chat_id)
        current_set = set(current)
        new_ones    = [t for t in merged_master if t not in current_set]

        if new_ones:
            final = sorted(list(current_set | set(merged_master)))
            wl_set(chat_id, final)
            bot.send_message(chat_id,
                f"📥 *{len(new_ones)} yeni hisse* watchlist'e eklendi!\n"
                f"📋 Toplam: *{len(final)} hisse*\n\n"
                f"Yeni eklenenler (ilk 30):\n" +
                ", ".join(new_ones[:30]) +
                (f"\n...ve {len(new_ones)-30} tane daha" if len(new_ones) > 30 else "")
            )
        else:
            bot.send_message(chat_id,
                f"✅ Liste güncel! {len(current)} hisse zaten ekli, yeni hisse yok."
            )

    except Exception as e:
        bot.send_message(chat_id, f"❌ Hata: {e}")


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
    # 20 EMA çifti – kısa/orta/uzun vadeli + Fibonacci bazlı
    pairs = [
        # Kısa vadeli
        (3,8),(4,9),(5,10),(5,13),(6,13),
        # Orta vadeli
        (8,21),(9,21),(10,21),(10,26),(12,26),
        (13,34),(14,28),(10,30),(15,30),
        # Uzun vadeli
        (20,50),(21,55),(20,60),(21,89),(50,200),
        # Fibonacci bazlı
        (8,13),(13,21),
    ]
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
            # HACİM & MOMENTUM ANALİZİ
            # ══════════════════════════════
            vm = {}
            if has_daily:
                vm = calc_volume_momentum(df_d)

            # ══════════════════════════════
            # GÖSTER / GİZLE KOŞULU
            # ══════════════════════════════
            rsi_extreme = any("ASIRI" in r for r in rsi_lines)
            has_signal  = bool(signals)
            force_show  = ticker_list is not None

            show = has_signal or rsi_extreme or force_show

            if show:
                vol = df_d["Close"].pct_change().std() * 100 if has_daily else 0
                msg_lines = [f"{'🔥' if vol>2 else '📌'} *{ticker}* {'(Yüksek Vol)' if vol>2 else ''}".strip()]

                # Sinyallere hacim/momentum onayı ekle
                enriched_signals = []
                for sig in signals:
                    is_buy  = any(k in sig for k in ["AL","YUKARI","POZİTİF"])
                    is_sell = any(k in sig for k in ["SAT","AŞAĞI","NEGATİF"])
                    if vm and is_buy:
                        if vm.get("confirm_buy"):
                            sig += "\n   ✅ Hacim & Momentum DESTEKLEYOR"
                        else:
                            warnings = []
                            vr = vm.get("vol_ratio")
                            bp = vm.get("buy_pressure")
                            mf = vm.get("mom_fast")
                            obv = vm.get("obv_trend")
                            if vr is not None and vr < 0.8:
                                warnings.append("hacim düşük")
                            if bp is not None and bp < 0.5:
                                warnings.append("satım baskısı var")
                            if mf is not None and mf < 0:
                                warnings.append(f"momentum negatif ({mf:+.1f}%)")
                            if obv == "ASAGI":
                                warnings.append("OBV aşağı")
                            if warnings:
                                sig += f"\n   ⚠️ Zayıf: {', '.join(warnings)}"
                            else:
                                sig += "\n   🔶 Hacim/Momentum nötr"
                    elif vm and is_sell:
                        if vm.get("confirm_sell"):
                            sig += "\n   ✅ Hacim & Momentum DESTEKLEYOR"
                        else:
                            warnings = []
                            vr = vm.get("vol_ratio")
                            bp = vm.get("buy_pressure")
                            mf = vm.get("mom_fast")
                            obv = vm.get("obv_trend")
                            if vr is not None and vr < 0.8:
                                warnings.append("hacim düşük")
                            if bp is not None and bp > 0.5:
                                warnings.append("alım baskısı hâlâ var")
                            if mf is not None and mf > 0:
                                warnings.append(f"momentum pozitif ({mf:+.1f}%)")
                            if obv == "YUKARI":
                                warnings.append("OBV yukarı")
                            if warnings:
                                sig += f"\n   ⚠️ Zayıf: {', '.join(warnings)}"
                            else:
                                sig += "\n   🔶 Hacim/Momentum nötr"
                    enriched_signals.append(sig)

                if enriched_signals:
                    msg_lines += enriched_signals
                msg_lines += rsi_lines

                # Hacim/momentum özet satırı
                if vm and vm.get("summary"):
                    msg_lines.append(vm["summary"])

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

def _run_optimizeall(chat_id):
    """Tüm watchlist'i sırayla optimize eder. Her 10 hissede ilerleme mesajı gönderir."""
    reset_cancel_flag(chat_id, "optimizeall")
    tickers = wl_get(chat_id)
    if not tickers:
        bot.send_message(chat_id, "📭 Liste boş! Önce /addall yaz."); return

    total    = len(tickers)
    done     = 0
    skipped  = 0
    improved = 0

    bot.send_message(chat_id,
        f"⚙️ *Toplu Optimize Başlıyor*\n"
        f"📋 {total} hisse sırayla optimize edilecek\n"
        f"⏱ Tahmini süre: ~{total*25//3600}sa {(total*25%3600)//60}dk\n"
        f"🚫 Durdurmak için: /iptal optimizeall"
    )

    for i, ticker in enumerate(tickers):
        # İptal kontrolü
        if is_cancelled(chat_id, "optimizeall"):
            bot.send_message(chat_id,
                f"🚫 Toplu optimize durduruldu.\n"
                f"✅ Tamamlanan: {done} | ⏭ Atlanan: {skipped} | 📈 İyileştirilen: {improved}")
            return

        try:
            pairs = find_best_ema_pair(ticker, chat_id=chat_id)
            if pairs is None:
                # optimize iptal eventi set edilmiş
                bot.send_message(chat_id, "🚫 Optimize iptal edildi.")
                return
            if pairs:
                old = ema_get(ticker)
                ema_set(ticker, pairs)
                # Varsayılandan farklıysa "iyileştirme" say
                if pairs["daily"] != (9,21) or pairs["weekly"] != (9,21):
                    improved += 1
                done += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"OptimizeAll hata {ticker}: {e}")
            skipped += 1

        # Her 10 hissede bir ilerleme raporu
        if (i + 1) % 10 == 0:
            pct = int((i+1) / total * 100)
            bar = "█" * (pct//10) + "░" * (10 - pct//10)
            bot.send_message(chat_id,
                f"⚙️ *Optimize: {i+1}/{total}* ({pct}%)\n"
                f"`{bar}`\n"
                f"✅ Tamamlanan: {done} | 📈 İyileştirilen: {improved} | ⏭ Atlanan: {skipped}"
            )

        time.sleep(0.5)  # DB yazma için kısa bekleme

    bot.send_message(chat_id,
        f"🎉 *Toplu Optimize Tamamlandı!*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📋 Toplam hisse: {total}\n"
        f"✅ Optimize edilen: {done}\n"
        f"📈 Varsayılandan farklı: {improved}\n"
        f"⏭ Veri yetersiz (atlandı): {skipped}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Artık /check ile taramayı başlatabilirsin."
    )

# ═══════════════════════════════════════════════
# Bot Komutları
# ═══════════════════════════════════════════════
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.reply_to(message,
        "📊 *BIST Teknik Analiz Botu*\n\n"
        "*── Tarama ──*\n"
        "/check all — Tüm listeyi tara ⭐\n"
        "/check THYAO — Tek hisse analiz\n"
        "/check 50 — Rastgele 50 hisse\n"
        "/checksingle THYAO — Detaylı debug çıktısı\n\n"
        "*── Liste Yönetimi ──*\n"
        "/addall — BIST hisselerini ekle (460 hisse)\n"
        "/refreshlist — Yahoo'dan güncel tam listeyi çek ve ekle ⭐\n"
        "/add THYAO — Tek hisse ekle\n"
        "/remove THYAO — Tek hisse çıkar\n"
        "/watchlist — İzleme listeni gör\n\n"
        "*── Optimize ──*\n"
        "/optimize THYAO — Tek hisse EMA optimize\n"
        "/optimizeall — Tüm listeyi optimize et (haftalık çalıştır)\n\n"
        "*── İptal ──*\n"
        "/iptal check — Taramayı durdur\n"
        "/iptal optimize — Tek optimize durdur\n"
        "/iptal optimizeall — Toplu optimize durdur\n\n"
        "*── Yedek / Geri Yükleme ──*\n"
        "/backup — Tüm veriyi JSON olarak yedekle 💾\n"
        "/loadbackup — JSON yedekten geri yükle 📂\n"
        "/status — Bot sağlık durumu"
    )

@bot.message_handler(commands=['iptal'])
def iptal(message):
    chat_id = str(message.chat.id)
    parts   = message.text.strip().split()
    op      = parts[1].lower() if len(parts) > 1 else "check"
    get_cancel_flag(chat_id, op).set()
    bot.reply_to(message, f"🚫 '{op}' işlemi iptal sinyali gönderildi.")

@bot.message_handler(commands=['optimizeall'])
def optimizeall(message):
    chat_id = str(message.chat.id)
    lst     = wl_get(chat_id)
    if not lst:
        bot.reply_to(message, "📭 Liste boş! Önce /addall yaz."); return
    bot.reply_to(message,
        f"⚙️ *Toplu optimize başlatılıyor...*\n"
        f"📋 {len(lst)} hisse optimize edilecek\n"
        f"💡 Bu işlemi haftada bir kez yapman yeterli.\n"
        f"🚫 Durdurmak için: /iptal optimizeall"
    )
    threading.Thread(target=_run_optimizeall, args=(chat_id,), daemon=True).start()

@bot.message_handler(commands=['addall'])
def add_all(message):
    chat_id = str(message.chat.id)
    bot.reply_to(message,
        "📋 BIST hisseleri ekleniyor...\n"
        "💡 Daha kapsamlı liste için /refreshlist komutunu dene!")
    tickers = get_all_bist_tickers()
    wl_set(chat_id, tickers)
    bot.send_message(chat_id, f"✅ {len(tickers)} hisse eklendi.")

@bot.message_handler(commands=['refreshlist'])
def refreshlist(message):
    chat_id = str(message.chat.id)
    threading.Thread(target=_run_refreshlist, args=(chat_id,), daemon=True).start()


@bot.message_handler(commands=['backup'])
def backup(message):
    """Watchlist + EMA ayarlarını Telegram mesajı olarak gönderir."""
    chat_id = str(message.chat.id)
    try:
        wl = wl_get(chat_id)
        if not wl:
            bot.reply_to(message, "📭 Yedeklenecek veri yok. Liste boş.")
            return

        # EMA ayarlarını topla
        ema_data = {}
        for ticker in wl:
            ep = ema_get(ticker)
            default = {"daily": [9,21], "weekly": [9,21]}
            if ep != {"daily": (9,21), "weekly": (9,21)}:
                ema_data[ticker] = {
                    "daily":  list(ep["daily"]),
                    "weekly": list(ep["weekly"]),
                }

        backup_payload = {
            "version":    2,
            "date":       datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%Y-%m-%d %H:%M'),
            "chat_id":    chat_id,
            "watchlist":  wl,
            "ema_custom": ema_data,   # sadece varsayılandan farklı olanlar
        }

        payload_str = json.dumps(backup_payload, ensure_ascii=False)
        bot.send_message(chat_id,
            f"💾 *Yedek Oluşturuldu*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📋 Watchlist: {len(wl)} hisse\n"
            f"⚙️ Özel EMA: {len(ema_data)} hisse\n"
            f"🕐 Tarih: {backup_payload['date']}\n\n"
            f"⬇️ *Yedek dosyası aşağıda gönderiliyor...*"
        )

        # JSON dosyası olarak gönder
        import io
        file_obj = io.BytesIO(payload_str.encode('utf-8'))
        file_obj.name = f"bist_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        bot.send_document(
            chat_id,
            file_obj,
            caption=f"📦 BIST Bot Yedek — {len(wl)} hisse\n"
                    f"Geri yüklemek için bu dosyayı bota gönder ve /loadbackup yaz."
        )

    except Exception as e:
        bot.reply_to(message, f"❌ Yedek hatası: {e}")


@bot.message_handler(commands=['loadbackup'])
def loadbackup(message):
    """Son gönderilen JSON dosyasından watchlist + EMA ayarlarını geri yükler."""
    chat_id = str(message.chat.id)
    # Eğer doğrudan komut gönderildiyse bilgi ver
    if not message.reply_to_message:
        bot.reply_to(message,
            "📂 *Yedek Yükleme*\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "1️⃣ /backup komutuyla aldığın JSON dosyasını bota gönder\n"
            "2️⃣ Dosyayı gönderirken caption'a /loadbackup yaz\n\n"
            "Veya: önce JSON dosyasını gönder, sonra o mesajı yanıtlayarak /loadbackup yaz."
        )
        return
    _process_backup_file(message, chat_id)


@bot.message_handler(content_types=['document'])
def handle_document(message):
    """Gönderilen JSON dosyasını otomatik tanı ve yedek yükleme öner."""
    chat_id = str(message.chat.id)
    try:
        fname = message.document.file_name or ""
        if fname.startswith("bist_backup") and fname.endswith(".json"):
            bot.reply_to(message,
                "📦 Yedek dosyası algılandı!\n"
                "Yüklemek için bu mesajı yanıtla ve /loadbackup yaz.\n"
                "Veya caption'a /loadbackup yaz."
            )
            # Caption'da /loadbackup varsa direkt yükle
            if message.caption and "/loadbackup" in message.caption:
                _process_backup_file(message, chat_id, doc_message=message)
    except Exception:
        pass


def _process_backup_file(message, chat_id, doc_message=None):
    """JSON yedek dosyasını işler ve veritabanına yükler."""
    try:
        # Dosyayı bul: reply veya direkt
        target = doc_message or message.reply_to_message
        if not target or not target.document:
            bot.send_message(chat_id, "❌ Dosya bulunamadı. Lütfen JSON dosyasını mesaja ekle.")
            return

        bot.send_message(chat_id, "⏳ Yedek yükleniyor...")

        file_info = bot.get_file(target.document.file_id)
        downloaded = bot.download_file(file_info.file_path)
        payload = json.loads(downloaded.decode('utf-8'))

        version   = payload.get("version", 1)
        watchlist = payload.get("watchlist", [])
        ema_data  = payload.get("ema_custom", {})

        if not watchlist:
            bot.send_message(chat_id, "❌ Dosyada watchlist bulunamadı.")
            return

        # Watchlist yükle
        wl_set(chat_id, watchlist)

        # EMA ayarlarını yükle (sadece özelleştirilmiş olanlar)
        ema_loaded = 0
        for ticker, ep in ema_data.items():
            try:
                ema_set(ticker, {
                    "daily":  tuple(ep.get("daily",  [9,21])),
                    "weekly": tuple(ep.get("weekly", [9,21])),
                })
                ema_loaded += 1
            except Exception:
                pass

        backup_date = payload.get("date", "bilinmiyor")
        bot.send_message(chat_id,
            f"✅ *Yedek Başarıyla Yüklendi!*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📋 Watchlist: {len(watchlist)} hisse\n"
            f"⚙️ EMA ayarları: {ema_loaded} hisse\n"
            f"🕐 Yedek tarihi: {backup_date}\n\n"
            f"✅ Bot kullanıma hazır!"
        )

    except json.JSONDecodeError:
        bot.send_message(chat_id, "❌ Geçersiz JSON dosyası.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Yükleme hatası: {e}")


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

    # /check THYAO — tek hisse
    if len(parts) > 1 and parts[1].upper() not in ("ALL","TUM","TÜMÜ") and not parts[1].isdigit():
        ticker = parts[1].upper().replace(".IS","")
        reset_cancel_flag(chat_id, "check")
        bot.reply_to(message, f"🔍 *{ticker}* analiz ediliyor...")
        threading.Thread(target=scan_all_stocks, args=(chat_id, None, [ticker]), daemon=True).start()
        return

    if not wl_get(chat_id):
        bot.reply_to(message, "📭 Liste boş! Önce /addall yaz."); return

    # /check all veya /check — tümünü tara
    if len(parts) == 1 or parts[1].upper() in ("ALL","TUM","TÜMÜ"):
        total = len(wl_get(chat_id))
        bot.reply_to(message,
            f"🔍 *Tüm liste taranıyor...*\n"
            f"📋 {total} hisse\n"
            f"⏱ Tahmini süre: ~{total//20} dakika\n"
            f"🚫 Durdurmak için: /iptal check"
        )
        reset_cancel_flag(chat_id, "check")
        threading.Thread(target=scan_all_stocks, args=(chat_id, None), daemon=True).start()
        return

    # /check 50 — rastgele N hisse
    try:
        limit = int(parts[1])
        bot.reply_to(message,
            f"🔍 *{limit} rastgele hisse taranıyor...*\n"
            f"🚫 Durdurmak için: /iptal check"
        )
        reset_cancel_flag(chat_id, "check")
        threading.Thread(target=scan_all_stocks, args=(chat_id, limit), daemon=True).start()
    except ValueError:
        bot.reply_to(message, "Kullanım:\n/check all — tümünü tara\n/check 50 — rastgele 50\n/check THYAO — tek hisse")


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
        try:
            now = datetime.now(tr_tz)
            if now.hour == 18 and now.minute >= 5 and now.minute < 10 and scanned_date != now.date():
                scanned_date = now.date()
                for chat_id in wl_all_ids():
                    try:
                        scan_all_stocks(chat_id)
                    except Exception as e:
                        print(f"auto_scan hisse hata {chat_id}: {e}")
        except Exception as e:
            print(f"auto_scan döngü hata: {e}")
        time.sleep(60)

# ═══════════════════════════════════════════════
# ÇÖKÜŞ KORUMA SİSTEMİ
# ═══════════════════════════════════════════════

# Bot başlangıç zamanı ve istatistikler
_bot_start_time   = datetime.now(pytz.timezone('Europe/Istanbul'))
_error_count      = 0
_last_error       = None
_last_error_time  = None
_webhook_failures = 0

def _notify_admin(msg):
    """Tüm watchlist sahibi chat'lere hata bildirimi gönder."""
    try:
        for chat_id in wl_all_ids():
            try:
                bot.send_message(chat_id, f"⚠️ *Bot Uyarısı*\n{msg}", parse_mode='Markdown')
            except Exception:
                pass
    except Exception:
        pass

def safe_thread(target, args=(), name="thread", notify_on_crash=False):
    """
    Thread'i try/except içinde çalıştırır.
    Çökerse loglar, yeniden başlatır ve bildirim gönderir.
    """
    global _error_count, _last_error, _last_error_time
    def wrapper():
        crash_count = 0
        while True:
            try:
                target(*args)
                break  # Normal çıkış
            except Exception as e:
                crash_count      += 1
                _error_count     += 1
                _last_error       = str(e)
                _last_error_time  = datetime.now(pytz.timezone('Europe/Istanbul'))
                print(f"[CRASH] {name}: {e}")

                if notify_on_crash:
                    _notify_admin(
                        f"🔴 *Çökme Tespit Edildi!*\n"
                        f"━━━━━━━━━━━━━━━━━━━\n"
                        f"⚙️ Servis: `{name}`\n"
                        f"❌ Hata: `{str(e)[:200]}`\n"
                        f"🔢 Bu serviste çökme sayısı: {crash_count}\n"
                        f"♻️ 30 saniye sonra yeniden başlatılıyor..."
                    )

                time.sleep(30)

                # Yeniden başlatma bildirimi
                if notify_on_crash:
                    _notify_admin(
                        f"✅ *Çökmeden Kurtarıldı!*\n"
                        f"━━━━━━━━━━━━━━━━━━━\n"
                        f"⚙️ Servis: `{name}`\n"
                        f"🕐 Kurtarma zamanı: "
                        f"{datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%d.%m.%Y %H:%M:%S')}\n"
                        f"🔢 Toplam kurtarma: {crash_count}\n"
                        f"🟢 Bot çalışmaya devam ediyor."
                    )

                # Sürekli döngü olan thread'leri yeniden başlat
                if name in ("auto_scan", "keep_alive", "watchdog"):
                    continue
                break
    t = threading.Thread(target=wrapper, name=name, daemon=True)
    t.start()
    return t

def watchdog():
    """
    Her 5 dakikada bir kritik thread'lerin hayatta olup olmadığını kontrol eder.
    Ölmüşse yeniden başlatır. Webhook'un sağlıklı olup olmadığını kontrol eder.
    """
    global _webhook_failures
    critical_threads = {}

    def ensure_thread(name, target, args=()):
        t = critical_threads.get(name)
        if t is None or not t.is_alive():
            print(f"[WATCHDOG] {name} yeniden baslatiliyor...")
            new_t = threading.Thread(target=target, args=args, name=name, daemon=True)
            new_t.start()
            critical_threads[name] = new_t
            return True
        return False

    while True:
        try:
            restarted = []

            # keep_alive thread kontrolü
            if ensure_thread("keep_alive", keep_alive):
                restarted.append("keep_alive")

            # auto_scan thread kontrolü
            if ensure_thread("auto_scan", auto_scan):
                restarted.append("auto_scan")

            # Webhook sağlık kontrolü
            if RENDER_URL:
                try:
                    r = requests.get(f"{RENDER_URL}/health", timeout=10)
                    if r.status_code == 200:
                        _webhook_failures = 0
                    else:
                        _webhook_failures += 1
                except Exception:
                    _webhook_failures += 1

                # 3 ardışık başarısız health check → webhook'u yenile
                if _webhook_failures >= 3:
                    print("[WATCHDOG] Webhook yenileniyor...")
                    try:
                        set_webhook()
                        _webhook_failures = 0
                        _notify_admin("🔄 Webhook yenilendi (bağlantı sorunu tespit edildi)")
                    except Exception as e:
                        print(f"[WATCHDOG] Webhook yenileme hata: {e}")

            if restarted:
                _notify_admin(
                    f"✅ *Çökmeden Kurtarıldı!*\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"♻️ Yeniden başlatılan: {', '.join(restarted)}\n"
                    f"🕐 Kurtarma zamanı: "
                    f"{datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%d.%m.%Y %H:%M:%S')}\n"
                    f"🟢 Bot çalışmaya devam ediyor."
                )

        except Exception as e:
            print(f"[WATCHDOG] Hata: {e}")

        time.sleep(5 * 60)  # Her 5 dakikada bir kontrol


def global_exception_handler(exc_type, exc_value, exc_traceback):
    """Yakalanmamış tüm exception'ları loglar."""
    import traceback
    global _error_count, _last_error, _last_error_time
    _error_count    += 1
    _last_error      = str(exc_value)
    _last_error_time = datetime.now(pytz.timezone('Europe/Istanbul'))
    tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    print(f"[GLOBAL EXCEPTION]\n{tb_str}")


@bot.message_handler(commands=['status'])
def bot_status(message):
    """Bot'un sağlık durumunu gösterir."""
    tr_tz   = pytz.timezone('Europe/Istanbul')
    now     = datetime.now(tr_tz)
    uptime  = now - _bot_start_time
    hours   = int(uptime.total_seconds() // 3600)
    minutes = int((uptime.total_seconds() % 3600) // 60)

    # Thread durumları
    thread_names = [t.name for t in threading.enumerate()]
    ka_ok   = "keep_alive" in thread_names
    as_ok   = "auto_scan"  in thread_names
    wd_ok   = "watchdog"   in thread_names

    # DB durumu
    db_ok = False
    if DATABASE_URL:
        try:
            conn = db_connect()
            if conn:
                conn.close()
                db_ok = True
        except Exception:
            pass

    # Cache durumu
    cache_count = pc_count_today()
    wl_count    = len(wl_get(str(message.chat.id)))

    lines = [
        "🤖 *Bot Durum Raporu*",
        f"━━━━━━━━━━━━━━━━━━━",
        f"⏱ Çalışma süresi: {hours}sa {minutes}dk",
        f"🕐 Başlangıç: {_bot_start_time.strftime('%d.%m.%Y %H:%M')}",
        f"",
        f"*── Servisler ──*",
        f"{'✅' if ka_ok else '❌'} Keep-alive",
        f"{'✅' if as_ok else '❌'} Auto-scan (18:05)",
        f"{'✅' if wd_ok else '❌'} Watchdog",
        f"{'✅' if db_ok else '⚠️ Yok'} Veritabanı",
        f"",
        f"*── Veriler ──*",
        f"📋 Watchlist: {wl_count} hisse",
        f"💾 Cache (bugün): {cache_count} hisse",
        f"🔗 Webhook hata: {_webhook_failures}",
        f"",
        f"*── Hatalar ──*",
        f"🔢 Toplam hata: {_error_count}",
    ]

    if _last_error:
        t_str = _last_error_time.strftime('%H:%M:%S') if _last_error_time else "?"
        lines.append(f"🔴 Son hata ({t_str}): `{_last_error[:100]}`")
    else:
        lines.append("✅ Son hata: Yok")

    bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')


# ═══════════════════════════════════════════════
# ANA BAŞLATICI
# ═══════════════════════════════════════════════
if __name__ == "__main__":
    import sys

    # Global exception handler kur
    sys.excepthook = global_exception_handler

    print(f"BIST Bot baslatiliyor - PORT={PORT}")

    # DB başlat
    try:
        db_init()
    except Exception as e:
        print(f"DB init hata (devam ediliyor): {e}")

    # Webhook kur — başarısız olsa da devam et
    try:
        set_webhook()
    except Exception as e:
        print(f"Webhook hata (devam ediliyor): {e}")

    # Kritik servisleri safe_thread ile başlat
    safe_thread(keep_alive,  name="keep_alive",  notify_on_crash=True)
    safe_thread(auto_scan,   name="auto_scan",   notify_on_crash=True)
    safe_thread(watchdog,    name="watchdog",    notify_on_crash=True)

    # Flask'ı yeniden başlatma mekanizmasıyla çalıştır
    flask_crash_count = 0
    while True:
        try:
            app.run(host='0.0.0.0', port=PORT)
        except Exception as e:
            flask_crash_count += 1
            print(f"Flask hata, 10 saniye sonra yeniden baslatiliyor: {e}")
            _notify_admin(
                f"🔴 *Flask Sunucusu Çöktü!*\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"❌ Hata: `{str(e)[:200]}`\n"
                f"🔢 Flask çökme sayısı: {flask_crash_count}\n"
                f"♻️ 10 saniye sonra yeniden başlatılıyor..."
            )
            time.sleep(10)
            _notify_admin(
                f"✅ *Flask Sunucusu Kurtarıldı!*\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"🕐 Kurtarma zamanı: "
                f"{datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%d.%m.%Y %H:%M:%S')}\n"
                f"🟢 Bot çalışmaya devam ediyor."
            )
