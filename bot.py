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
TWELVE_KEY   = (os.getenv('TWELVE_DATA_KEY') or '').strip()
RENDER_URL   = (os.getenv('RENDER_URL') or '').strip()
DATABASE_URL = (os.getenv('DATABASE_URL') or '').strip()
PORT         = int(os.getenv('PORT', 10000))

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

TD_DELAY = 8.5   # saniye – TwelveData free: 8 istek/dk

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
# store       → watchlist, best_emas (JSON)
# price_cache → hisse fiyat verisi (JSON, günlük)
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
            # Genel anahtar-değer deposu
            cur.execute("""
                CREATE TABLE IF NOT EXISTS store (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            # Fiyat cache tablosu – tarih sütunuyla
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

# ─── Fiyat cache – PostgreSQL'e kaydet/oku ───
def pc_save(ticker, df):
    """DataFrame'i PostgreSQL'e kaydet."""
    conn = db_connect()
    if not conn:
        return
    try:
        # DataFrame → JSON (tarihleri string yap)
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
    """PostgreSQL'den oku. Bugün çekilmişse döner, eskiyse None."""
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
            fetched_at = row[1]   # date objesi
            today      = datetime.now(pytz.timezone('Europe/Istanbul')).date()
            # Borsanın kapandığı 18:00'den sonra çekilmişse yarın için de geçerli
            if fetched_at < today:
                return None  # Eski veri, yeniden çek
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
    """Bugün kaç hisse verisi DB'de var?"""
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
    return tuple(val) if val else (9, 21)

def ema_set(ticker, pair):
    if DATABASE_URL:
        db_set(f"ema:{ticker}", list(pair))
    else:
        _mem_emas[ticker] = pair

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

# ═══════════════════════════════════════════════
# TwelveData – kredi sayacı farkında
# ═══════════════════════════════════════════════
_api_credits_used = 0   # Oturum boyunca kullanılan kredi sayacı

def get_all_bist_tickers():
    if TWELVE_KEY:
        try:
            resp = requests.get(
                f"https://api.twelvedata.com/stocks?exchange=XIST&apikey={TWELVE_KEY}",
                timeout=15
            ).json()
            if resp.get('status') != 'error':
                t = [i['symbol'] for i in resp.get('data',[]) if i.get('exchange')=='XIST']
                if t:
                    return t[:600]
        except Exception:
            pass
    return BIST_FALLBACK

def fetch_from_twelvedata(ticker, outputsize=365):
    """
    TwelveData'dan veri çeker.
    Kredi bitti mesajı gelirse False döner (None değil – ayırt etmek için).
    """
    global _api_credits_used
    if not TWELVE_KEY:
        return pd.DataFrame()

    urls = [
        f"https://api.twelvedata.com/time_series?symbol={ticker}%3AXIST&interval=1day&apikey={TWELVE_KEY}&outputsize={outputsize}",
        f"https://api.twelvedata.com/time_series?symbol={ticker}&exchange=XIST&interval=1day&apikey={TWELVE_KEY}&outputsize={outputsize}",
    ]
    for url in urls:
        try:
            resp = requests.get(url, timeout=15).json()
            _api_credits_used += 1
            # Kredi bitti mi?
            msg = resp.get('message','')
            if 'out of API credits' in msg or 'run out' in msg:
                return 'CREDIT_EXCEEDED'
            if resp.get('status') == 'error' or 'values' not in resp:
                continue
            df = pd.DataFrame(resp['values'])
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.set_index('datetime').sort_index()
            df = df.rename(columns={'open':'Open','high':'High','low':'Low','close':'Close','volume':'Volume'})
            for col in ['Open','High','Low','Close','Volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.dropna(subset=['Close'])
            if len(df) > 5:
                return df
        except Exception:
            continue
        time.sleep(1)
    return pd.DataFrame()

def get_data(ticker):
    # 1. DB cache
    if DATABASE_URL:
        cached = pc_load(ticker)
        if cached is not None and not cached.empty:
            return cached

    # 2. TwelveData
    result = fetch_from_twelvedata(ticker)
    # String karşılaştırmasını isinstance ile yap (== DataFrame'de hata verir)
    if isinstance(result, str) and result == 'CREDIT_EXCEEDED':
        return 'CREDIT_EXCEEDED'
    if isinstance(result, pd.DataFrame) and not result.empty:
        if DATABASE_URL:
            pc_save(ticker, result)
        return result

    return pd.DataFrame()

# ═══════════════════════════════════════════════
# Analiz
# ═══════════════════════════════════════════════
def detect_divergence(df, window=60):
    if len(df) < window:
        return None, None
    recent        = df.iloc[-window:].copy()
    recent['RSI'] = calc_rsi(recent['Close'], 14)
    recent        = recent.dropna(subset=['Close','RSI'])
    if len(recent) < 20:
        return None, None
    price = recent['Close'].values
    rsi   = recent['RSI'].values
    pp,_  = find_peaks(price,  distance=5)
    rp,_  = find_peaks(rsi,    distance=5)
    pt,_  = find_peaks(-price, distance=5)
    rt,_  = find_peaks(-rsi,   distance=5)
    if len(pp)>=2 and len(rp)>=2 and price[pp[-1]]>price[pp[-2]] and rsi[rp[-1]]<rsi[rp[-2]]:
        return "NEGATIF UYUMSUZLUK","Satis baskisi!"
    if len(pt)>=2 and len(rt)>=2 and price[pt[-1]]<price[pt[-2]] and rsi[rt[-1]]>rsi[rt[-2]]:
        return "POZITIF UYUMSUZLUK","Alim firsati!"
    return None, None

def find_best_ema_pair(ticker):
    result = get_data(ticker)
    if isinstance(result, str) and result == 'CREDIT_EXCEEDED':
        return 'CREDIT_EXCEEDED'
    # DataFrame truth value bug fix: isinstance + .empty ayrı ayrı kontrol
    if not isinstance(result, pd.DataFrame):
        return None
    if result.empty or len(result) < 100:
        return None
    df    = result
    pairs = [(3,5),(5,8),(8,13),(9,21),(12,26),(20,50)]
    best_profit, best_pair = -999.0, (9,21)
    for short, long_ in pairs:
        tmp = df.copy()
        tmp['es'] = calc_ema(tmp['Close'], short)
        tmp['el'] = calc_ema(tmp['Close'], long_)
        tmp = tmp.dropna(subset=['es','el'])
        profit=0.0; in_pos=False; entry=0.0
        for i in range(1, len(tmp)):
            ep=tmp['es'].iloc[i-1]; lp=tmp['el'].iloc[i-1]
            ec=tmp['es'].iloc[i];   lc=tmp['el'].iloc[i]
            if not in_pos and ec>lc and ep<=lp:
                in_pos=True; entry=tmp['Close'].iloc[i]
            elif in_pos and ec<lc and ep>=lp:
                profit+=(tmp['Close'].iloc[i]-entry)/entry*100; in_pos=False
        if in_pos:
            profit+=(tmp['Close'].iloc[-1]-entry)/entry*100
        if profit>best_profit:
            best_profit,best_pair=profit,(short,long_)
    return best_pair

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

def scan_all_stocks(chat_id):
    chat_id = str(chat_id)
    tickers = wl_get(chat_id)
    if not tickers:
        bot.send_message(chat_id, "Liste bos! /addall yazin."); return

    total    = len(tickers)
    cached   = pc_count_today() if DATABASE_URL else 0
    to_dl    = max(0, total - cached)
    est_min  = int(to_dl * TD_DELAY // 60)

    bot.send_message(chat_id,
        f"{total} hisse taranacak.\n"
        f"DB cache: {cached} hisse hazir, {to_dl} API'den cekilecek.\n"
        f"Tahmini bekleme: {'~'+str(est_min)+' dk' if to_dl>0 else 'hemen basliyor'}.\n"
        f"API kredi kullanimi: her hisse = 1 kredi (gun limiti: 800)."
    )

    messages=[]; no_data=0; credit_stop=False

    for i, ticker in enumerate(tickers):
        if credit_stop:
            no_data += 1
            continue
        try:
            result = get_data(ticker)
            if isinstance(result, str) and result == 'CREDIT_EXCEEDED':
                credit_stop = True
                bot.send_message(chat_id,
                    f"GUNLUK API KREDI DOLDU ({i} hisse taranabildi).\n"
                    f"Yarin devam edilecek. DB'deki {cached} hissenin verisi korunuyor.")
                no_data += 1
                continue
            if not isinstance(result, pd.DataFrame):
                no_data += 1; continue
            if result.empty or len(result) < 20:
                no_data += 1; continue

            df = result.copy()
            df['RSI']   = calc_rsi(df['Close'],14)
            ep          = ema_get(ticker)
            df['EMA_s'] = calc_ema(df['Close'],ep[0])
            df['EMA_l'] = calc_ema(df['Close'],ep[1])
            df          = df.dropna(subset=['RSI','EMA_s','EMA_l'])
            if len(df)<2: continue

            cup = (df['EMA_s'].iloc[-2]<=df['EMA_l'].iloc[-2]) and (df['EMA_s'].iloc[-1]>df['EMA_l'].iloc[-1])
            cdn = (df['EMA_s'].iloc[-2]>=df['EMA_l'].iloc[-2]) and (df['EMA_s'].iloc[-1]<df['EMA_l'].iloc[-1])
            sig = "AL - EMA CROSS UP" if cup else ("SAT - EMA CROSS DOWN" if cdn else "")
            dt,dm = detect_divergence(df)
            if sig or dt:
                vol = df['Close'].pct_change().std()*100
                messages.append(
                    f"*{ticker}* ({'Yuksek' if vol>2 else 'Dusuk'} Vol)\n"
                    f"{sig}\nRSI: {df['RSI'].iloc[-1]:.1f}\n"
                    f"{dt or 'Normal'} {dm or ''}\n"
                    f"EMA: {ep[0]}-{ep[1]}\n"
                    f"https://tr.tradingview.com/chart/?symbol=BIST:{ticker}"
                )
            time.sleep(TD_DELAY)
        except Exception:
            continue

    if messages:
        send_long_message(chat_id, "\n\n".join(messages))
    elif not credit_stop:
        bot.send_message(chat_id, f"Tarama bitti. {total} hisse ({no_data} veri yok). Sinyal yok.")

# ═══════════════════════════════════════════════
# Komutlar
# ═══════════════════════════════════════════════
@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message,
        f"*BIST Bot*\n"
        f"Depolama: {'PostgreSQL (kalici)' if DATABASE_URL else 'In-memory (restart ta sifirlanir)'}\n\n"
        "/addall - Tum BIST hisselerini ekle\n"
        "/add HEKTS - Tek hisse ekle\n"
        "/remove HEKTS - Listeden cikar\n"
        "/check - Manuel tarama\n"
        "/optimize HEKTS - En iyi EMA bul\n"
        "/watchlist - Listeyi goster\n"
        "/credits - API kredi durumu\n"
        "/debug - Sistem durumu",
        parse_mode='Markdown'
    )

@bot.message_handler(commands=['credits'])
def credits_status(message):
    """TwelveData kredi durumu + DB cache özeti."""
    if not TWELVE_KEY:
        bot.reply_to(message, "TWELVE_KEY tanimli degil."); return
    try:
        # Free plan /api_usage cevap vermiyor,
        # bunun yerine tek satır veri çekip header'daki
        # X-RateLimit-* bilgilerini okuyoruz
        resp = requests.get(
            f"https://api.twelvedata.com/time_series?symbol=THYAO&exchange=XIST&interval=1day&outputsize=1&apikey={TWELVE_KEY}",
            timeout=10
        )
        data         = resp.json()
        # Bazı planlarda header'da gelir
        used         = resp.headers.get('X-RateLimit-Used-Day',      None)
        remaining    = resp.headers.get('X-RateLimit-Remaining-Day', None)
        limit        = resp.headers.get('X-RateLimit-Limit-Day',     None)

        # Header yoksa JSON'dan anlamaya çalış
        if not used:
            msg = data.get('message','')
            if 'out of API credits' in msg or 'run out' in msg:
                status_line = "KREDI DOLDU - yarin sifirlanir"
            elif 'values' in data:
                status_line = "API calisiyor (kredi mevcut)"
            else:
                status_line = f"Durum bilinmiyor: {data.get('message','?')}"
        else:
            status_line = f"Kullanilan: {used} / {limit}  |  Kalan: {remaining}"

        today_cache = pc_count_today() if DATABASE_URL else 0
        bot.reply_to(message,
            f"TwelveData durumu:\n"
            f"{status_line}\n\n"
            f"DB cache bugun: {today_cache} hisse\n"
            f"Bu oturumda yapilan istek: {_api_credits_used}\n\n"
            f"NOT: Free plan = 800 kredi/gun.\n"
            f"DB cache dolu oldugunda sifir kredi harcar."
        )
    except Exception as e:
        bot.reply_to(message, f"Kredi bilgisi alinamadi: {e}")

@bot.message_handler(commands=['debug'])
def debug(message):
    try:
        wh_url = bot.get_webhook_info().url or 'KURULU DEGIL'
    except Exception:
        wh_url = 'ALINAMADI'
    chat_id = str(message.chat.id)
    db_ok   = bool(db_connect())
    today_cache = pc_count_today() if DATABASE_URL else 0
    lines = [
        f"TOKEN: {'VAR' if TOKEN else 'YOK'}",
        f"TWELVE_KEY: {'VAR (' + TWELVE_KEY[:4] + '...)' if TWELVE_KEY else 'YOK'}",
        f"RENDER_URL: {RENDER_URL or 'YOK'}",
        f"DATABASE_URL: {'VAR' if DATABASE_URL else 'YOK'}",
        f"DB baglanti: {'OK' if db_ok else 'HATA'}",
        f"Webhook: {wh_url}",
        f"Watchlist: {len(wl_get(chat_id))} hisse",
        f"DB price cache bugun: {today_cache} hisse",
        f"Oturum API kullanimi: {_api_credits_used} istek",
    ]
    bot.reply_to(message, "\n".join(lines))

@bot.message_handler(commands=['rawapi'])
def raw_api(message):
    """Ham API cevabını göster – hangi format çalışıyor?"""
    try:
        ticker = message.text.split()[1].upper().replace('.IS','')
    except IndexError:
        bot.reply_to(message, "Kullanim: /rawapi HEKTS"); return

    if not TWELVE_KEY:
        bot.reply_to(message, "TWELVE_KEY tanimli degil."); return

    bot.reply_to(message, f"{ticker} icin ham API cevabi aliniyor...")

    urls = [
        ("exchange=XIST", f"https://api.twelvedata.com/time_series?symbol={ticker}&exchange=XIST&interval=1day&apikey={TWELVE_KEY}&outputsize=10"),
        ("symbol:XIST",   f"https://api.twelvedata.com/time_series?symbol={ticker}%3AXIST&interval=1day&apikey={TWELVE_KEY}&outputsize=10"),
    ]
    for name, url in urls:
        try:
            resp = requests.get(url, timeout=15).json()
            if 'values' in resp:
                rows = len(resp['values'])
                sample = resp['values'][0] if rows > 0 else {}
                bot.send_message(str(message.chat.id),
                    f"OK [{name}]: {rows} satir\n"
                    f"Ilk satir: {sample}\n"
                    f"Meta: {resp.get('meta',{})}"
                )
            else:
                bot.send_message(str(message.chat.id),
                    f"FAIL [{name}]: {resp.get('message') or resp.get('status','?')}"
                )
        except Exception as e:
            bot.send_message(str(message.chat.id), f"HATA [{name}]: {e}")
        time.sleep(2)

@bot.message_handler(commands=['add'])
def add_stock(message):
    try:
        ticker  = message.text.split()[1].upper().replace('.IS','')
        chat_id = str(message.chat.id)
        tickers = wl_get(chat_id)
        if ticker not in tickers:
            tickers.append(ticker)
            wl_set(chat_id, tickers)
            bot.reply_to(message, f"{ticker} eklendi! (Toplam: {len(tickers)})")
        else:
            bot.reply_to(message, f"{ticker} zaten listende.")
    except Exception:
        bot.reply_to(message, "Kullanim: /add HEKTS")

@bot.message_handler(commands=['remove'])
def remove_stock(message):
    try:
        ticker  = message.text.split()[1].upper().replace('.IS','')
        chat_id = str(message.chat.id)
        tickers = wl_get(chat_id)
        if ticker in tickers:
            tickers.remove(ticker)
            wl_set(chat_id, tickers)
            bot.reply_to(message, f"{ticker} cikarildi. (Kalan: {len(tickers)})")
        else:
            bot.reply_to(message, f"{ticker} listende yok.")
    except Exception:
        bot.reply_to(message, "Kullanim: /remove HEKTS")

@bot.message_handler(commands=['addall'])
def add_all(message):
    chat_id = str(message.chat.id)
    bot.reply_to(message, "BIST hisseleri yukleniyor...")
    all_t   = get_all_bist_tickers()
    current = wl_get(chat_id)
    new     = [t for t in all_t if t not in current]
    current.extend(new)
    wl_set(chat_id, current)
    bot.reply_to(message,
        f"{len(new)} hisse eklendi! Toplam: {len(current)}\n"
        f"Kaynak: {'TwelveData' if TWELVE_KEY else 'yedek liste'}\n"
        f"NOT: /check ilk calistiginda ~{len(current)//8} dakika surecek.\n"
        f"Sonraki /check anlık biter (DB cache kullanir)."
    )

@bot.message_handler(commands=['optimize'])
def optimize(message):
    try:
        ticker = message.text.split()[1].upper().replace('.IS','')
        bot.reply_to(message, f"{ticker} icin veri aliniyor (once DB cache kontrol ediliyor)...")
        pair = find_best_ema_pair(ticker)
        if isinstance(pair, str) and pair == 'CREDIT_EXCEEDED':
            bot.reply_to(message, "Gunluk API kredisi doldu. Yarin tekrar dene veya /credits yaz.")
        elif pair is None:
            bot.reply_to(message, f"{ticker}: Veri alinamadi veya yetersiz (<100 gun).")
        else:
            ema_set(ticker, pair)
            bot.reply_to(message,
                f"{ticker} Best EMA: {pair[0]}-{pair[1]}\n"
                f"{'DB ye kaydedildi.' if DATABASE_URL else 'In-memory kaydedildi.'}")
    except Exception as e:
        bot.reply_to(message, f"Hata: {e}")

@bot.message_handler(commands=['check'])
def manual_check(message):
    chat_id = str(message.chat.id)
    if not wl_get(chat_id):
        bot.reply_to(message, "Liste bos! Once /addall yazin."); return
    threading.Thread(target=scan_all_stocks, args=(chat_id,), daemon=True).start()

@bot.message_handler(commands=['watchlist'])
def show_list(message):
    chat_id = str(message.chat.id)
    lst = wl_get(chat_id)
    if lst:
        send_long_message(chat_id, f"*Listen* ({len(lst)} hisse):\n" + "\n".join(lst))
    else:
        bot.reply_to(message, "Liste bos - /addall yazin")

def auto_scan():
    tr_tz = pytz.timezone('Europe/Istanbul')
    scanned_date = None
    while True:
        now = datetime.now(tr_tz)
        # 18:05'te tara – borsa kapandıktan 5 dk sonra
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
