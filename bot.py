import os
import time
import threading
from datetime import datetime
import telebot
import yfinance as yf
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
import requests
from dotenv import load_dotenv
import pytz
from flask import Flask

load_dotenv()
TOKEN      = (os.getenv('TELEGRAM_TOKEN') or '').strip()
TWELVE_KEY = (os.getenv('TWELVE_DATA_KEY') or '').strip()
RENDER_URL = (os.getenv('RENDER_URL') or '').strip()
PORT       = int(os.getenv('PORT', 10000))

bot = telebot.TeleBot(TOKEN)
app = Flask(__name__)

watchlist = {}
best_emas = {}

BIST_FALLBACK = [
    "THYAO","GARAN","ASELS","KCHOL","EREGL","AKBNK","YKBNK","SISE","TUPRS","SAHOL",
    "FROTO","TOASO","PETKM","HALKB","VAKBN","TTKOM","BIMAS","AKSEN","ENKAI","KOZAL",
    "ISCTR","ARCLK","PGSUS","TAVHL","TCELL","DOHOL","OYAKC","OTKAR","GUBRF","MGROS",
    "SOKM","ULKER","CCOLA","AGHOL","EKGYO","LOGO","NETAS","VESTL","TSKB","ALARK",
    "ALFAS","AEFES","KRDMD","GOLTS","SODA","KONYA","EGEEN","TKFEN","KERVT","HEKTS",
    "GOODY","DURDO","ADEL","ADANA","AKMGY","ALCTL","ANACM","ANHYT","ANSGR","ASTOR",
    "AYGAZ","BAGFS","BAKAB","BANVT","BARMA","BASCM","BERA","BFREN","BIOEN","BIZIM",
    "BJKAS","BLCYT","BMSTL","BNTAS","BOSSA","BRISA","BRKSN","BRYAT","BSOKE","BTCIM",
    "BUCIM","BURCE","BURVA","BVSAN","CATES","CEMTS","CIMSA","CLEBI","CRDFA","CRFSA",
    "CUSAN","CVKMD","DAGHL","DARDL","DENGE","DERHL","DERIM","DESAS","DESPC","DEVA",
    "DGATE","DITAS","DMSAS","DNISI","DOGUB","DORTS","DPENS","DYOBY","DZGYO","ECILC",
    "ECZYT","EDIP","EGGUB","EGPRO","EGSER","EKIZ","EMKEL","EMNIS","ENDL","EPLAS",
    "ERSU","ESCOM","ESEN","ETYAT","EUHOL","FADE","FENER","FONET","FORTE","GARFA",
    "GEDIK","GEDZA","GENIL","GENTS","GEREL","GESAN","GIMAT","GLBMD","GLRYH","GLYHO",
    "GMTAS","GNDUZ","GRSEL","GRTRK","GSDDE","GSDHO","GSRAY","GUBRF","GWIND","HEDEF",
    "HZNDR","IDEAS","IDGYO","IEYHO","IHEVA","IHGZT","IHLAS","IHLGM","IHYAY","INDES",
    "INFO","INGRM","INVEO","IPEKE","ISATR","ISBIR","ITTFK","IZFAS","IZMDC","JANTS",
    "KAPLM","KAREL","KARSN","KARTN","KATMR","KBORU","KENT","KERVN","KLKIM","KLMSN",
    "KONKA","KONTR","KOPOL","KORDS","KOZAA","KRDMA","KRDMB","KRONT","KRSAN","KSTUR",
    "KUYAS","LIDER","LKMNH","LUKSK","MACKO","MAKIM","MANAS","MARTI","MAVI","MEDTR",
    "MEPET","MERCN","MERKO","METRO","MIPAZ","MNDRS","MNVRL","MOBTL","MOGAN","MPARK",
    "MRDIN","MRSHL","MSGYO","MTRKS","NATEN","NBORU","NTGAZ","NTHOL","NUGYO","NUHCM",
    "OBASE","ODAS","ORGE","ORMA","OSTIM","PAMEL","PAPIL","PARSN","PASEU","PENGD",
    "PENTA","PETUN","PINSU","PKART","PLTUR","POLHO","POLTK","PRZMA","QNBFB","QNBFL",
    "RAYSG","RHEAG","RNPOL","RUBNS","SAFKR","SAMAT","SANEL","SANFM","SANKO","SARKY",
    "SAYAS","SDTTR","SEGYO","SEKFK","SEKUR","SELEC","SELGD","SELVA","SEYKM","SILVR",
    "SKBNK","SMART","SMILE","SNPAM","SONME","SUWEN","TARKM","TATEN","TATGD","TBORG",
    "TEKTU","TGSAS","TIRE","TMSN","TRCAS","TRILC","TSPOR","TUCLK","TUREX","TURSG",
    "TUYAP","ULUUN","USAK","VAKFN","VAKKO","VANGD","VBTYZ","VERUS","VKGYO","YAPRK",
    "YATAS","YBTAS","YESIL","YUNSA","ZOREN"
]

@app.route('/')
def home():
    return "BIST Bot calisiyor.", 200

@app.route('/health')
def health():
    return "OK", 200

def keep_alive():
    if not RENDER_URL:
        return
    while True:
        try:
            requests.get(f"{RENDER_URL}/health", timeout=10)
        except Exception:
            pass
        time.sleep(14 * 60)

def calc_rsi(series, length=14):
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=length - 1, min_periods=length).mean()
    avg_loss = loss.ewm(com=length - 1, min_periods=length).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def get_all_bist_tickers():
    if TWELVE_KEY:
        try:
            url  = f"https://api.twelvedata.com/stocks?exchange=XIST&apikey={TWELVE_KEY}"
            resp = requests.get(url, timeout=15).json()
            if resp.get('status') != 'error':
                data    = resp.get('data', [])
                tickers = [item['symbol'] for item in data if item.get('exchange') == 'XIST']
                if tickers:
                    return tickers[:600]
        except Exception:
            pass
    return BIST_FALLBACK

# ─────────────────────────────────────────────
# TOPLU veri çekme – yfinance rate limit sorununu çözer
# Tek tek çekmek yerine tüm listeyi bir seferde indir,
# sonra her ticker'ı DataFrame'den al
# ─────────────────────────────────────────────
_bulk_cache = {}          # { ticker: df }
_bulk_cache_date = None   # Son çekim tarihi

def bulk_download(tickers, period="1y"):
    """
    Tüm BIST listesini tek bir yfinance.download() çağrısında indir.
    Yahoo Finance tek tek sorgularda rate limit kesiyor,
    toplu sorguda kesmez.
    """
    global _bulk_cache, _bulk_cache_date
    today = datetime.now().date()
    if _bulk_cache_date == today and _bulk_cache:
        return  # Bugün zaten indirdik

    symbols = [f"{t}.IS" for t in tickers]
    try:
        raw = yf.download(
            symbols,
            period=period,
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="ticker",   # Her ticker ayrı kolon grubu
            threads=True         # Paralel indirme
        )
        # raw.columns → MultiIndex: (field, ticker)
        # Her ticker için Close serisini çıkar
        new_cache = {}
        for t in tickers:
            sym = f"{t}.IS"
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    df = raw.xs(sym, axis=1, level=1)
                else:
                    df = raw  # Tek ticker durumu
                df = df.dropna(how='all')
                if len(df) > 10:
                    new_cache[t] = df
            except Exception:
                pass
        _bulk_cache      = new_cache
        _bulk_cache_date = today
    except Exception:
        pass

def get_data_from_cache(ticker):
    """Önce cache'e bak, yoksa TwelveData'ya git."""
    if ticker in _bulk_cache:
        return _bulk_cache[ticker].copy()

    # Cache miss → TwelveData yedek
    if TWELVE_KEY:
        try:
            url = (
                f"https://api.twelvedata.com/time_series"
                f"?symbol={ticker}&exchange=XIST&interval=1day"
                f"&apikey={TWELVE_KEY}&outputsize=500"
            )
            resp = requests.get(url, timeout=10).json()
            if resp.get('status') != 'error' and 'values' in resp:
                df = pd.DataFrame(resp['values'])
                df['datetime'] = pd.to_datetime(df['datetime'])
                df = df.set_index('datetime').sort_index()
                df = df.rename(columns={
                    'open': 'Open', 'high': 'High',
                    'low': 'Low', 'close': 'Close', 'volume': 'Volume'
                }).astype(float)
                return df
        except Exception:
            pass

    return pd.DataFrame()

def get_data(ticker, period="2y", interval="1d"):
    """Optimize + manuel /add için tekil veri çekme (retry ile)."""
    for attempt in range(3):
        try:
            df = yf.download(
                f"{ticker}.IS",
                period=period,
                interval=interval,
                auto_adjust=True,
                progress=False
            )
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            if not df.empty and len(df) > 10:
                return df
        except Exception:
            pass
        time.sleep(2 * (attempt + 1))   # 2s, 4s, 6s backoff

    if TWELVE_KEY:
        try:
            url = (
                f"https://api.twelvedata.com/time_series"
                f"?symbol={ticker}&exchange=XIST&interval=1day"
                f"&apikey={TWELVE_KEY}&outputsize=500"
            )
            resp = requests.get(url, timeout=10).json()
            if resp.get('status') != 'error' and 'values' in resp:
                df = pd.DataFrame(resp['values'])
                df['datetime'] = pd.to_datetime(df['datetime'])
                df = df.set_index('datetime').sort_index()
                df = df.rename(columns={
                    'open': 'Open', 'high': 'High',
                    'low': 'Low', 'close': 'Close', 'volume': 'Volume'
                }).astype(float)
                return df
        except Exception:
            pass

    return pd.DataFrame()

def detect_divergence(df, window=60):
    if len(df) < window:
        return None, None
    recent        = df.iloc[-window:].copy()
    recent['RSI'] = calc_rsi(recent['Close'], 14)
    recent        = recent.dropna(subset=['Close', 'RSI'])
    if len(recent) < 20:
        return None, None
    price = recent['Close'].values
    rsi   = recent['RSI'].values
    p_peaks,   _ = find_peaks(price,  distance=5)
    r_peaks,   _ = find_peaks(rsi,    distance=5)
    p_troughs, _ = find_peaks(-price, distance=5)
    r_troughs, _ = find_peaks(-rsi,   distance=5)
    if (len(p_peaks) >= 2 and len(r_peaks) >= 2
            and price[p_peaks[-1]] > price[p_peaks[-2]]
            and rsi[r_peaks[-1]]   < rsi[r_peaks[-2]]):
        return "NEGATIF UYUMSUZLUK", "Satis baskisi!"
    if (len(p_troughs) >= 2 and len(r_troughs) >= 2
            and price[p_troughs[-1]] < price[p_troughs[-2]]
            and rsi[r_troughs[-1]]   > rsi[r_troughs[-2]]):
        return "POZITIF UYUMSUZLUK", "Alim firsati!"
    return None, None

def find_best_ema_pair(ticker):
    df = get_data(ticker)
    if df.empty or len(df) < 100:
        return (9, 21)
    pairs = [(3, 5), (5, 8), (8, 13), (9, 21), (12, 26), (20, 50)]
    best_profit, best_pair = -999.0, (9, 21)
    for short, long in pairs:
        tmp          = df.copy()
        tmp['ema_s'] = calc_ema(tmp['Close'], short)
        tmp['ema_l'] = calc_ema(tmp['Close'], long)
        tmp          = tmp.dropna(subset=['ema_s', 'ema_l'])
        profit = 0.0
        in_pos = False
        entry  = 0.0
        for i in range(1, len(tmp)):
            es_prev = tmp['ema_s'].iloc[i - 1]
            el_prev = tmp['ema_l'].iloc[i - 1]
            es_cur  = tmp['ema_s'].iloc[i]
            el_cur  = tmp['ema_l'].iloc[i]
            if not in_pos and es_cur > el_cur and es_prev <= el_prev:
                in_pos = True
                entry  = tmp['Close'].iloc[i]
            elif in_pos and es_cur < el_cur and es_prev >= el_prev:
                profit += (tmp['Close'].iloc[i] - entry) / entry * 100
                in_pos  = False
        if in_pos:
            profit += (tmp['Close'].iloc[-1] - entry) / entry * 100
        if profit > best_profit:
            best_profit, best_pair = profit, (short, long)
    return best_pair

def send_long_message(chat_id, text):
    if len(text) <= 4000:
        bot.send_message(chat_id, text, parse_mode='Markdown')
        return
    parts   = []
    current = ""
    for block in text.split('\n\n'):
        if len(current) + len(block) + 2 > 4000:
            parts.append(current.strip())
            current = block
        else:
            current += block + '\n\n'
    if current:
        parts.append(current.strip())
    for part in parts:
        bot.send_message(chat_id, part, parse_mode='Markdown')
        time.sleep(0.5)

def scan_all_stocks(chat_id):
    chat_id = str(chat_id)
    if chat_id not in watchlist or not watchlist[chat_id]:
        return

    tickers = watchlist[chat_id]
    total   = len(tickers)

    # ── ADIM 1: Toplu indirme (rate limit olmaz) ──
    bot.send_message(chat_id, f"Veriler indiriliyor ({total} hisse)...")
    bulk_download(tickers, period="1y")

    # ── ADIM 2: Her hisseyi analiz et ──
    messages = []
    no_data  = 0

    for ticker in tickers:
        try:
            df = get_data_from_cache(ticker)
            if df.empty or len(df) < 20:
                no_data += 1
                continue

            df['RSI']   = calc_rsi(df['Close'], 14)
            ema_pair    = best_emas.get(ticker, (9, 21))
            df['EMA_s'] = calc_ema(df['Close'], ema_pair[0])
            df['EMA_l'] = calc_ema(df['Close'], ema_pair[1])
            df          = df.dropna(subset=['RSI', 'EMA_s', 'EMA_l'])
            if len(df) < 2:
                continue

            cross_up   = (df['EMA_s'].iloc[-2] <= df['EMA_l'].iloc[-2]) and (df['EMA_s'].iloc[-1] > df['EMA_l'].iloc[-1])
            cross_down = (df['EMA_s'].iloc[-2] >= df['EMA_l'].iloc[-2]) and (df['EMA_s'].iloc[-1] < df['EMA_l'].iloc[-1])

            signal = ""
            if cross_up:
                signal = "AL - EMA CROSS UP"
            elif cross_down:
                signal = "SAT - EMA CROSS DOWN"

            div_type, div_msg = detect_divergence(df)

            if signal or div_type:
                vol      = df['Close'].pct_change().std() * 100
                karakter = "Yuksek Vol" if vol > 2 else "Dusuk Vol"
                msg = (
                    f"*{ticker}* ({karakter})\n"
                    f"{signal}\n"
                    f"RSI: {df['RSI'].iloc[-1]:.1f}\n"
                    f"{div_type or 'Normal'} {div_msg or ''}\n"
                    f"EMA: {ema_pair[0]}-{ema_pair[1]}\n"
                    f"https://tr.tradingview.com/chart/?symbol=BIST:{ticker}"
                )
                messages.append(msg)
        except Exception:
            continue

    if messages:
        send_long_message(chat_id, "\n\n".join(messages))
    else:
        bot.send_message(
            chat_id,
            f"Tarama bitti. {total} hisse, {no_data} veri yok.\nBugün sinyal yok."
        )

# ─────────────────────────────────────────────
# Telegram komutları
# ─────────────────────────────────────────────
@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(
        message,
        "*BIST Bot* - RSI Diverjans + EMA Kesisim\n\n"
        "/addall - Tum BIST hisselerini ekle\n"
        "/add HEKTS - Tek hisse ekle\n"
        "/remove HEKTS - Listeden cikar\n"
        "/check - Manuel tarama\n"
        "/optimize HEKTS - En iyi EMA bul\n"
        "/watchlist - Listeyi goster\n"
        "/debug - Baglanti durumu",
        parse_mode='Markdown'
    )

@bot.message_handler(commands=['debug'])
def debug(message):
    lines = []
    lines.append(f"TOKEN: {'VAR' if TOKEN else 'YOK'}")
    lines.append(f"TWELVE_DATA_KEY: {'VAR (' + TWELVE_KEY[:4] + '...)' if TWELVE_KEY else 'YOK'}")
    lines.append(f"RENDER_URL: {RENDER_URL or 'YOK'}")
    lines.append(f"PORT: {PORT}")
    chat_id = str(message.chat.id)
    lines.append(f"Watchlist: {len(watchlist.get(chat_id, []))} hisse")
    lines.append(f"Bulk cache: {len(_bulk_cache)} hisse ({_bulk_cache_date or 'bos'})")
    if TWELVE_KEY:
        try:
            r = requests.get(
                f"https://api.twelvedata.com/stocks?exchange=XIST&apikey={TWELVE_KEY}",
                timeout=10
            ).json()
            if r.get('status') == 'error':
                lines.append(f"TwelveData: HATA - {r.get('message','?')}")
            else:
                lines.append(f"TwelveData: OK - {len(r.get('data',[]))} hisse")
        except Exception as e:
            lines.append(f"TwelveData: HATA - {e}")
    try:
        test = yf.download("THYAO.IS", period="5d", progress=False)
        if isinstance(test.columns, pd.MultiIndex):
            test.columns = test.columns.get_level_values(0)
        lines.append(f"yfinance: {'OK - ' + str(len(test)) + ' gun' if not test.empty else 'VERI YOK'}")
    except Exception as e:
        lines.append(f"yfinance: HATA - {e}")
    bot.reply_to(message, "\n".join(lines))

@bot.message_handler(commands=['add'])
def add_stock(message):
    try:
        ticker  = message.text.split()[1].upper().replace('.IS', '')
        chat_id = str(message.chat.id)
        watchlist.setdefault(chat_id, [])
        if ticker not in watchlist[chat_id]:
            watchlist[chat_id].append(ticker)
            bot.reply_to(message, f"{ticker} eklendi!")
        else:
            bot.reply_to(message, f"{ticker} zaten listende.")
    except Exception:
        bot.reply_to(message, "Kullanim: /add HEKTS")

@bot.message_handler(commands=['remove'])
def remove_stock(message):
    try:
        ticker  = message.text.split()[1].upper().replace('.IS', '')
        chat_id = str(message.chat.id)
        if ticker in watchlist.get(chat_id, []):
            watchlist[chat_id].remove(ticker)
            bot.reply_to(message, f"{ticker} listeden cikarildi.")
        else:
            bot.reply_to(message, f"{ticker} listende yok.")
    except Exception:
        bot.reply_to(message, "Kullanim: /remove HEKTS")

@bot.message_handler(commands=['addall'])
def add_all(message):
    chat_id = str(message.chat.id)
    bot.reply_to(message, "BIST hisseleri yukleniyor...")
    tickers = get_all_bist_tickers()
    watchlist.setdefault(chat_id, [])
    added = sum(
        1 for t in tickers
        if t not in watchlist[chat_id] and not watchlist[chat_id].append(t)
    )
    kaynak = "TwelveData" if TWELVE_KEY else "yedek liste"
    bot.reply_to(message, f"{added} hisse eklendi! Toplam: {len(watchlist[chat_id])}\nKaynak: {kaynak}")

@bot.message_handler(commands=['optimize'])
def optimize(message):
    try:
        ticker = message.text.split()[1].upper().replace('.IS', '')
        bot.reply_to(message, f"{ticker} optimize ediliyor...")
        pair = find_best_ema_pair(ticker)
        best_emas[ticker] = pair
        bot.reply_to(message, f"{ticker} Best EMA: {pair[0]}-{pair[1]}")
    except Exception:
        bot.reply_to(message, "Kullanim: /optimize HEKTS")

@bot.message_handler(commands=['check'])
def manual_check(message):
    chat_id = str(message.chat.id)
    lst     = watchlist.get(chat_id, [])
    if not lst:
        bot.reply_to(message, "Liste bos! Once /addall yazin.")
        return
    bot.reply_to(message, f"{len(lst)} hisse icin tarama basliyor...")
    threading.Thread(target=scan_all_stocks, args=(chat_id,), daemon=True).start()

@bot.message_handler(commands=['watchlist'])
def show_list(message):
    chat_id = str(message.chat.id)
    lst     = watchlist.get(chat_id, [])
    if lst:
        send_long_message(chat_id, f"*Listen* ({len(lst)} hisse):\n" + "\n".join(lst))
    else:
        bot.reply_to(message, "Liste bos - /addall yazin")

def auto_scan():
    tr_tz        = pytz.timezone('Europe/Istanbul')
    scanned_date = None
    while True:
        now   = datetime.now(tr_tz)
        today = now.date()
        if now.hour == 18 and now.minute < 5 and scanned_date != today:
            scanned_date = today
            for chat_id in list(watchlist.keys()):
                scan_all_stocks(chat_id)
        time.sleep(60)

if __name__ == "__main__":
    print(f"BIST Bot baslatiliyor - PORT={PORT}")
    threading.Thread(target=keep_alive, daemon=True).start()
    threading.Thread(target=auto_scan,  daemon=True).start()
    threading.Thread(
        target=lambda: bot.infinity_polling(none_stop=True),
        daemon=True
    ).start()
    app.run(host='0.0.0.0', port=PORT)
