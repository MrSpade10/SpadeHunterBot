import os
import time
import threading
from datetime import datetime
import telebot
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
import requests
from dotenv import load_dotenv
import pytz
from flask import Flask, request, abort

load_dotenv()
TOKEN      = (os.getenv('TELEGRAM_TOKEN') or '').strip()
TWELVE_KEY = (os.getenv('TWELVE_DATA_KEY') or '').strip()
RENDER_URL = (os.getenv('RENDER_URL') or '').strip()   # https://SERVIS.onrender.com
PORT       = int(os.getenv('PORT', 10000))

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)

watchlist    = {}
best_emas    = {}
_price_cache = {}
_cache_date  = None

TD_DELAY = 8.5

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

# ─────────────────────────────────────────────
# Flask – Webhook endpoint
# Polling yok → 409 Conflict olmaz
# ─────────────────────────────────────────────
@app.route('/')
def home():
    return "BIST Bot calisiyor.", 200

@app.route('/health')
def health():
    return "OK", 200

@app.route(f'/webhook/{TOKEN}', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_str = request.get_data(as_text=True)
        update   = telebot.types.Update.de_json(json_str)
        threading.Thread(
            target=bot.process_new_updates,
            args=([update],),
            daemon=True
        ).start()
        return 'OK', 200
    abort(403)

# ─────────────────────────────────────────────
# Webhook'u Telegram'a kaydet
# ─────────────────────────────────────────────
def set_webhook():
    if not RENDER_URL:
        print("HATA: RENDER_URL env degiskeni eksik! Webhook kurulamadi.")
        return
    webhook_url = f"{RENDER_URL}/webhook/{TOKEN}"
    # Önce eskiyi sil
    bot.remove_webhook()
    time.sleep(1)
    # Yenisini kaydet
    result = bot.set_webhook(url=webhook_url)
    print(f"Webhook kuruldu: {webhook_url} → {result}")

# ─────────────────────────────────────────────
# Keep-alive
# ─────────────────────────────────────────────
def keep_alive():
    if not RENDER_URL:
        return
    while True:
        try:
            requests.get(f"{RENDER_URL}/health", timeout=10)
        except Exception:
            pass
        time.sleep(14 * 60)

# ─────────────────────────────────────────────
# Hesaplamalar
# ─────────────────────────────────────────────
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

# ─────────────────────────────────────────────
# TwelveData veri çekme
# ─────────────────────────────────────────────
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

def fetch_from_twelvedata(ticker, outputsize=365):
    if not TWELVE_KEY:
        return pd.DataFrame()
    try:
        url  = (
            f"https://api.twelvedata.com/time_series"
            f"?symbol={ticker}&exchange=XIST&interval=1day"
            f"&apikey={TWELVE_KEY}&outputsize={outputsize}"
        )
        resp = requests.get(url, timeout=15).json()
        if resp.get('status') == 'error' or 'values' not in resp:
            return pd.DataFrame()
        df = pd.DataFrame(resp['values'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.set_index('datetime').sort_index()
        df = df.rename(columns={'open':'Open','high':'High','low':'Low','close':'Close','volume':'Volume'})
        for col in ['Open','High','Low','Close','Volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    except Exception:
        return pd.DataFrame()

def bulk_fetch(tickers, chat_id):
    global _cache_date
    to_fetch = [t for t in tickers if t not in _price_cache]
    if not to_fetch:
        return
    total   = len(to_fetch)
    fetched = 0
    for i, ticker in enumerate(to_fetch):
        df = fetch_from_twelvedata(ticker)
        if not df.empty:
            _price_cache[ticker] = df
            fetched += 1
        if (i + 1) % 20 == 0 or (i + 1) == total:
            try:
                bot.send_message(chat_id,
                    f"Indiriliyor: {i+1}/{total} ({fetched} basarili)")
            except Exception:
                pass
        time.sleep(TD_DELAY)
    _cache_date = datetime.now().date()

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
    df = fetch_from_twelvedata(ticker, outputsize=500)
    if df.empty or len(df) < 100:
        return (9, 21)
    pairs = [(3,5),(5,8),(8,13),(9,21),(12,26),(20,50)]
    best_profit, best_pair = -999.0, (9, 21)
    for short, long in pairs:
        tmp          = df.copy()
        tmp['ema_s'] = calc_ema(tmp['Close'], short)
        tmp['ema_l'] = calc_ema(tmp['Close'], long)
        tmp          = tmp.dropna(subset=['ema_s','ema_l'])
        profit = 0.0; in_pos = False; entry = 0.0
        for i in range(1, len(tmp)):
            ep = tmp['ema_s'].iloc[i-1]; el = tmp['ema_l'].iloc[i-1]
            ec = tmp['ema_s'].iloc[i];   lc = tmp['ema_l'].iloc[i]
            if not in_pos and ec > lc and ep <= el:
                in_pos = True; entry = tmp['Close'].iloc[i]
            elif in_pos and ec < lc and ep >= el:
                profit += (tmp['Close'].iloc[i] - entry) / entry * 100
                in_pos = False
        if in_pos:
            profit += (tmp['Close'].iloc[-1] - entry) / entry * 100
        if profit > best_profit:
            best_profit, best_pair = profit, (short, long)
    return best_pair

def send_long_message(chat_id, text):
    if len(text) <= 4000:
        bot.send_message(chat_id, text, parse_mode='Markdown')
        return
    parts = []; current = ""
    for block in text.split('\n\n'):
        if len(current) + len(block) + 2 > 4000:
            parts.append(current.strip()); current = block
        else:
            current += block + '\n\n'
    if current:
        parts.append(current.strip())
    for part in parts:
        bot.send_message(chat_id, part, parse_mode='Markdown')
        time.sleep(0.5)

def scan_all_stocks(chat_id):
    chat_id = str(chat_id)
    if not watchlist.get(chat_id):
        return
    tickers = watchlist[chat_id]
    total   = len(tickers)
    bot.send_message(chat_id,
        f"{total} hisse taranacak.\n"
        f"Rate limit: 8 istek/dk → ~{total * TD_DELAY // 60:.0f} dk (cache yoksa).\n"
        f"Cache dolunca ayni gun anlık biter."
    )
    bulk_fetch(tickers, chat_id)
    messages = []; no_data = 0
    for ticker in tickers:
        try:
            df = _price_cache.get(ticker, pd.DataFrame())
            if df.empty or len(df) < 20:
                no_data += 1; continue
            df = df.copy()
            df['RSI']   = calc_rsi(df['Close'], 14)
            ema_pair    = best_emas.get(ticker, (9, 21))
            df['EMA_s'] = calc_ema(df['Close'], ema_pair[0])
            df['EMA_l'] = calc_ema(df['Close'], ema_pair[1])
            df          = df.dropna(subset=['RSI','EMA_s','EMA_l'])
            if len(df) < 2: continue
            cross_up   = (df['EMA_s'].iloc[-2] <= df['EMA_l'].iloc[-2]) and (df['EMA_s'].iloc[-1] > df['EMA_l'].iloc[-1])
            cross_down = (df['EMA_s'].iloc[-2] >= df['EMA_l'].iloc[-2]) and (df['EMA_s'].iloc[-1] < df['EMA_l'].iloc[-1])
            signal     = "AL - EMA CROSS UP" if cross_up else ("SAT - EMA CROSS DOWN" if cross_down else "")
            div_type, div_msg = detect_divergence(df)
            if signal or div_type:
                vol = df['Close'].pct_change().std() * 100
                msg = (
                    f"*{ticker}* ({'Yuksek' if vol > 2 else 'Dusuk'} Vol)\n"
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
        bot.send_message(chat_id,
            f"Tarama bitti. {total} hisse, {no_data} veri yok. Bugun sinyal yok.")

# ─────────────────────────────────────────────
# Komutlar
# ─────────────────────────────────────────────
@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message,
        "*BIST Bot* - RSI Diverjans + EMA Kesisim\n\n"
        "/addall - Tum BIST hisselerini ekle\n"
        "/add HEKTS - Tek hisse ekle\n"
        "/remove HEKTS - Listeden cikar\n"
        "/check - Manuel tarama\n"
        "/clearcache - Onbellegi temizle\n"
        "/optimize HEKTS - En iyi EMA bul\n"
        "/watchlist - Listeyi goster\n"
        "/debug - Baglanti durumu",
        parse_mode='Markdown'
    )

@bot.message_handler(commands=['debug'])
def debug(message):
    wh = bot.get_webhook_info()
    lines = [
        f"TOKEN: {'VAR' if TOKEN else 'YOK'}",
        f"TWELVE_KEY: {'VAR (' + TWELVE_KEY[:4] + '...)' if TWELVE_KEY else 'YOK - bot calisemaz!'}",
        f"RENDER_URL: {RENDER_URL or 'YOK - webhook kurulamaz!'}",
        f"Webhook URL: {wh.url or 'KURULU DEGIL'}",
        f"Watchlist: {len(watchlist.get(str(message.chat.id), []))} hisse",
        f"Cache: {len(_price_cache)} hisse ({_cache_date or 'bos'})",
    ]
    if TWELVE_KEY:
        try:
            r = requests.get(
                f"https://api.twelvedata.com/stocks?exchange=XIST&apikey={TWELVE_KEY}",
                timeout=10
            ).json()
            lines.append(f"TwelveData: {'OK - ' + str(len(r.get('data',[]))) + ' hisse' if r.get('status') != 'error' else 'HATA - ' + r.get('message','?')}")
        except Exception as e:
            lines.append(f"TwelveData: BAGLANTI HATASI - {e}")
        try:
            df = fetch_from_twelvedata("THYAO", outputsize=5)
            lines.append(f"THYAO veri testi: {'OK - ' + str(len(df)) + ' satir' if not df.empty else 'BASARISIZ'}")
        except Exception as e:
            lines.append(f"THYAO testi: HATA - {e}")
    bot.reply_to(message, "\n".join(lines))

@bot.message_handler(commands=['add'])
def add_stock(message):
    try:
        ticker  = message.text.split()[1].upper().replace('.IS','')
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
        ticker  = message.text.split()[1].upper().replace('.IS','')
        chat_id = str(message.chat.id)
        if ticker in watchlist.get(chat_id,[]):
            watchlist[chat_id].remove(ticker)
            bot.reply_to(message, f"{ticker} cikarildi.")
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
    added = sum(1 for t in tickers if t not in watchlist[chat_id] and not watchlist[chat_id].append(t))
    bot.reply_to(message,
        f"{added} hisse eklendi! Toplam: {len(watchlist[chat_id])}\n"
        f"Kaynak: {'TwelveData' if TWELVE_KEY else 'yedek liste'}")

@bot.message_handler(commands=['clearcache'])
def clear_cache(message):
    _price_cache.clear()
    bot.reply_to(message, "Cache temizlendi.")

@bot.message_handler(commands=['optimize'])
def optimize(message):
    try:
        ticker = message.text.split()[1].upper().replace('.IS','')
        bot.reply_to(message, f"{ticker} icin veri cekiliyor...")
        pair = find_best_ema_pair(ticker)
        best_emas[ticker] = pair
        bot.reply_to(message, f"{ticker} Best EMA: {pair[0]}-{pair[1]}")
    except Exception:
        bot.reply_to(message, "Kullanim: /optimize HEKTS")

@bot.message_handler(commands=['check'])
def manual_check(message):
    chat_id = str(message.chat.id)
    if not watchlist.get(chat_id):
        bot.reply_to(message, "Liste bos! Once /addall yazin.")
        return
    threading.Thread(target=scan_all_stocks, args=(chat_id,), daemon=True).start()

@bot.message_handler(commands=['watchlist'])
def show_list(message):
    chat_id = str(message.chat.id)
    lst = watchlist.get(chat_id, [])
    if lst:
        send_long_message(chat_id, f"*Listen* ({len(lst)} hisse):\n" + "\n".join(lst))
    else:
        bot.reply_to(message, "Liste bos - /addall yazin")

def auto_scan():
    tr_tz = pytz.timezone('Europe/Istanbul')
    scanned_date = None
    while True:
        now = datetime.now(tr_tz)
        if now.hour == 18 and now.minute < 5 and scanned_date != now.date():
            scanned_date = now.date()
            _price_cache.clear()
            for chat_id in list(watchlist.keys()):
                scan_all_stocks(chat_id)
        time.sleep(60)

if __name__ == "__main__":
    print(f"BIST Bot baslatiliyor - PORT={PORT}")
    set_webhook()
    threading.Thread(target=keep_alive, daemon=True).start()
    threading.Thread(target=auto_scan,  daemon=True).start()
    app.run(host='0.0.0.0', port=PORT)
