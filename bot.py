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
RENDER_URL = (os.getenv('RENDER_URL') or '').strip()
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
# Flask – Webhook
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
    result = bot.set_webhook(url=f"{RENDER_URL}/webhook/{TOKEN}")
    print(f"Webhook: {result}")

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
# TwelveData – 3 farklı URL formatını dene
# TwelveData bazen exchange= kabul etmiyor,
# symbol=TICKER:XIST formatı daha güvenilir
# ─────────────────────────────────────────────
def fetch_from_twelvedata(ticker, outputsize=365):
    if not TWELVE_KEY:
        return pd.DataFrame()

    # Denenecek URL formatları (sırayla)
    urls = [
        # Format 1: symbol=TICKER:XIST  ← en güvenilir
        f"https://api.twelvedata.com/time_series?symbol={ticker}%3AXIST&interval=1day&apikey={TWELVE_KEY}&outputsize={outputsize}",
        # Format 2: symbol=TICKER&exchange=XIST
        f"https://api.twelvedata.com/time_series?symbol={ticker}&exchange=XIST&interval=1day&apikey={TWELVE_KEY}&outputsize={outputsize}",
        # Format 3: symbol=TICKER.IS  (bazı sağlayıcılar bu formatı tanır)
        f"https://api.twelvedata.com/time_series?symbol={ticker}.IS&interval=1day&apikey={TWELVE_KEY}&outputsize={outputsize}",
    ]

    for url in urls:
        try:
            resp = requests.get(url, timeout=15).json()
            if resp.get('status') == 'error' or 'values' not in resp:
                continue
            df = pd.DataFrame(resp['values'])
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.set_index('datetime').sort_index()
            df = df.rename(columns={
                'open':'Open','high':'High','low':'Low',
                'close':'Close','volume':'Volume'
            })
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

def get_all_bist_tickers():
    if TWELVE_KEY:
        try:
            resp = requests.get(
                f"https://api.twelvedata.com/stocks?exchange=XIST&apikey={TWELVE_KEY}",
                timeout=15
            ).json()
            if resp.get('status') != 'error':
                data = resp.get('data', [])
                t = [item['symbol'] for item in data if item.get('exchange') == 'XIST']
                if t:
                    return t[:600]
        except Exception:
            pass
    return BIST_FALLBACK

def bulk_fetch(tickers, chat_id):
    global _cache_date
    to_fetch = [t for t in tickers if t not in _price_cache]
    if not to_fetch:
        bot.send_message(chat_id, "Cache hazir, analiz basliyor...")
        return
    total = len(to_fetch); fetched = 0
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
    for short, long_ in pairs:
        tmp = df.copy()
        tmp['es'] = calc_ema(tmp['Close'], short)
        tmp['el'] = calc_ema(tmp['Close'], long_)
        tmp = tmp.dropna(subset=['es','el'])
        profit = 0.0; in_pos = False; entry = 0.0
        for i in range(1, len(tmp)):
            ep = tmp['es'].iloc[i-1]; lp = tmp['el'].iloc[i-1]
            ec = tmp['es'].iloc[i];   lc = tmp['el'].iloc[i]
            if not in_pos and ec > lc and ep <= lp:
                in_pos = True; entry = tmp['Close'].iloc[i]
            elif in_pos and ec < lc and ep >= lp:
                profit += (tmp['Close'].iloc[i] - entry) / entry * 100
                in_pos = False
        if in_pos:
            profit += (tmp['Close'].iloc[-1] - entry) / entry * 100
        if profit > best_profit:
            best_profit, best_pair = profit, (short, long_)
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
        bot.send_message(chat_id, "Liste bos! /addall yazin.")
        return
    tickers = watchlist[chat_id]
    total   = len(tickers)
    cached  = sum(1 for t in tickers if t in _price_cache)
    to_dl   = total - cached
    bot.send_message(chat_id,
        f"{total} hisse taranacak.\n"
        f"Cache: {cached} hisse hazir, {to_dl} indirilecek.\n"
        f"{'~' + str(int(to_dl * TD_DELAY // 60)) + ' dk bekleniyor.' if to_dl > 0 else 'Hemen basliyor!'}"
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
            ep          = best_emas.get(ticker, (9, 21))
            df['EMA_s'] = calc_ema(df['Close'], ep[0])
            df['EMA_l'] = calc_ema(df['Close'], ep[1])
            df          = df.dropna(subset=['RSI','EMA_s','EMA_l'])
            if len(df) < 2: continue
            cup  = (df['EMA_s'].iloc[-2] <= df['EMA_l'].iloc[-2]) and (df['EMA_s'].iloc[-1] > df['EMA_l'].iloc[-1])
            cdn  = (df['EMA_s'].iloc[-2] >= df['EMA_l'].iloc[-2]) and (df['EMA_s'].iloc[-1] < df['EMA_l'].iloc[-1])
            sig  = "AL - EMA CROSS UP" if cup else ("SAT - EMA CROSS DOWN" if cdn else "")
            dt, dm = detect_divergence(df)
            if sig or dt:
                vol = df['Close'].pct_change().std() * 100
                messages.append(
                    f"*{ticker}* ({'Yuksek' if vol>2 else 'Dusuk'} Vol)\n"
                    f"{sig}\nRSI: {df['RSI'].iloc[-1]:.1f}\n"
                    f"{dt or 'Normal'} {dm or ''}\n"
                    f"EMA: {ep[0]}-{ep[1]}\n"
                    f"https://tr.tradingview.com/chart/?symbol=BIST:{ticker}"
                )
        except Exception:
            continue
    if messages:
        send_long_message(chat_id, "\n\n".join(messages))
    else:
        bot.send_message(chat_id,
            f"Tarama bitti. {total} hisse ({no_data} veri yok). Sinyal yok.")

# ─────────────────────────────────────────────
# Komutlar
# ─────────────────────────────────────────────
@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message,
        "*BIST Bot*\n\n"
        "/addall - Tum BIST hisselerini ekle\n"
        "/add HEKTS - Tek hisse ekle\n"
        "/remove HEKTS - Listeden cikar\n"
        "/check - Manuel tarama\n"
        "/clearcache - Onbellegi temizle\n"
        "/optimize HEKTS - En iyi EMA bul\n"
        "/watchlist - Listeyi goster\n"
        "/testapi HEKTS - API ham cevabini goster\n"
        "/debug - Baglanti durumu",
        parse_mode='Markdown'
    )

# ─────────────────────────────────────────────
# /testapi – TwelveData ham cevabını göster
# Hangi URL formatı çalışıyor bunu anlamak için
# ─────────────────────────────────────────────
@bot.message_handler(commands=['testapi'])
def test_api(message):
    try:
        ticker = message.text.split()[1].upper().replace('.IS','')
    except IndexError:
        bot.reply_to(message, "Kullanim: /testapi HEKTS"); return

    bot.reply_to(message, f"{ticker} icin 3 format deneniyor...")

    formats = [
        ("TICKER:XIST", f"https://api.twelvedata.com/time_series?symbol={ticker}%3AXIST&interval=1day&apikey={TWELVE_KEY}&outputsize=5"),
        ("TICKER+exchange=XIST", f"https://api.twelvedata.com/time_series?symbol={ticker}&exchange=XIST&interval=1day&apikey={TWELVE_KEY}&outputsize=5"),
        ("TICKER.IS", f"https://api.twelvedata.com/time_series?symbol={ticker}.IS&interval=1day&apikey={TWELVE_KEY}&outputsize=5"),
    ]

    results = []
    for name, url in formats:
        try:
            resp = requests.get(url, timeout=15).json()
            if 'values' in resp:
                results.append(f"✅ {name}: {len(resp['values'])} satir veri geldi")
            else:
                results.append(f"❌ {name}: {resp.get('message') or resp.get('status','?')}")
        except Exception as e:
            results.append(f"❌ {name}: {e}")
        time.sleep(2)

    bot.send_message(str(message.chat.id), "\n".join(results))

@bot.message_handler(commands=['debug'])
def debug(message):
    try:
        wh = bot.get_webhook_info()
        wh_url = wh.url or 'KURULU DEGIL'
    except Exception:
        wh_url = 'ALINAMADI'
    lines = [
        f"TOKEN: {'VAR' if TOKEN else 'YOK'}",
        f"TWELVE_KEY: {'VAR (' + TWELVE_KEY[:4] + '...)' if TWELVE_KEY else 'YOK'}",
        f"RENDER_URL: {RENDER_URL or 'YOK'}",
        f"Webhook: {wh_url}",
        f"Watchlist: {len(watchlist.get(str(message.chat.id), []))} hisse",
        f"Cache: {len(_price_cache)} hisse ({_cache_date or 'bos'})",
    ]
    if TWELVE_KEY:
        try:
            r = requests.get(
                f"https://api.twelvedata.com/stocks?exchange=XIST&apikey={TWELVE_KEY}",
                timeout=10
            ).json()
            cnt = len(r.get('data', []))
            lines.append(f"TwelveData stocks: {'OK - ' + str(cnt) + ' hisse' if r.get('status') != 'error' else 'HATA - ' + r.get('message','?')}")
        except Exception as e:
            lines.append(f"TwelveData stocks: HATA - {e}")
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
        f"Kaynak: {'TwelveData' if TWELVE_KEY else 'yedek liste'}\n"
        f"Simdi /testapi THYAO yazarak API'nin calistigini dogrula.")

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
        if pair == (9, 21):
            bot.reply_to(message,
                f"{ticker}: Veri alinamadi veya yetersiz (<100 gun).\n"
                f"Varsayilan 9-21 kullanildi.\n"
                f"Kontrol icin: /testapi {ticker}")
        else:
            best_emas[ticker] = pair
            bot.reply_to(message, f"{ticker} Best EMA: {pair[0]}-{pair[1]}")
    except Exception as e:
        bot.reply_to(message, f"Hata: {e}")

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
