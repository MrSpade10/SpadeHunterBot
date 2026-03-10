import os
import time
import threading
from datetime import datetime
import telebot
import yfinance as yf
import pandas as pd
import pandas_ta as ta
from scipy.signal import find_peaks
import requests
from dotenv import load_dotenv
import pytz
from flask import Flask

load_dotenv()
TOKEN      = os.getenv('TELEGRAM_TOKEN')
TWELVE_KEY = os.getenv('TWELVE_DATA_KEY')
# Render Dashboard → Environment → RENDER_URL = https://SERVIS-ADIN.onrender.com
RENDER_URL = os.getenv('RENDER_URL', '')
PORT       = int(os.getenv('PORT', 10000))  # Render otomatik atar

bot = telebot.TeleBot(TOKEN)
app = Flask(__name__)

# Memory (Render free tier restart olunca sıfırlanır)
watchlist = {}
best_emas = {}

# ─────────────────────────────────────────────
# Flask – Render "port dinleniyor mu?" kontrolü
# Bu iki endpoint OLMADAN Render servisi hemen kapatır!
# ─────────────────────────────────────────────
@app.route('/')
def home():
    return "🤖 BIST Bot çalışıyor.", 200

@app.route('/health')
def health():
    return "OK", 200

# ─────────────────────────────────────────────
# Keep-alive: Free tier 15 dakika isteksiz kalırsa uyur.
# Kendi URL'ine her 14 dakikada ping at → uyanık kal.
# RENDER_URL env değişkenini ayarlamazsan çalışmaz ama
# bot yine de açık kalır (sadece uyuyabilir).
# ─────────────────────────────────────────────
def keep_alive():
    if not RENDER_URL:
        return
    while True:
        try:
            requests.get(f"{RENDER_URL}/health", timeout=10)
        except Exception:
            pass
        time.sleep(14 * 60)  # 14 dakika

# ─────────────────────────────────────────────
# Tüm BIST ticker listesini TwelveData'dan çek
# ─────────────────────────────────────────────
def get_all_bist_tickers():
    if not TWELVE_KEY:
        return []
    try:
        url  = f"https://api.twelvedata.com/stocks?exchange=XIST&apikey={TWELVE_KEY}"
        resp = requests.get(url, timeout=15).json()
        if resp.get('status') == 'error':
            return []
        data = resp.get('data', [])
        return [item['symbol'] for item in data if item.get('exchange') == 'XIST'][:600]
    except Exception:
        return []

# ─────────────────────────────────────────────
# Fiyat verisi – yfinance önce, TwelveData yedek
# BUG FIX: Yeni yfinance MultiIndex kolon döner → düzleştir
# ─────────────────────────────────────────────
def get_data(ticker, period="2y", interval="1d"):
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

    if TWELVE_KEY:
        try:
            url = (
                f"https://api.twelvedata.com/time_series"
                f"?symbol={ticker}&exchange=XIST&interval=1day"
                f"&apikey={TWELVE_KEY}&outputsize=500"
            )
            resp = requests.get(url, timeout=10).json()
            if resp.get('status') == 'error' or 'values' not in resp:
                return pd.DataFrame()
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

# ─────────────────────────────────────────────
# RSI Diverjansı – hizalı indeks ile
# BUG FIX: price ve rsi aynı df'den dropna → boyut garantili eşit
# ─────────────────────────────────────────────
def detect_divergence(df, window=60):
    if len(df) < window:
        return None, None

    recent = df.iloc[-window:].copy()
    recent['RSI'] = ta.rsi(recent['Close'], length=14)
    recent = recent.dropna(subset=['Close', 'RSI'])

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
        return "NEGATİF UYUMSUZLUK", "Satış baskısı!"

    if (len(p_troughs) >= 2 and len(r_troughs) >= 2
            and price[p_troughs[-1]] < price[p_troughs[-2]]
            and rsi[r_troughs[-1]]   > rsi[r_troughs[-2]]):
        return "POZİTİF UYUMSUZLUK", "Alım fırsatı!"

    return None, None

# ─────────────────────────────────────────────
# En iyi EMA çifti backtest
# BUG FIX: Her zaman tuple döner (list karışıklığı yok)
# ─────────────────────────────────────────────
def find_best_ema_pair(ticker):
    df = get_data(ticker)
    if df.empty or len(df) < 100:
        return (9, 21)

    pairs = [(3, 5), (5, 8), (8, 13), (9, 21), (12, 26), (20, 50)]
    best_profit, best_pair = -999.0, (9, 21)

    for short, long in pairs:
        tmp = df.copy()
        tmp['ema_s'] = ta.ema(tmp['Close'], length=short)
        tmp['ema_l'] = ta.ema(tmp['Close'], length=long)
        tmp = tmp.dropna(subset=['ema_s', 'ema_l'])

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

# ─────────────────────────────────────────────
# 4096 karakter Telegram limitini aş
# ─────────────────────────────────────────────
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

# ─────────────────────────────────────────────
# Tüm hisseleri tara
# ─────────────────────────────────────────────
def scan_all_stocks(chat_id):
    chat_id = str(chat_id)
    if chat_id not in watchlist or not watchlist[chat_id]:
        return

    messages = []
    for ticker in watchlist[chat_id]:
        try:
            df = get_data(ticker, "1y", "1d")
            if df.empty or len(df) < 20:
                continue

            df['RSI']   = ta.rsi(df['Close'], length=14)
            ema_pair    = best_emas.get(ticker, (9, 21))
            df['EMA_s'] = ta.ema(df['Close'], length=ema_pair[0])
            df['EMA_l'] = ta.ema(df['Close'], length=ema_pair[1])
            df = df.dropna(subset=['RSI', 'EMA_s', 'EMA_l'])
            if len(df) < 2:
                continue

            cross_up   = (df['EMA_s'].iloc[-2] <= df['EMA_l'].iloc[-2]) and (df['EMA_s'].iloc[-1] > df['EMA_l'].iloc[-1])
            cross_down = (df['EMA_s'].iloc[-2] >= df['EMA_l'].iloc[-2]) and (df['EMA_s'].iloc[-1] < df['EMA_l'].iloc[-1])

            signal = ""
            if cross_up:
                signal = "🚀 AL - EMA CROSS UP"
            elif cross_down:
                signal = "🔻 SAT - EMA CROSS DOWN"

            div_type, div_msg = detect_divergence(df)

            if signal or div_type:
                vol      = df['Close'].pct_change().std() * 100
                karakter = "Yüksek Vol" if vol > 2 else "Düşük Vol"
                msg = (
                    f"📊 *{ticker}* ({karakter})\n"
                    f"{signal}\n"
                    f"RSI: {df['RSI'].iloc[-1]:.1f}\n"
                    f"{div_type or 'Normal'} {div_msg or ''}\n"
                    f"Best EMA: {ema_pair[0]}-{ema_pair[1]}\n"
                    f"📈 https://tr.tradingview.com/chart/?symbol=BIST:{ticker}"
                )
                messages.append(msg)
        except Exception:
            continue
        time.sleep(0.3)

    if messages:
        send_long_message(chat_id, "\n\n".join(messages))

# ─────────────────────────────────────────────
# Telegram komutları
# ─────────────────────────────────────────────
@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(
        message,
        "🚀 *BIST Bot* – RSI Diverjans + EMA Kesişim\n\n"
        "Render restart olursa hemen `/addall` yaz.\n\n"
        "Komutlar:\n"
        "/add HEKTS\n"
        "/remove HEKTS\n"
        "/addall (550+)\n"
        "/optimize HEKTS\n"
        "/check\n"
        "/watchlist",
        parse_mode='Markdown'
    )

@bot.message_handler(commands=['add'])
def add_stock(message):
    try:
        ticker  = message.text.split()[1].upper().replace('.IS', '')
        chat_id = str(message.chat.id)
        watchlist.setdefault(chat_id, [])
        if ticker not in watchlist[chat_id]:
            watchlist[chat_id].append(ticker)
            bot.reply_to(message, f"✅ {ticker} eklendi!")
        else:
            bot.reply_to(message, f"ℹ️ {ticker} zaten listende.")
    except Exception:
        bot.reply_to(message, "❌ Kullanım: /add HEKTS")

@bot.message_handler(commands=['remove'])
def remove_stock(message):
    try:
        ticker  = message.text.split()[1].upper().replace('.IS', '')
        chat_id = str(message.chat.id)
        if ticker in watchlist.get(chat_id, []):
            watchlist[chat_id].remove(ticker)
            bot.reply_to(message, f"🗑️ {ticker} listeden çıkarıldı.")
        else:
            bot.reply_to(message, f"ℹ️ {ticker} listende yok.")
    except Exception:
        bot.reply_to(message, "❌ Kullanım: /remove HEKTS")

@bot.message_handler(commands=['addall'])
def add_all(message):
    chat_id = str(message.chat.id)
    bot.reply_to(message, "🔄 Tüm BIST hisseleri yükleniyor... ~10 sn")
    tickers = get_all_bist_tickers()
    if not tickers:
        bot.reply_to(message, "❌ Ticker listesi alınamadı. TWELVE_DATA_KEY kontrol et.")
        return
    watchlist.setdefault(chat_id, [])
    added = sum(
        1 for t in tickers
        if t not in watchlist[chat_id] and not watchlist[chat_id].append(t)
    )
    bot.reply_to(message, f"✅ {added} hisse eklendi! Toplam: {len(watchlist[chat_id])}")

@bot.message_handler(commands=['optimize'])
def optimize(message):
    try:
        ticker = message.text.split()[1].upper().replace('.IS', '')
        bot.reply_to(message, f"🔍 {ticker} optimize ediliyor (20-30 sn)...")
        pair = find_best_ema_pair(ticker)
        best_emas[ticker] = pair
        bot.reply_to(message, f"✅ {ticker} Best EMA: {pair[0]}-{pair[1]}")
    except Exception:
        bot.reply_to(message, "❌ Kullanım: /optimize HEKTS")

@bot.message_handler(commands=['check'])
def manual_check(message):
    bot.reply_to(message, "🔍 Hisseler taranıyor (birkaç dakika)...")
    scan_all_stocks(str(message.chat.id))
    bot.reply_to(message, "✅ Tarama tamamlandı!")

@bot.message_handler(commands=['watchlist'])
def show_list(message):
    chat_id = str(message.chat.id)
    lst = watchlist.get(chat_id, [])
    if lst:
        send_long_message(chat_id, f"📋 *Listen* ({len(lst)} hisse):\n" + "\n".join(lst))
    else:
        bot.reply_to(message, "Boş → /addall yaz")

# ─────────────────────────────────────────────
# Otomatik günlük tarama – 18:00 TR saati
# BUG FIX: scanned_date ile aynı gün çift tetikleme engellendi
# ─────────────────────────────────────────────
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

# ─────────────────────────────────────────────
# Başlat:
#   1. Keep-alive thread  (Render uyku modunu önler)
#   2. Auto-scan thread   (18:00 otomatik tarama)
#   3. Telegram bot thread
#   4. Flask HTTP sunucusu (Render bu olmazsa servisi öldürür)
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🤖 BIST Bot başlatılıyor – PORT={PORT}")

    threading.Thread(target=keep_alive,  daemon=True).start()
    threading.Thread(target=auto_scan,   daemon=True).start()
    threading.Thread(
        target=lambda: bot.infinity_polling(none_stop=True),
        daemon=True
    ).start()

    # Flask ana thread'de çalışır – Render'ın port kontrolünü geçer
    app.run(host='0.0.0.0', port=PORT)
