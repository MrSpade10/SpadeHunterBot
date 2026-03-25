# upgrade_bot_once.py
# -*- coding: utf-8 -*-
from pathlib import Path
import re
import ast
import shutil
import sys

BOT = Path("Bot.py")
if not BOT.exists():
    print("HATA: Bot.py bulunamadi.")
    sys.exit(1)

text = BOT.read_text(encoding="utf-8")


def fail(msg):
    print(f"HATA: {msg}")
    sys.exit(1)


def replace_between(start_marker, end_marker, new_block, label):
    global text
    i = text.find(start_marker)
    if i == -1:
        fail(f"{label} start marker bulunamadi: {start_marker[:80]}")
    j = text.find(end_marker, i)
    if j == -1:
        fail(f"{label} end marker bulunamadi: {end_marker[:80]}")
    text = text[:i] + new_block.rstrip() + "\n\n" + text[j:]


def replace_exact(old, new, label, required=True):
    global text
    if old in text:
        text = text.replace(old, new, 1)
    elif required:
        fail(f"{label} bulunamadi.")


def ensure_import(line):
    global text
    if line in text:
        return
    anchor = "from flask import Flask, request, abort"
    idx = text.find(anchor)
    if idx == -1:
        fail("Import anchor bulunamadi")
    ins = idx + len(anchor)
    text = text[:ins] + "\n" + line + text[ins:]


def insert_after(marker, block, label):
    global text
    if block.strip() in text:
        return
    i = text.find(marker)
    if i == -1:
        fail(f"{label} marker bulunamadi")
    i = i + len(marker)
    text = text[:i] + "\n" + block.rstrip() + "\n" + text[i:]


def patch_function_block(func_name, end_marker, mutator, label):
    global text
    start = text.find(f"def {func_name}(")
    if start == -1:
        fail(f"{label}: def {func_name} bulunamadi")
    end = text.find(end_marker, start)
    if end == -1:
        fail(f"{label}: end marker bulunamadi")
    block = text[start:end]
    new_block = mutator(block)
    text = text[:start] + new_block.rstrip() + "\n\n" + text[end:]


# -------------------------------------------------------------------
# 1) Import ekleri
# -------------------------------------------------------------------
ensure_import("import copy")
ensure_import("from urllib.parse import quote_plus")
ensure_import("from concurrent.futures import ThreadPoolExecutor")
ensure_import("from psycopg2 import pool as pg_pool")

# -------------------------------------------------------------------
# 2) Global ayarlar + controlled task
# -------------------------------------------------------------------
globals_block = r'''
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
YAHOO_MIN_REQUEST_GAP = float(os.getenv("YAHOO_MIN_REQUEST_GAP", "0.15"))
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "6"))
DB_MEM_TTL_SEC = int(os.getenv("DB_MEM_TTL_SEC", "60"))

_yahoo_http_lock = threading.Lock()
_yahoo_last_ts = 0.0

_db_pool = None
_db_pool_lock = threading.Lock()
_db_mem_cache = {}
_db_mem_lock = threading.Lock()

_price_mem_cache = {}
_price_mem_lock = threading.Lock()

_tarama_ind_cache = {"date": None, "data": {}}
_tarama_ind_lock = threading.Lock()

_heavy_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="bist-heavy")
_task_lock = threading.Lock()
_task_futures = {}  # (chat_id, op) -> future


def _task_key(chat_id, op):
    return (str(chat_id), str(op))


def _active_ops_for_chat(chat_id):
    cid = str(chat_id)
    return [op for (c, op), fut in _task_futures.items() if c == cid and not fut.done()]


def start_controlled_task(chat_id, op, fn, args=(), exclusive=False):
    key = _task_key(chat_id, op)
    with _task_lock:
        old = _task_futures.get(key)
        if old and not old.done():
            return False, f"{op} zaten calisiyor"

        if exclusive:
            actives = _active_ops_for_chat(chat_id)
            if actives:
                return False, f"aktif islem var: {', '.join(sorted(set(actives)))}"

        def _runner():
            try:
                fn(*args)
            finally:
                with _task_lock:
                    _task_futures.pop(key, None)

        fut = _heavy_executor.submit(_runner)
        _task_futures[key] = fut

    return True, None
'''
insert_after('TD_DELAY = 1.5   # saniye – Yahoo Finance için yeterli', globals_block, "global-block")

# -------------------------------------------------------------------
# 3) DB bolumu tam degisim (pool + mem cache + batch)
# -------------------------------------------------------------------
db_block = r'''
class _PooledConnWrapper:
    def __init__(self, p, conn):
        self._pool = p
        self._conn = conn
        self._closed = False

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def close(self):
        if not self._closed:
            self._pool.putconn(self._conn)
            self._closed = True


def _ensure_db_pool():
    global _db_pool
    if not DATABASE_URL:
        return None
    if _db_pool is not None:
        return _db_pool
    with _db_pool_lock:
        if _db_pool is None:
            try:
                _db_pool = pg_pool.ThreadedConnectionPool(
                    DB_POOL_MIN, DB_POOL_MAX, DATABASE_URL, sslmode='require'
                )
            except Exception as e:
                print(f"DB pool hata, direct fallback: {e}")
                _db_pool = None
    return _db_pool


def _db_cache_get(key):
    with _db_mem_lock:
        row = _db_mem_cache.get(key)
        if not row:
            return None, False
        if time.time() - row["ts"] > DB_MEM_TTL_SEC:
            _db_mem_cache.pop(key, None)
            return None, False
        return copy.deepcopy(row["val"]), True


def _db_cache_put(key, val):
    with _db_mem_lock:
        _db_mem_cache[key] = {"ts": time.time(), "val": copy.deepcopy(val)}


def _db_cache_del(key):
    with _db_mem_lock:
        _db_mem_cache.pop(key, None)


def db_connect():
    if not DATABASE_URL:
        return None
    p = _ensure_db_pool()
    if p is not None:
        try:
            return _PooledConnWrapper(p, p.getconn())
        except Exception as e:
            print(f"DB pool getconn hata, direct fallback: {e}")
    try:
        return psycopg2.connect(DATABASE_URL, sslmode='require')
    except Exception as e:
        print(f"DB hata: {e}")
        return None


def db_init():
    conn = db_connect()
    if not conn:
        print("DB yok - in-memory mod.")
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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_price_cache_date ON price_cache(fetched_at)")
        conn.commit()
        print("DB hazir.")
    except Exception as e:
        print(f"DB init hata: {e}")
    finally:
        conn.close()


def db_get(key, default=None):
    val, ok = _db_cache_get(key)
    if ok:
        return val
    conn = db_connect()
    if not conn:
        return default
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM store WHERE key=%s", (key,))
            row = cur.fetchone()
            out = json.loads(row[0]) if row else default
            _db_cache_put(key, out)
            return out
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
            """, (key, json.dumps(value, ensure_ascii=False)))
        conn.commit()
        _db_cache_put(key, value)
    except Exception as e:
        print(f"DB set hata: {e}")
    finally:
        conn.close()


def db_set_many(items):
    if not items:
        return
    conn = db_connect()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            for k, v in items:
                cur.execute("""
                    INSERT INTO store(key,value) VALUES(%s,%s)
                    ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
                """, (k, json.dumps(v, ensure_ascii=False)))
                _db_cache_put(k, v)
        conn.commit()
    except Exception as e:
        print(f"DB set_many hata: {e}")
    finally:
        conn.close()


def db_del(key):
    conn = db_connect()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM store WHERE key=%s", (key,))
        conn.commit()
        _db_cache_del(key)
    except Exception as e:
        print(f"DB del hata: {e}")
    finally:
        conn.close()
'''
replace_between(
    "def db_connect():",
    "# ═══════════════════════════════════════════════════════════════\n# TRADE KİTABI",
    db_block,
    "db-block",
)

# -------------------------------------------------------------------
# 4) Yahoo lock helper
# -------------------------------------------------------------------
yahoo_helper = r'''
def _yahoo_get(url, headers=None, timeout=20):
    global _yahoo_last_ts
    with _yahoo_http_lock:
        now = time.time()
        delta = now - _yahoo_last_ts
        if delta < YAHOO_MIN_REQUEST_GAP:
            time.sleep(YAHOO_MIN_REQUEST_GAP - delta)
        resp = requests.get(url, headers=headers, timeout=timeout)
        _yahoo_last_ts = time.time()
        return resp
'''
insert_after("def detect_divergence(df, window=60, min_bars=5, max_bars=40):", yahoo_helper, "yahoo-helper-insert")

# fetch_bist_tickers_yahoo icinde requests.get -> _yahoo_get
def _patch_fetch_bist(block):
    block = block.replace("requests.get(url, headers=headers, timeout=8)", "_yahoo_get(url, headers=headers, timeout=8)")
    block = block.replace("requests.get(url2, headers=headers, timeout=12)", "_yahoo_get(url2, headers=headers, timeout=12)")
    return block

patch_function_block("fetch_bist_tickers_yahoo", "\n\ndef get_all_bist_tickers():", _patch_fetch_bist, "fetch_bist_tickers_yahoo")

# fetch_yahoo_direct icinde requests.get -> _yahoo_get
def _patch_fetch_yahoo_direct(block):
    block = block.replace("requests.get(url, headers=headers, timeout=20)", "_yahoo_get(url, headers=headers, timeout=20)")
    return block

patch_function_block("fetch_yahoo_direct", "\n\ndef resample_weekly(", _patch_fetch_yahoo_direct, "fetch_yahoo_direct")

# -------------------------------------------------------------------
# 5) pc_save / pc_load / invalidate cache
# -------------------------------------------------------------------
pc_save_new = r'''
def pc_save(ticker, df):
    conn = db_connect()
    if not conn:
        return
    try:
        data = df.reset_index()
        data['datetime'] = data['datetime'].astype(str)
        payload = data.to_json(orient='records')
        today_tr = datetime.now(pytz.timezone('Europe/Istanbul')).date()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO price_cache(ticker, fetched_at, data)
                VALUES(%s, %s, %s)
                ON CONFLICT(ticker) DO UPDATE
                    SET fetched_at=EXCLUDED.fetched_at, data=EXCLUDED.data
            """, (ticker, today_tr, payload))
        conn.commit()
        with _price_mem_lock:
            _price_mem_cache[ticker] = {"date": today_tr, "df": df.copy()}
    except Exception as e:
        print(f"pc_save hata {ticker}: {e}")
    finally:
        conn.close()
'''
replace_between("def pc_save(ticker, df):", "\n\ndef pc_load(ticker):", pc_save_new, "pc_save")

pc_load_new = r'''
def pc_load(ticker):
    today = datetime.now(pytz.timezone('Europe/Istanbul')).date()
    with _price_mem_lock:
        row = _price_mem_cache.get(ticker)
        if row and row.get("date") == today:
            return row["df"].copy()

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
            if fetched_at < today:
                return None
            records = json.loads(row[0])
            df = pd.DataFrame(records)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.set_index('datetime').sort_index()
            for col in ['Open','High','Low','Close','Volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            with _price_mem_lock:
                _price_mem_cache[ticker] = {"date": today, "df": df.copy()}
            return df
    except Exception as e:
        print(f"pc_load hata {ticker}: {e}")
        return None
    finally:
        conn.close()
'''
replace_between("def pc_load(ticker):", "\n\ndef pc_count_today():", pc_load_new, "pc_load")

# _invalidate_cache_set update
replace_exact(
    "def _invalidate_cache_set():\n    \"\"\"Yeni veri indirince cache setini sıfırla.\"\"\"\n    global _cached_today_set\n    _cached_today_set = None",
    "def _invalidate_cache_set():\n    \"\"\"Yeni veri indirince cache setini sifirla.\"\"\"\n    global _cached_today_set\n    _cached_today_set = None\n    with _price_mem_lock:\n        _price_mem_cache.clear()",
    "invalidate-cache-update",
    required=False,
)

# -------------------------------------------------------------------
# 6) tara_indicators patch (df param + cache)
# -------------------------------------------------------------------
def patch_tara_indicators(block):
    block = block.replace("def tara_indicators(ticker):", "def tara_indicators(ticker, df_d=None):", 1)

    old_fetch = (
        "    df_d, _ = get_data(ticker)\n"
        "    if not isinstance(df_d, pd.DataFrame) or df_d.empty or len(df_d) < 60:\n"
        "        return None\n"
    )
    new_fetch = (
        "    if df_d is None:\n"
        "        today_key = datetime.now(pytz.timezone(\"Europe/Istanbul\")).strftime(\"%Y-%m-%d\")\n"
        "        with _tarama_ind_lock:\n"
        "            if _tarama_ind_cache[\"date\"] != today_key:\n"
        "                _tarama_ind_cache[\"date\"] = today_key\n"
        "                _tarama_ind_cache[\"data\"] = {}\n"
        "            cached = _tarama_ind_cache[\"data\"].get(ticker)\n"
        "        if cached is not None:\n"
        "            return copy.deepcopy(cached)\n"
        "        df_d, _ = get_data(ticker)\n"
        "\n"
        "    if not isinstance(df_d, pd.DataFrame) or df_d.empty or len(df_d) < 60:\n"
        "        return None\n"
    )
    if old_fetch in block:
        block = block.replace(old_fetch, new_fetch, 1)
    else:
        fail("tara_indicators fetch blogu bulunamadi")

    # return dict -> result dict
    block = block.replace("\n    return {\n", "\n    result = {\n", 1)

    # sona cache yazma + return
    pos = block.rfind("\n    }\n")
    if pos == -1:
        fail("tara_indicators return dict sonu bulunamadi")

    tail = (
        "\n    }\n"
        "\n"
        "    with _tarama_ind_lock:\n"
        "        today_key = datetime.now(pytz.timezone(\"Europe/Istanbul\")).strftime(\"%Y-%m-%d\")\n"
        "        if _tarama_ind_cache[\"date\"] != today_key:\n"
        "            _tarama_ind_cache[\"date\"] = today_key\n"
        "            _tarama_ind_cache[\"data\"] = {}\n"
        "        _tarama_ind_cache[\"data\"][ticker] = copy.deepcopy(result)\n"
        "\n"
        "    return copy.deepcopy(result)\n"
    )
    block = block[:pos] + tail + block[pos + len("\n    }\n"):]
    return block


patch_function_block("tara_indicators", "\n\n# ── Strateji Filtreleri", patch_tara_indicators, "tara_indicators")

# -------------------------------------------------------------------
# 7) _tara_all icinde df cache kullanimi
# -------------------------------------------------------------------
def patch_tara_all(block):
    block = block.replace(
        "    missing = [t for t in tickers if t not in cached_set]\n",
        "    missing = [t for t in tickers if t not in cached_set]\n    df_cache = {}\n",
        1,
    )

    block = block.replace(
        "            get_data(ticker)\n",
        "            df_d, _ = get_data(ticker)\n            if isinstance(df_d, pd.DataFrame) and not df_d.empty:\n                df_cache[ticker] = df_d\n",
        1,
    )

    block = block.replace(
        "            ind = tara_indicators(ticker)\n",
        "            if ticker not in df_cache:\n                df_d, _ = get_data(ticker)\n                df_cache[ticker] = df_d\n            ind = tara_indicators(ticker, df_d=df_cache.get(ticker))\n",
        1,
    )
    return block


patch_function_block("_tara_all", "\n\n@bot.message_handler(commands=['tarasonuc'])", patch_tara_all, "_tara_all")

# -------------------------------------------------------------------
# 8) scan_all_stocks patch (mode + gap + confidence/risk satiri)
# -------------------------------------------------------------------
def patch_scan_all(block):
    block = block.replace(
        "def scan_all_stocks(chat_id, limit=None, ticker_list=None):",
        "def scan_all_stocks(chat_id, limit=None, ticker_list=None, signal_mode=\"buy\"):",
        1,
    )

    block = block.replace(
        "    messages = []; no_data = 0\n",
        "    messages = []; no_data = 0\n    signal_mode = (signal_mode or \"buy\").lower()\n",
        1,
    )

    marker = "                # ── Mesaj bloklarını oluştur ──"
    filt = (
        "                # Mode filtreleme\n"
        "                if not force_show:\n"
        "                    sm = signal_mode\n"
        "                    if sm == \"buy\" and not buy_sigs:\n"
        "                        continue\n"
        "                    if sm in (\"sell\", \"risk\") and not sell_sigs:\n"
        "                        continue\n"
        "                    if sm == \"all\" and not (buy_sigs or sell_sigs or rsi_extreme):\n"
        "                        continue\n\n"
    )
    if marker in block and "Mode filtreleme" not in block:
        block = block.replace(marker, filt + marker, 1)

    block = block.replace(
        "                parts_msg = [header, \"\"]\n",
        "                if force_show and has_daily and len(df_d) >= 2:\n"
        "                    prev_close = float(df_d['Close'].iloc[-2])\n"
        "                    open_today = float(df_d['Open'].iloc[-1])\n"
        "                    gap_pct = ((open_today - prev_close) / prev_close * 100) if prev_close > 0 else 0.0\n"
        "                    gap_type = 'GAP UP' if gap_pct > 0 else ('GAP DOWN' if gap_pct < 0 else 'FLAT')\n"
        "                    parts_msg = [header, f\"🕳 Acilis:{open_today:.2f} | Onceki Kapanis:{prev_close:.2f} | Gap:{gap_pct:+.2f}% ({gap_type})\", \"\"]\n"
        "                else:\n"
        "                    parts_msg = [header, \"\"]\n",
        1,
    )

    add_before_ema = (
        "                # Guven + rejim + risk satiri\n"
        "                regime = \"UNKNOWN\"\n"
        "                conf = 50\n"
        "                risk_line = None\n"
        "                try:\n"
        "                    if has_daily and len(df_d) >= 220:\n"
        "                        close_s = df_d['Close']\n"
        "                        ema50 = calc_ema(close_s, 50).iloc[-1]\n"
        "                        ema200 = calc_ema(close_s, 200).iloc[-1]\n"
        "                        adxv = calc_adx(df_d).iloc[-1]\n"
        "                        vol20 = close_s.pct_change().rolling(20).std().iloc[-1] * 100\n"
        "                        if not pd.isna(ema50) and not pd.isna(ema200) and not pd.isna(adxv) and not pd.isna(vol20):\n"
        "                            if ema50 > ema200 and adxv >= 22:\n"
        "                                regime = 'TREND_UP'\n"
        "                            elif ema50 < ema200 and adxv >= 22:\n"
        "                                regime = 'TREND_DOWN'\n"
        "                            elif vol20 >= 2.5:\n"
        "                                regime = 'HIGH_VOL'\n"
        "                            else:\n"
        "                                regime = 'RANGE'\n"
        "\n"
        "                    conf = 50\n"
        "                    if buy_sigs:\n"
        "                        conf += 10\n"
        "                    if sell_sigs:\n"
        "                        conf += 5\n"
        "                    if vm.get('confirm_buy') or vm.get('confirm_sell'):\n"
        "                        conf += 12\n"
        "                    rv = vm.get('vol_ratio') or 0\n"
        "                    if rv >= 1.5:\n"
        "                        conf += 8\n"
        "                    if regime in ('TREND_UP', 'TREND_DOWN'):\n"
        "                        conf += 7\n"
        "                    if regime == 'HIGH_VOL':\n"
        "                        conf -= 8\n"
        "                    conf = max(0, min(100, int(conf)))\n"
        "\n"
        "                    if has_daily and buy_sigs and len(df_d) >= 20:\n"
        "                        high = df_d['High']; low = df_d['Low']; close = df_d['Close']\n"
        "                        tr = pd.concat([\n"
        "                            high - low,\n"
        "                            (high - close.shift()).abs(),\n"
        "                            (low - close.shift()).abs()\n"
        "                        ], axis=1).max(axis=1)\n"
        "                        atr = tr.rolling(14).mean().iloc[-1]\n"
        "                        entry = float(close.iloc[-1])\n"
        "                        if not pd.isna(atr) and atr > 0:\n"
        "                            stop = entry - 1.5 * atr\n"
        "                            tp1 = entry + 1.5 * atr\n"
        "                            tp2 = entry + 3.0 * atr\n"
        "                            rr = abs(tp1 - entry) / abs(entry - stop) if abs(entry - stop) > 0 else 0\n"
        "                            risk_line = f\"🛡 Risk -> Giris:{entry:.2f} Stop:{stop:.2f} TP1:{tp1:.2f} TP2:{tp2:.2f} RR:{rr:.2f}\"\n"
        "                except Exception:\n"
        "                    pass\n"
        "\n"
        "                parts_msg.append(f\"🧭 Rejim: {regime} | 🎚 Guven: {conf}/100\")\n"
        "                if risk_line:\n"
        "                    parts_msg.append(risk_line)\n"
    )
    if "🧭 Rejim:" not in block:
        block = block.replace("                parts_msg.append(f\"📐 EMA → G:{ep_d[0]}-{ep_d[1]} | H:{ep_w[0]}-{ep_w[1]}\")",
                              add_before_ema + "                parts_msg.append(f\"📐 EMA → G:{ep_d[0]}-{ep_d[1]} | H:{ep_w[0]}-{ep_w[1]}\")", 1)
    return block


patch_function_block("scan_all_stocks", "\n\ndef _db_cached_today", patch_scan_all, "scan_all_stocks")

# -------------------------------------------------------------------
# 9) manual_check tam degisim
# -------------------------------------------------------------------
manual_check_block = r'''
@bot.message_handler(commands=['check'])
def manual_check(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()

    if not wl_get(chat_id):
        bot.reply_to(message, "📭 Liste bos! Once /addall yaz."); return

    # /check THYAO
    if len(parts) > 1 and parts[1].upper() not in ("ALL","TUM","TÜMÜ","SELL","RISK","FULL") and not parts[1].isdigit():
        ticker = parts[1].upper().replace(".IS","")
        reset_cancel_flag(chat_id, "check")
        ok, err = start_controlled_task(
            chat_id, "check", scan_all_stocks,
            args=(chat_id, None, [ticker], "all"),
            exclusive=True
        )
        if not ok:
            bot.reply_to(message, f"⏳ {err}"); return
        bot.reply_to(message, f"🔍 *{ticker}* analiz ediliyor...")
        return

    mode = "buy"
    if len(parts) > 1:
        p = parts[1].lower()
        if p in ("sell", "risk"):
            mode = "sell"
        elif p == "full":
            mode = "all"

    # /check 50
    if len(parts) > 1 and parts[1].isdigit():
        limit = int(parts[1])
        reset_cancel_flag(chat_id, "check")
        ok, err = start_controlled_task(
            chat_id, "check", scan_all_stocks,
            args=(chat_id, limit, None, mode),
            exclusive=True
        )
        if not ok:
            bot.reply_to(message, f"⏳ {err}"); return
        bot.reply_to(message,
            f"🔍 *{limit} rastgele hisse taraniyor...*\n"
            f"🎯 Mod: {mode.upper()}\n"
            f"🚫 Durdurmak için: /iptal check")
        return

    # /check all veya /check
    total = len(wl_get(chat_id))
    reset_cancel_flag(chat_id, "check")
    ok, err = start_controlled_task(
        chat_id, "check", scan_all_stocks,
        args=(chat_id, None, None, mode),
        exclusive=True
    )
    if not ok:
        bot.reply_to(message, f"⏳ {err}"); return

    bot.reply_to(message,
        f"🔍 *Tarama basladi...*\n"
        f"📋 {total} hisse\n"
        f"🎯 Mod: {mode.upper()}\n"
        f"🚫 Durdurmak için: /iptal check")
'''
replace_between(
    "@bot.message_handler(commands=['check'])\ndef manual_check(message):",
    "\n\n@bot.message_handler(commands=['sinyal'])",
    manual_check_block,
    "manual_check",
)

# -------------------------------------------------------------------
# 10) /tara icindeki thread'leri controlled task'a cevir
# -------------------------------------------------------------------
replace_exact(
    "    if kod == \"ALL\":\n        threading.Thread(target=_tara_all, args=(chat_id,), daemon=True).start()\n        return",
    "    if kod == \"ALL\":\n        ok, err = start_controlled_task(chat_id, \"tara\", _tara_all, args=(chat_id,), exclusive=True)\n        if not ok:\n            bot.send_message(chat_id, f\"⏳ {err}\")\n        return",
    "cmd_tara_all_patch",
    required=False,
)

replace_exact(
    "    if kod == \"SPADE\":\n        threading.Thread(target=_tara_spade, args=(chat_id,), daemon=True).start()\n        return",
    "    if kod == \"SPADE\":\n        ok, err = start_controlled_task(chat_id, \"tara\", _tara_spade, args=(chat_id,), exclusive=True)\n        if not ok:\n            bot.send_message(chat_id, f\"⏳ {err}\")\n        return",
    "cmd_tara_spade_patch",
    required=False,
)

replace_exact(
    "        threading.Thread(target=_tara_single, args=(chat_id, kod), daemon=True).start()",
    "        ok, err = start_controlled_task(chat_id, \"tara\", _tara_single, args=(chat_id, kod), exclusive=True)\n        if not ok:\n            bot.send_message(chat_id, f\"⏳ {err}\")",
    "cmd_tara_single_patch",
    required=False,
)

# -------------------------------------------------------------------
# 11) /haber tam degisim (KAP + priority + sector alarm)
# -------------------------------------------------------------------
cmd_haber_block = r'''
@bot.message_handler(commands=['haber'])
def cmd_haber(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("haber", chat_id, parts[1] if len(parts)>1 else "genel")

    if not _groq_keys():
        bot.reply_to(message, "❌ Hic GROQ_KEY tanimli degil. Render'a ekleyin."); return

    ticker = parts[1].upper().replace(".IS","") if len(parts) > 1 else None

    def _rank(item):
        textx = f"{item.get('title','')} {item.get('desc','')} {item.get('source','')}".lower()
        kritik = ["fed","faiz","rate","enflasyon","inflation","tcmb","kriz","savas","yaptirim","resesyon"]
        yuksek = ["kap","ozel durum","bilanco","kar","zarar","temettu"]
        orta = ["dolar","petrol","altin","risk istahi","brent"]
        if any(k in textx for k in kritik): return "kritik"
        if any(k in textx for k in yuksek): return "yuksek"
        if any(k in textx for k in orta): return "orta"
        return "dusuk"

    def _dedupe(items):
        seen = set()
        out = []
        for n in items:
            key = ((n.get("title","") or "").strip().lower(), (n.get("link","") or "").strip().lower())
            if key in seen:
                continue
            seen.add(key)
            out.append(n)
        return out

    def _card(i, n):
        em = {"kritik":"🔴","yuksek":"🟠","orta":"🟡","dusuk":"🟢"}.get(n.get("priority","orta"), "🟡")
        lines = [f"{em} {i}. [{n.get('source','?')}] {n.get('title','').strip()}"]
        if n.get("desc"):
            lines.append(f"📝 {n['desc'][:160].strip()}")
        if n.get("pub"):
            lines.append(f"🕒 {n['pub']}")
        return "\\n".join(lines)

    def _block(title, items, icon="📰"):
        lines = [f"{icon} {title}", "━━━━━━━━━━━━━━━━━━━"]
        for idx, it in enumerate(items[:5], 1):
            lines.append(_card(idx, it))
            lines.append("━━━━━━━━━━━━━━━━━━━")
        return "\\n".join(lines)

    def _kap_news(tick=None, max_items=6):
        queries = []
        if tick:
            queries.append(f"site:kap.org.tr {tick} KAP")
            queries.append(f"{tick} ozel durum aciklamasi KAP")
        else:
            queries.append("site:kap.org.tr BIST KAP aciklamasi")
        out = []
        for q in queries:
            url = "https://news.google.com/rss/search?q=" + quote_plus(q) + "&hl=tr&gl=TR&ceid=TR:tr"
            items = fetch_rss(url, max_items=max_items)
            for it in items:
                it["source"] = "KAP"
                it["category"] = "kap"
                out.append(it)
        return _dedupe(out)

    def _sector_alarm(news_items):
        sektor_map = {
            "banka": ["AKBNK","GARAN","ISCTR","YKBNK","HALKB","VAKBN"],
            "havacilik": ["THYAO","PGSUS","TAVHL"],
            "enerji": ["TUPRS","AKSEN","EN
