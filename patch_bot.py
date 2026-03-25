# patch_bot.py
# Python 3.9+
from pathlib import Path
import shutil
import ast
import sys

def find_bot_file():
    for p in ["bot.py", "Bot.py"]:
        fp = Path(p)
        if fp.exists():
            return fp
    return None

BOT = find_bot_file()
if not BOT:
    print("HATA: bot.py veya Bot.py bulunamadi.")
    sys.exit(1)

src = BOT.read_text(encoding="utf-8")

def fail(msg):
    print(f"HATA: {msg}")
    sys.exit(1)

def ensure_import(line, anchor="from flask import Flask, request, abort"):
    global src
    if line in src:
        return
    i = src.find(anchor)
    if i == -1:
        fail(f"Import anchor bulunamadi: {anchor}")
    i += len(anchor)
    src = src[:i] + "\n" + line + src[i:]

def insert_after(marker, block, label):
    global src
    if block.strip() in src:
        return
    i = src.find(marker)
    if i == -1:
        fail(f"{label}: marker bulunamadi")
    i += len(marker)
    src = src[:i] + "\n" + block.rstrip() + "\n" + src[i:]

def replace_between(start_marker, end_marker, new_block, label):
    global src
    i = src.find(start_marker)
    if i == -1:
        fail(f"{label}: start bulunamadi")
    j = src.find(end_marker, i)
    if j == -1:
        fail(f"{label}: end bulunamadi")
    src = src[:i] + new_block.rstrip() + "\n\n" + src[j:]

def replace_once(old, new, label, required=False):
    global src
    if old in src:
        src = src.replace(old, new, 1)
    elif required:
        fail(f"{label}: bulunamadi")

def replace_all(old, new):
    global src
    src = src.replace(old, new)

# --------------------------------------------------------------------
# 0) Kirik patch kalintilarini temizle (onceki hatali script izi)
# --------------------------------------------------------------------
if "cmd_haber_block = r'''" in src:
    # Bu iz varsa dosya kirik. En guvenli cozum:
    # marker'dan dosya sonuna kadar kesiyoruz ve ana baslatici markerini geri ariyoruz.
    i = src.find("cmd_haber_block = r'''")
    # ana baslatici bolumunu bulmaya calis
    k = src.find("# ═══════════════════════════════════════════════\n# ANA BAŞLATICI", i)
    if k == -1:
        # baslatıcı bulunamazsa markerdan sonrasini at
        src = src[:i]
    else:
        # script kalintisini temizle, ana baslaticiyi koru
        src = src[:i] + "\n\n" + src[k:]

# --------------------------------------------------------------------
# 1) Import ekleri
# --------------------------------------------------------------------
ensure_import("import copy")
ensure_import("from concurrent.futures import ThreadPoolExecutor")
ensure_import("from psycopg2 import pool as pg_pool")

# --------------------------------------------------------------------
# 2) Global ayarlar + controlled task
# --------------------------------------------------------------------
runtime_block = r'''
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
if "MAX_WORKERS = int(os.getenv(" not in src:
    insert_after('TD_DELAY = 1.5   # saniye – Yahoo Finance için yeterli', runtime_block, "runtime-block")

# --------------------------------------------------------------------
# 3) DB bolumu (pool + mem cache + batch)
# --------------------------------------------------------------------
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
        if __import__("time").time() - row["ts"] > DB_MEM_TTL_SEC:
            _db_mem_cache.pop(key, None)
            return None, False
        return copy.deepcopy(row["val"]), True


def _db_cache_put(key, val):
    with _db_mem_lock:
        _db_mem_cache[key] = {"ts": __import__("time").time(), "val": copy.deepcopy(val)}


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
    "db-block"
)

# --------------------------------------------------------------------
# 4) Yahoo lock helper ve kullanimi
# --------------------------------------------------------------------
yahoo_helper = r'''
def _yahoo_get(url, headers=None, timeout=20):
    global _yahoo_last_ts
    with _yahoo_http_lock:
        now = __import__("time").time()
        delta = now - _yahoo_last_ts
        if delta < YAHOO_MIN_REQUEST_GAP:
            __import__("time").sleep(YAHOO_MIN_REQUEST_GAP - delta)
        resp = requests.get(url, headers=headers, timeout=timeout)
        _yahoo_last_ts = __import__("time").time()
        return resp
'''
if "def _yahoo_get(" not in src:
    src = src.replace("def fetch_bist_tickers_yahoo():", yahoo_helper + "\n\ndef fetch_bist_tickers_yahoo():", 1)

replace_all("requests.get(url, headers=headers, timeout=8)", "_yahoo_get(url, headers=headers, timeout=8)")
replace_all("requests.get(url2, headers=headers, timeout=12)", "_yahoo_get(url2, headers=headers, timeout=12)")
replace_all("requests.get(url, headers=headers, timeout=20)", "_yahoo_get(url, headers=headers, timeout=20)")

# --------------------------------------------------------------------
# 5) pc_save / pc_load ve cache invalidation
# --------------------------------------------------------------------
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
replace_between("def pc_save(ticker, df):", "def pc_load(ticker):", pc_save_new, "pc_save")

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
replace_between("def pc_load(ticker):", "def pc_count_today():", pc_load_new, "pc_load")

replace_once(
    "def _invalidate_cache_set():\n    \"\"\"Yeni veri indirince cache setini sıfırla.\"\"\"\n    global _cached_today_set\n    _cached_today_set = None\n",
    "def _invalidate_cache_set():\n    \"\"\"Yeni veri indirince cache setini sıfırla.\"\"\"\n    global _cached_today_set\n    _cached_today_set = None\n    with _price_mem_lock:\n        _price_mem_cache.clear()\n",
    "invalidate-cache",
    required=False,
)

# --------------------------------------------------------------------
# 6) tara_indicators df param + _tara_all df reuse
# --------------------------------------------------------------------
replace_once("def tara_indicators(ticker):", "def tara_indicators(ticker, df_d=None):", "tara_ind-signature", required=False)

replace_once(
    "    df_d, _ = get_data(ticker)\n    if not isinstance(df_d, pd.DataFrame) or df_d.empty or len(df_d) < 60:\n        return None\n",
    "    if df_d is None:\n        df_d, _ = get_data(ticker)\n    if not isinstance(df_d, pd.DataFrame) or df_d.empty or len(df_d) < 60:\n        return None\n",
    "tara_ind-fetch",
    required=False,
)

replace_once(
    "    missing = [t for t in tickers if t not in cached_set]\n",
    "    missing = [t for t in tickers if t not in cached_set]\n    df_cache = {}\n",
    "tara_all-dfcache-init",
    required=False,
)

replace_once(
    "            get_data(ticker)\n",
    "            df_d, _ = get_data(ticker)\n            if isinstance(df_d, pd.DataFrame) and not df_d.empty:\n                df_cache[ticker] = df_d\n",
    "tara_all-download-save-df",
    required=False,
)

replace_once(
    "            ind = tara_indicators(ticker)\n",
    "            if ticker not in df_cache:\n                df_d, _ = get_data(ticker)\n                df_cache[ticker] = df_d\n            ind = tara_indicators(ticker, df_d=df_cache.get(ticker))\n",
    "tara_all-use-df",
    required=False,
)

# --------------------------------------------------------------------
# 7) scan_all_stocks mode + manual_check controlled
# --------------------------------------------------------------------
replace_once(
    "def scan_all_stocks(chat_id, limit=None, ticker_list=None):",
    "def scan_all_stocks(chat_id, limit=None, ticker_list=None, signal_mode=\"buy\"):",
    "scan-signature",
    required=False,
)

replace_once(
    "    messages = []; no_data = 0\n",
    "    messages = []; no_data = 0\n    signal_mode = (signal_mode or \"buy\").lower()\n",
    "scan-mode-init",
    required=False,
)

replace_once(
    "            show = has_signal or rsi_extreme or force_show\n\n            if show:\n",
    "            show = has_signal or rsi_extreme or force_show\n\n"
    "            if not force_show:\n"
    "                if signal_mode == \"buy\" and not any(any(k in s for k in [\"AL\",\"YUKARI\",\"POZİTİF\"]) for s in signals):\n"
    "                    continue\n"
    "                if signal_mode in (\"sell\", \"risk\") and not any(any(k in s for k in [\"SAT\",\"AŞAĞI\",\"NEGATİF\"]) for s in signals):\n"
    "                    continue\n"
    "                if signal_mode == \"all\":\n"
    "                    pass\n\n"
    "            if show:\n",
    "scan-mode-filter",
    required=False,
)

manual_check_block = r'''
@bot.message_handler(commands=['check'])
def manual_check(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()

    if not wl_get(chat_id):
        bot.reply_to(message, "📭 Liste boş! Önce /addall yaz."); return

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
            f"🔍 *{limit} rastgele hisse taranıyor...*\n"
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
        f"🔍 *Tüm liste taranıyor...*\n"
        f"📋 {total} hisse\n"
        f"🎯 Mod: {mode.upper()}\n"
        f"🚫 Durdurmak için: /iptal check")
'''
replace_between(
    "@bot.message_handler(commands=['check'])\ndef manual_check(message):",
    "@bot.message_handler(commands=['sinyal'])",
    manual_check_block,
    "manual_check"
)

# --------------------------------------------------------------------
# 8) Heavy handlers -> controlled task
# --------------------------------------------------------------------
replace_once(
    "    threading.Thread(target=_run_optimizeall, args=(chat_id,), daemon=True).start()",
    "    ok, err = start_controlled_task(chat_id, \"optimizeall\", _run_optimizeall, args=(chat_id,), exclusive=True)\n"
    "    if not ok:\n"
    "        bot.reply_to(message, f\"⏳ {err}\")",
    "optimizeall-handler",
    required=False
)

replace_once(
    "    threading.Thread(target=_run_refreshlist, args=(chat_id,), daemon=True).start()",
    "    ok, err = start_controlled_task(chat_id, \"refreshlist\", _run_refreshlist, args=(chat_id,), exclusive=True)\n"
    "    if not ok:\n"
    "        bot.reply_to(message, f\"⏳ {err}\")",
    "refreshlist-handler",
    required=False
)

replace_once(
    "        threading.Thread(target=_run_optimize, args=(chat_id, ticker), daemon=True).start()",
    "        ok, err = start_controlled_task(chat_id, \"optimize\", _run_optimize, args=(chat_id, ticker), exclusive=True)\n"
    "        if not ok:\n"
    "            bot.reply_to(message, f\"⏳ {err}\")",
    "optimize-handler",
    required=False
)

replace_once(
    "    threading.Thread(target=_run_sira, args=(chat_id, komutlar), daemon=True).start()",
    "    ok, err = start_controlled_task(chat_id, \"sira\", _run_sira, args=(chat_id, komutlar), exclusive=True)\n"
    "    if not ok:\n"
    "        bot.reply_to(message, f\"⏳ {err}\")",
    "sira-handler",
    required=False
)

replace_once(
    "    threading.Thread(target=_run_bulten, daemon=True).start()",
    "    ok, err = start_controlled_task(chat_id, \"bulten\", _run_bulten, args=(), exclusive=True)\n"
    "    if not ok:\n"
    "        bot.reply_to(message, f\"⏳ {err}\")",
    "bulten-handler",
    required=False
)

replace_once(
    "    ticker = parts[1].upper().replace(\".IS\",\"\") if len(parts) > 1 else None\n    threading.Thread(target=_run_haber, args=(ticker,), daemon=True).start()",
    "    ticker = parts[1].upper().replace(\".IS\",\"\") if len(parts) > 1 else None\n"
    "    ok, err = start_controlled_task(chat_id, \"haber\", _run_haber, args=(ticker,), exclusive=True)\n"
    "    if not ok:\n"
    "        bot.reply_to(message, f\"⏳ {err}\")",
    "haber-handler",
    required=False
)

replace_once(
    "    if kod == \"ALL\":\n        threading.Thread(target=_tara_all, args=(chat_id,), daemon=True).start()\n        return",
    "    if kod == \"ALL\":\n        ok, err = start_controlled_task(chat_id, \"tara\", _tara_all, args=(chat_id,), exclusive=True)\n        if not ok:\n            bot.send_message(chat_id, f\"⏳ {err}\")\n        return",
    "tara-all-handler",
    required=False
)

replace_once(
    "    if kod == \"SPADE\":\n        threading.Thread(target=_tara_spade, args=(chat_id,), daemon=True).start()\n        return",
    "    if kod == \"SPADE\":\n        ok, err = start_controlled_task(chat_id, \"tara\", _tara_spade, args=(chat_id,), exclusive=True)\n        if not ok:\n            bot.send_message(chat_id, f\"⏳ {err}\")\n        return",
    "tara-spade-handler",
    required=False
)

replace_once(
    "        threading.Thread(target=_tara_single, args=(chat_id, kod), daemon=True).start()",
    "        ok, err = start_controlled_task(chat_id, \"tara\", _tara_single, args=(chat_id, kod), exclusive=True)\n"
    "        if not ok:\n"
    "            bot.send_message(chat_id, f\"⏳ {err}\")",
    "tara-single-handler",
    required=False
)

# auto_scan cakisma azalt
replace_once(
    "                        threading.Thread(target=_tara_all, args=(chat_id,), daemon=True).start()",
    "                        start_controlled_task(chat_id, \"tara\", _tara_all, args=(chat_id,), exclusive=True)",
    "auto-scan-friday",
    required=False
)

replace_once(
    "                        scan_all_stocks(chat_id)",
    "                        ok, _ = start_controlled_task(chat_id, \"check\", scan_all_stocks, args=(chat_id, None, None, \"buy\"), exclusive=True)\n"
    "                        if not ok:\n"
    "                            continue",
    "auto-scan-check",
    required=False
)

# --------------------------------------------------------------------
# 9) Backtest ekle
# --------------------------------------------------------------------
backtest_block = r'''
def _max_drawdown_pct(equity_curve):
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    mdd = 0.0
    for v in equity_curve:
        if v > peak:
            peak = v
        dd = (v - peak) / peak * 100 if peak > 0 else 0
        if dd < mdd:
            mdd = dd
    return round(mdd, 2)


def backtest_ema(df, short=9, long_=21):
    if df is None or df.empty or len(df) < 120:
        return None

    d = df.copy().dropna(subset=["Close"]).tail(504)
    d["es"] = calc_ema(d["Close"], short)
    d["el"] = calc_ema(d["Close"], long_)
    d = d.dropna(subset=["es", "el"])
    if len(d) < 80:
        return None

    trades = []
    in_pos = False
    entry = 0.0
    equity = [100.0]

    for i in range(1, len(d)):
        ep, lp = d["es"].iloc[i-1], d["el"].iloc[i-1]
        ec, lc = d["es"].iloc[i], d["el"].iloc[i]
        px = float(d["Close"].iloc[i])

        if (not in_pos) and ec > lc and ep <= lp:
            in_pos = True
            entry = px
        elif in_pos and ec < lc and ep >= lp:
            r = (px - entry) / entry * 100
            trades.append(r)
            equity.append(equity[-1] * (1 + r / 100))
            in_pos = False

    if in_pos:
        px = float(d["Close"].iloc[-1])
        r = (px - entry) / entry * 100
        trades.append(r)
        equity.append(equity[-1] * (1 + r / 100))

    if not trades:
        return {"trades":0, "win_rate":0.0, "avg_ret":0.0, "net_ret":0.0, "max_dd":0.0, "profit_factor":0.0}

    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))

    return {
        "trades": len(trades),
        "win_rate": round(len(wins)/len(trades)*100, 2),
        "avg_ret": round(sum(trades)/len(trades), 2),
        "net_ret": round(equity[-1]-100, 2),
        "max_dd": _max_drawdown_pct(equity),
        "profit_factor": round(gross_win/gross_loss, 2) if gross_loss > 0 else 99.0,
    }


def _run_backtest(chat_id, mode="single", ticker=None, limit=None):
    reset_cancel_flag(chat_id, "backtest")

    if mode == "single":
        df_d, _ = get_data(ticker)
        r = backtest_ema(df_d)
        if not r:
            bot.send_message(chat_id, f"❌ {ticker} için veri yetersiz")
            return
        bot.send_message(chat_id,
            f"📘 BACKTEST {ticker} (2 yıl, EMA9/21)\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"İşlem: {r['trades']}\n"
            f"WinRate: %{r['win_rate']}\n"
            f"Ort Getiri: %{r['avg_ret']}\n"
            f"Net Getiri: %{r['net_ret']}\n"
            f"MaxDD: %{r['max_dd']}\n"
            f"ProfitFactor: {r['profit_factor']}")
        return

    tickers = wl_get(chat_id)
    if not tickers:
        bot.send_message(chat_id, "📭 Watchlist boş")
        return
    if limit:
        tickers = tickers[:limit]

    bot.send_message(chat_id, f"📘 Toplu backtest başladı ({len(tickers)} hisse)")
    rows = []

    for i, t in enumerate(tickers, 1):
        if is_cancelled(chat_id, "backtest"):
            bot.send_message(chat_id, "🚫 Backtest iptal edildi")
            return
        try:
            df_d, _ = get_data(t)
            r = backtest_ema(df_d)
            if r and r["trades"] > 0:
                rows.append((t, r))
        except Exception:
            pass
        if i % 25 == 0:
            bot.send_message(chat_id, f"📊 Backtest: {i}/{len(tickers)}")

    if not rows:
        bot.send_message(chat_id, "⚠️ Sonuç bulunamadı")
        return

    rows.sort(key=lambda x: x[1]["net_ret"], reverse=True)
    top = rows[:10]
    worst = rows[-5:]

    lines = ["📘 BACKTEST ÖZET (EMA9/21, son 2 yıl)", "━━━━━━━━━━━━━━━━━━━", "🏆 TOP 10"]
    for t, r in top:
        lines.append(f"{t}: Net %{r['net_ret']} | WR %{r['win_rate']} | PF {r['profit_factor']}")
    lines += ["", "⚠️ EN ZAYIF 5"]
    for t, r in worst:
        lines.append(f"{t}: Net %{r['net_ret']} | WR %{r['win_rate']} | PF {r['profit_factor']}")

    safe_send(chat_id, "\n".join(lines))


@bot.message_handler(commands=['backtest'])
def cmd_backtest(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()

    if len(parts) < 2:
        bot.reply_to(message, "Kullanım: /backtest THYAO | /backtest all [limit]")
        return

    arg = parts[1].lower()
    if arg == "all":
        limit = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
        ok, err = start_controlled_task(chat_id, "backtest", _run_backtest, args=(chat_id, "all", None, limit), exclusive=True)
        if not ok:
            bot.reply_to(message, f"⏳ {err}")
            return
        bot.reply_to(message, "📘 Toplu backtest başladı...")
        return

    ticker = parts[1].upper().replace(".IS", "")
    ok, err = start_controlled_task(chat_id, "backtest", _run_backtest, args=(chat_id, "single", ticker, None), exclusive=True)
    if not ok:
        bot.reply_to(message, f"⏳ {err}")
        return
    bot.reply_to(message, f"📘 {ticker} backtest başladı...")
'''
if "def backtest_ema(" not in src:
    marker = "@bot.message_handler(commands=['spade'])"
    i = src.find(marker)
    if i == -1:
        fail("backtest insertion marker bulunamadi")
    src = src[:i] + backtest_block.rstrip() + "\n\n" + src[i:]

# --------------------------------------------------------------------
# 10) Komut listesi + help
# --------------------------------------------------------------------
if 'BotCommand("backtest"' not in src:
    src = src.replace(
        '        telebot.types.BotCommand("optimizeall",  "Tüm listeyi optimize et"),',
        '        telebot.types.BotCommand("optimizeall",  "Tüm listeyi optimize et"),\n'
        '        telebot.types.BotCommand("backtest",     "Backtest: /backtest THYAO | /backtest all"),'
    )

if "/backtest THYAO" not in src:
    src = src.replace(
        '        "/optimizeall — Tüm listeyi optimize et\\n\\n"\n',
        '        "/optimizeall — Tüm listeyi optimize et\\n"\n'
        '        "/backtest THYAO — son 2 yıl EMA9/21 performans\\n"\n'
        '        "/backtest all 100 — ilk 100 hissede toplu test\\n\\n"\n'
    )

# --------------------------------------------------------------------
# 11) spade progress indent fix (best effort)
# --------------------------------------------------------------------
src = src.replace(
    "            if (i+1) == 1:  # İlk hisseden sonra canlı olduğunu bildir\n                bot.send_message(chat_id, f\"📊 Hesaplama başladı — {tickers[0]} OK\")\n        if (i+1) % 10 == 0:  # Her 10'da bir bildir (haftalık hesap daha ağır)\n                gecen = max(1, int(time.time() - baslangic))\n                kalan = int((len(tickers)-(i+1)) * (gecen/(i+1)))\n                kalan_str = f\"{kalan//60}dk {kalan%60}sn\" if kalan >= 60 else f\"{kalan}sn\"\n                bot.send_message(chat_id,\n                    f\"📊 {i+1}/{len(tickers)} işlendi | \"\n                    f\"🏆 {len(tam_onay_list)} tam | ⚡ {len(master_buy_list)} master\\n\"\n                    f\"⏳ Kalan: ~{kalan_str}\")",
    "            if (i+1) == 1:\n                bot.send_message(chat_id, f\"📊 Hesaplama başladı — {tickers[0]} OK\")\n            if (i+1) % 10 == 0:\n                gecen = max(1, int(time.time() - baslangic))\n                kalan = int((len(tickers)-(i+1)) * (gecen/(i+1)))\n                kalan_str = f\"{kalan//60}dk {kalan%60}sn\" if kalan >= 60 else f\"{kalan}sn\"\n                bot.send_message(chat_id,\n                    f\"📊 {i+1}/{len(tickers)} işlendi | \"\n                    f\"🏆 {len(tam_onay_list)} tam | ⚡ {len(master_buy_list)} master\\n\"\n                    f\"⏳ Kalan: ~{kalan_str}\")"
)

# --------------------------------------------------------------------
# 12) Syntax check + write
# --------------------------------------------------------------------
try:
    ast.parse(src)
except Exception as e:
    print("HATA: Uretilen bot.py syntax hatali, yazilmadi.")
    print(e)
    sys.exit(1)

backup = BOT.with_suffix(".py.bak")
if not backup.exists():
    shutil.copy2(BOT, backup)

BOT.write_text(src, encoding="utf-8")

print("OK: bot.py guncellendi.")
print(f"Yedek: {backup.name}")
print("Test:")
print("  python -m py_compile bot.py")
print("  python bot.py")
