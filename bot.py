import os
import collections
import time
import json
import threading
from datetime import datetime, timedelta
import telebot
import pandas as pd
import numpy as np
import traceback
import requests
from dotenv import load_dotenv
import pytz
import psycopg2
import xml.etree.ElementTree as ET
from flask import Flask, request, abort

load_dotenv()
TOKEN        = (os.getenv('TELEGRAM_TOKEN') or '').strip()
RENDER_URL   = (os.getenv('RENDER_URL') or '').strip()
DATABASE_URL = (os.getenv('DATABASE_URL') or '').strip()
PORT         = int(os.getenv('PORT', 10000))
GEMINI_KEY   = (os.getenv('GEMINI_KEY')  or '').strip()
GEMINI_KEY2  = (os.getenv('GEMINI_KEY2') or '').strip()
GEMINI_KEY3  = (os.getenv('GEMINI_KEY3') or '').strip()
GROQ_KEY     = (os.getenv('GROQ_KEY')  or '').strip()
GROQ_KEY2    = (os.getenv('GROQ_KEY2') or '').strip()
GROQ_KEY3    = (os.getenv('GROQ_KEY3') or '').strip()

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





# ── find_peaks: scipy kaldırıldı, numpy implementasyonu ──────
def find_peaks(arr, distance=1):
    """scipy.signal.find_peaks yerine basit numpy implementasyonu."""
    arr = np.asarray(arr)
    peaks = []
    n = len(arr)
    i = 1
    while i < n - 1:
        if arr[i] > arr[i-1] and arr[i] > arr[i+1]:
            if not peaks or (i - peaks[-1]) >= distance:
                peaks.append(i)
        i += 1
    return np.array(peaks), {}


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

def db_del(key):
    conn = db_connect()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM store WHERE key=%s", (key,))
        conn.commit()
    except Exception as e:
        print(f"DB del hata: {e}")
    finally:
        conn.close()




# ═══════════════════════════════════════════════════════════════
# TRADE KİTABI — /al /sat /kitap
# Her alım satım kaydedilir, AI yorumu eklenir, istatistik tutulur
# ═══════════════════════════════════════════════════════════════

def kitap_get(chat_id):
    """Trade kitabını DB'den yükle. Liste halinde işlemler döner."""
    return db_get(f"kitap:{chat_id}", [])

def kitap_set(chat_id, islemler):
    """Trade kitabını DB'ye kaydet."""
    db_set(f"kitap:{chat_id}", islemler)

def kitap_acik_get(chat_id):
    """Açık pozisyonları DB'den yükle. {ticker: {fiyat, adet, tarih, neden}} dict döner."""
    return db_get(f"kitap_acik:{chat_id}", {})

def kitap_acik_set(chat_id, acik):
    """Açık pozisyonları DB'ye kaydet."""
    db_set(f"kitap_acik:{chat_id}", acik)

@bot.message_handler(commands=['al'])
def cmd_kitap_al(message):
    """
    Trade kitabına alış ekle.
    Kullanım: /al THYAO 145.50 100 SMA200 üstüne geçti RSI 52
    Sebep zorunlu değil ama yazılırsa AI yorumda daha iyi analiz yapar.
    """
    chat_id = str(message.chat.id)
    parts = message.text.strip().split(None, 4)  # max 5 parça: /al TICKER FIYAT ADET [SEBEP]
    if len(parts) < 4:
        bot.reply_to(message,
            "📗 TRADE KİTABI — Alış\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Kullanım: /al THYAO 145.50 100 sebebi\n\n"
            "Örnekler:\n"
            "/al THYAO 145.50 100 SMA200 üstüne geçti, RSI 52\n"
            "/al EREGL 89.20 200 Strateji 2 sinyali, hacim 2x\n\n"
            "Sebep yazmasan da olur ama AI yorumu daha zengin olur."
        )
        return

    try:
        ticker = parts[1].upper().replace(".IS", "")
        fiyat  = float(parts[2].replace(",", "."))
        adet   = float(parts[3].replace(",", "."))
        neden  = parts[4].strip() if len(parts) > 4 else ""
    except (ValueError, IndexError):
        bot.reply_to(message, "❌ Format hatalı.\nÖrnek: /al THYAO 145.50 100 RSI kırılımı")
        return

    if fiyat <= 0 or adet <= 0:
        bot.reply_to(message, "❌ Fiyat ve adet sıfırdan büyük olmalı.")
        return

    tr_tz = pytz.timezone("Europe/Istanbul")
    tarih = datetime.now(tr_tz).strftime("%d.%m.%Y %H:%M")
    toplam = fiyat * adet

    # Açık pozisyonlara ekle — ağırlıklı ortalama maliyet
    acik = kitap_acik_get(chat_id)
    if ticker in acik:
        mevcut = acik[ticker]
        eski_adet = float(mevcut["adet"])
        eski_fiyat = float(mevcut["fiyat"])
        yeni_adet = eski_adet + adet
        yeni_fiyat = (eski_adet * eski_fiyat + adet * fiyat) / yeni_adet
        acik[ticker]["adet"]  = round(yeni_adet, 2)
        acik[ticker]["fiyat"] = round(yeni_fiyat, 4)
        acik[ticker]["neden"] = neden or acik[ticker].get("neden", "")
    else:
        acik[ticker] = {
            "fiyat": round(fiyat, 4),
            "adet":  round(adet, 2),
            "tarih": tarih,
            "neden": neden,
        }
    kitap_acik_set(chat_id, acik)

    # Trade kitabına da kaydet (ham işlem olarak)
    islemler = kitap_get(chat_id)
    islem_id = len(islemler) + 1
    islemler.append({
        "id":     islem_id,
        "yon":    "AL",
        "ticker": ticker,
        "fiyat":  round(fiyat, 4),
        "adet":   round(adet, 2),
        "tarih":  tarih,
        "neden":  neden,
        "ai_yorum": "",
    })
    kitap_set(chat_id, islemler)

    acik_bilgi = ""
    if ticker in acik and float(acik[ticker]["adet"]) > adet:
        acik_bilgi = f"\nOrt. maliyet: {acik[ticker]['fiyat']:.2f}₺ | Toplam: {acik[ticker]['adet']:.0f} adet"

    bot.reply_to(message,
        f"📗 *{ticker}* alış kaydedildi (#{islem_id})\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"💰 {adet:.0f} adet × {fiyat:.2f}₺ = {toplam:,.0f}₺\n"
        f"📅 {tarih}\n"
        f"📝 Sebep: {neden if neden else '(belirtilmedi)'}"
        f"{acik_bilgi}\n\n"
        f"Satış için: /sat {ticker} FIYAT ADET sebep"
    )

@bot.message_handler(commands=['sat'])
def cmd_kitap_sat(message):
    """
    Trade kitabına satış ekle. Kâr/zarar hesaplanır ve AI yorumu yapılır.
    Kullanım: /sat THYAO 162.00 100 Hedef fiyata ulaştı
    """
    chat_id = str(message.chat.id)
    parts = message.text.strip().split(None, 4)
    if len(parts) < 4:
        bot.reply_to(message,
            "📕 TRADE KİTABI — Satış\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Kullanım: /sat THYAO 162.00 100 sebebi\n\n"
            "Örnekler:\n"
            "/sat THYAO 162.00 100 Hedef fiyata ulaştı\n"
            "/sat EREGL 85.00 200 Stop-loss tetiklendi\n\n"
            "Sebep yazmasan da olur — AI yine de analiz yapar."
        )
        return

    try:
        ticker = parts[1].upper().replace(".IS", "")
        fiyat  = float(parts[2].replace(",", "."))
        adet   = float(parts[3].replace(",", "."))
        neden  = parts[4].strip() if len(parts) > 4 else ""
    except (ValueError, IndexError):
        bot.reply_to(message, "❌ Format hatalı.\nÖrnek: /sat THYAO 162.00 100 stop tetiklendi")
        return

    if fiyat <= 0 or adet <= 0:
        bot.reply_to(message, "❌ Fiyat ve adet sıfırdan büyük olmalı.")
        return

    tr_tz = pytz.timezone("Europe/Istanbul")
    tarih = datetime.now(tr_tz).strftime("%d.%m.%Y %H:%M")
    acik = kitap_acik_get(chat_id)

    # Alış maliyeti hesapla
    if ticker in acik:
        alis_fiyat = float(acik[ticker]["fiyat"])
        alis_tarih = acik[ticker].get("tarih", "?")
        alis_neden = acik[ticker].get("neden", "")
        # Süre hesapla
        sure_str = "?"
        try:
            alis_dt = datetime.strptime(alis_tarih, "%d.%m.%Y %H:%M")
            sat_dt  = datetime.strptime(tarih, "%d.%m.%Y %H:%M")
            gun = (sat_dt - alis_dt).days
            sure_str = f"{gun} gün"
        except Exception:
            pass
        # Pozisyonu güncelle
        kalan_adet = float(acik[ticker]["adet"]) - adet
        if kalan_adet <= 0.001:
            del acik[ticker]
        else:
            acik[ticker]["adet"] = round(kalan_adet, 2)
        kitap_acik_set(chat_id, acik)
    else:
        alis_fiyat = None
        alis_tarih = "?"
        alis_neden = ""
        sure_str = "?"

    # Kâr/zarar hesapla
    if alis_fiyat is not None and alis_fiyat > 0:
        kaz_tl  = (fiyat - alis_fiyat) * adet
        kaz_pct = (fiyat - alis_fiyat) / alis_fiyat * 100
    else:
        kaz_tl  = None
        kaz_pct = None

    kaz_str = ""
    if kaz_tl is not None:
        emoji_kaz = "🟢" if kaz_tl >= 0 else "🔴"
        kaz_str = (
            f"\n{emoji_kaz} KÂR/ZARAR: {'+' if kaz_tl>=0 else ''}{kaz_tl:,.0f}₺ "
            f"({'+' if kaz_pct>=0 else ''}{kaz_pct:.1f}%)"
            f"\n📅 Süre: {sure_str}"
        )

    # Trade kitabına kaydet
    islemler = kitap_get(chat_id)
    islem_id = len(islemler) + 1
    yeni_islem = {
        "id":         islem_id,
        "yon":        "SAT",
        "ticker":     ticker,
        "fiyat":      round(fiyat, 4),
        "adet":       round(adet, 2),
        "tarih":      tarih,
        "neden":      neden,
        "alis_fiyat": round(alis_fiyat, 4) if alis_fiyat else None,
        "alis_tarih": alis_tarih,
        "alis_neden": alis_neden,
        "kaz_tl":     round(kaz_tl, 2) if kaz_tl is not None else None,
        "kaz_pct":    round(kaz_pct, 2) if kaz_pct is not None else None,
        "sure":       sure_str,
        "ai_yorum":   "",
    }
    islemler.append(yeni_islem)
    kitap_set(chat_id, islemler)

    # Kullanıcıya özet mesaj
    bot.reply_to(message,
        f"📕 *{ticker}* satış kaydedildi (#{islem_id})\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"💰 {adet:.0f} adet × {fiyat:.2f}₺ = {adet*fiyat:,.0f}₺\n"
        f"📅 {tarih}\n"
        f"📝 Sebep: {neden if neden else '(belirtilmedi)'}"
        f"{kaz_str}\n\n"
        f"🤖 AI yorumu hazırlanıyor..."
    )

    # AI yorumu ayrı thread'de
    def _ai_yorum():
        try:
            alis_bilgi = (
                f"Alış: {alis_fiyat:.2f}₺ ({alis_tarih})\n"
                f"Alış sebebi: {alis_neden if alis_neden else 'belirtilmemiş'}\n"
            ) if alis_fiyat else "Alış kaydı bulunamadı.\n"

            kaz_bilgi = (
                f"Kâr/Zarar: {'+' if kaz_tl>=0 else ''}{kaz_tl:,.0f}₺ "
                f"({'+' if kaz_pct>=0 else ''}{kaz_pct:.1f}%) — {sure_str}"
            ) if kaz_tl is not None else "Kâr/zarar hesaplanamadı"

            prompt = f"""Sen deneyimli bir BIST teknik analisti ve trade koçusun. 
Bir yatırımcının işlemini değerlendir ve öğretici geri bildirim ver.

İŞLEM:
Hisse: {ticker}
{alis_bilgi}Satış: {fiyat:.2f}₺ ({tarih})
Satış sebebi: {neden if neden else 'belirtilmemiş'}
{kaz_bilgi}

Türkçe, kısa ve net yanıt ver — koç gibi konuş, yargılamadan analiz et:

📊 İŞLEM DEĞERLENDİRMESİ: (bu trade iyi miydi, teknik okuma doğru muydu — 2 cümle)
✅ DOĞRU YAPILAN: (en güçlü karar neydi)
⚠️ GELİŞTİRİLEBİLİR: (bir sonraki benzer fırsatta ne farklı yapılabilir)
🧠 DERS: (bu işlemden çıkarılacak tek cümlelik ders)"""

            yorum = gemini_ask(prompt, max_tokens=350)
            if not yorum or yorum.startswith("⚠️"):
                yorum = groq_ask(prompt, max_tokens=350)

            # Yorumu kaydet
            islemler2 = kitap_get(chat_id)
            for isl in islemler2:
                if isl["id"] == islem_id:
                    isl["ai_yorum"] = yorum
                    break
            kitap_set(chat_id, islemler2)

            safe_send(chat_id,
                f"🤖 *{ticker} Trade Analizi* (#{islem_id})\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"{yorum}"
            )
        except Exception as e:
            debug_log("WARN", "kitap_ai_yorum", str(e)[:100])
            bot.send_message(chat_id, f"⚠️ AI yorumu yapılamadı: {str(e)[:60]}")

    threading.Thread(target=_ai_yorum, daemon=True).start()

@bot.message_handler(commands=['kitap'])
def cmd_kitap(message):
    """
    Trade kitabını göster.
    /kitap          — son 20 işlem
    /kitap THYAO    — o hissenin tüm işlemleri
    /kitap ozet     — genel istatistik
    /kitap acik     — açık pozisyonlar
    """
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("kitap", chat_id, parts[1] if len(parts) > 1 else "liste")

    islemler = kitap_get(chat_id)

    # ── /kitap ozet ──
    if len(parts) > 1 and parts[1].lower() == "ozet":
        if not islemler:
            bot.reply_to(message, "📒 Henüz kayıtlı işlem yok.\n/al ve /sat komutlarıyla işlem kaydet.")
            return

        satislar = [i for i in islemler if i["yon"] == "SAT" and i.get("kaz_pct") is not None]
        alislar  = [i for i in islemler if i["yon"] == "AL"]
        karlar   = [i for i in satislar if (i.get("kaz_tl") or 0) >= 0]
        zararlar = [i for i in satislar if (i.get("kaz_tl") or 0) < 0]

        toplam_kaz_tl = sum(i.get("kaz_tl") or 0 for i in satislar)
        ort_pct = (sum(i.get("kaz_pct") or 0 for i in satislar) / len(satislar)) if satislar else 0

        en_iyi = max(satislar, key=lambda x: x.get("kaz_pct") if x.get("kaz_pct") is not None else -999) if satislar else None
        en_kotu = min(satislar, key=lambda x: x.get("kaz_pct") if x.get("kaz_pct") is not None else 999) if satislar else None

        acik = kitap_acik_get(chat_id)

        satirlar = [
            "📒 TRADE KİTABI — ÖZET",
            "━━━━━━━━━━━━━━━━━━━",
            f"📋 Toplam işlem: {len(islemler)} ({len(alislar)} alış, {len(satislar)} satış)",
            f"🟢 Kârlı işlem:  {len(karlar)}",
            f"🔴 Zararlı işlem: {len(zararlar)}",
            f"📊 Başarı oranı:  {len(karlar)/len(satislar)*100:.0f}%" if satislar else "📊 Başarı oranı: -",
            f"💰 Net kâr/zarar: {'+' if toplam_kaz_tl>=0 else ''}{toplam_kaz_tl:,.0f}₺",
            f"📈 Ort. getiri:   {'+' if ort_pct>=0 else ''}{ort_pct:.1f}%",
            "",
        ]

        if en_iyi:
            satirlar.append(
                f"🏆 EN İYİ: {en_iyi['ticker']} "
                f"+{en_iyi.get('kaz_pct',0):.1f}% ({en_iyi.get('kaz_tl',0):+,.0f}₺)"
            )
        if en_kotu:
            satirlar.append(
                f"💔 EN KÖTÜ: {en_kotu['ticker']} "
                f"{en_kotu.get('kaz_pct',0):.1f}% ({en_kotu.get('kaz_tl',0):+,.0f}₺)"
            )
        if acik:
            satirlar.append(f"\n📂 Açık pozisyon: {', '.join(acik.keys())}")

        satirlar.append("\n/kitap — tüm işlemler  |  /kitap THYAO — tek hisse")
        bot.reply_to(message, "\n".join(satirlar))
        return

    # ── /kitap acik ──
    if len(parts) > 1 and parts[1].lower() == "acik":
        acik = kitap_acik_get(chat_id)
        if not acik:
            bot.reply_to(message, "📂 Açık pozisyon yok."); return

        satirlar = ["📂 AÇIK POZİSYONLAR", "━━━━━━━━━━━━━━━━━━━"]
        for tick, poz in acik.items():
            satirlar.append(
                f"📌 *{tick}*\n"
                f"   {poz['adet']:.0f} adet × {poz['fiyat']:.2f}₺ = {float(poz['adet'])*float(poz['fiyat']):,.0f}₺\n"
                f"   Tarih: {poz.get('tarih','?')}\n"
                f"   Sebep: {poz.get('neden','belirtilmemiş') or 'belirtilmemiş'}"
            )
        satirlar.append("\nSatmak için: /sat THYAO FIYAT ADET sebep")
        safe_send(chat_id, "\n".join(satirlar))
        return

    # ── /kitap THYAO ──
    if len(parts) > 1 and parts[1].lower() not in ("ozet", "acik"):
        ticker = parts[1].upper().replace(".IS", "")
        filtre = [i for i in islemler if i["ticker"] == ticker]
        if not filtre:
            bot.reply_to(message, f"📒 {ticker} için işlem kaydı yok."); return

        satirlar = [f"📒 {ticker} — İŞLEM GEÇMİŞİ ({len(filtre)} işlem)", "━━━━━━━━━━━━━━━━━━━"]
        for isl in filtre[-15:]:  # Son 15 işlem
            yon_emoji = "📗" if isl["yon"] == "AL" else "📕"
            kaz_bilgi = ""
            if isl["yon"] == "SAT" and isl.get("kaz_pct") is not None:
                kp = isl["kaz_pct"]
                kt = isl.get("kaz_tl") or 0
                kaz_bilgi = f" → {'+' if kp>=0 else ''}{kp:.1f}% ({'+' if kt>=0 else ''}{kt:,.0f}₺)"
            satirlar.append(
                f"{yon_emoji} #{isl['id']} {isl['yon']} — {isl['fiyat']:.2f}₺ × {isl['adet']:.0f} adet{kaz_bilgi}\n"
                f"   📅 {isl['tarih']}  📝 {isl.get('neden','') or '(sebep yok)'}"
            )
            if isl.get("ai_yorum"):
                ilk_satir = isl["ai_yorum"].split("\n")[0][:80]
                satirlar.append(f"   🤖 {ilk_satir}...")
            satirlar.append("")
        safe_send(chat_id, "\n".join(satirlar))
        return

    # ── /kitap — son 20 işlem ──
    if not islemler:
        bot.reply_to(message,
            "📒 Trade kitabı boş.\n\n"
            "Kullanım:\n"
            "/al THYAO 145.50 100 RSI kırılımı\n"
            "/sat THYAO 162.00 100 hedefe ulaştı\n\n"
            "Diğer:\n"
            "/kitap ozet — istatistikler\n"
            "/kitap acik — açık pozisyonlar\n"
            "/kitap THYAO — tek hisse geçmişi"
        )
        return

    son20 = islemler[-20:]
    satirlar = [f"📒 TRADE KİTABI — Son {len(son20)} İşlem", "━━━━━━━━━━━━━━━━━━━"]
    for isl in reversed(son20):  # En yeniden göster
        yon_emoji = "📗" if isl["yon"] == "AL" else "📕"
        kaz_bilgi = ""
        if isl["yon"] == "SAT" and isl.get("kaz_pct") is not None:
            kp = isl["kaz_pct"]
            kt = isl.get("kaz_tl", 0) or 0
            kaz_bilgi = f"  {'+' if kp>=0 else ''}{kp:.1f}% / {'+' if kt>=0 else ''}{kt:,.0f}₺"
        satirlar.append(
            f"{yon_emoji} #{isl['id']} *{isl['ticker']}* {isl['yon']} "
            f"{isl['fiyat']:.2f}₺ × {isl['adet']:.0f}{kaz_bilgi}\n"
            f"   {isl['tarih']}  {isl.get('neden','')[:50] or '(sebep yok)'}"
        )

    acik = kitap_acik_get(chat_id)
    if acik:
        satirlar.append(f"\n📂 Açık: {', '.join(acik.keys())}  →  /kitap acik")
    satirlar.append("\n/kitap ozet  |  /kitap THYAO  |  /kitap acik")
    safe_send(chat_id, "\n".join(satirlar))


# ═══════════════════════════════════════════════════════════════
# ÇOKLU STRATEJİ UYARISI — Bir hisse 3+ stratejide eşleşiyorsa
# ═══════════════════════════════════════════════════════════════

def coklu_strateji_kontrol(chat_id, tum_eslesen):
    """
    tum_eslesen: {kod: [ind, ...]} dict
    3+ stratejide eşleşen hisseler varsa özel uyarı gönder.
    """
    try:
        # Her hissenin kaç stratejiye girdiğini say
        sayac = {}
        hangi = {}
        for kod, eslesen_list in tum_eslesen.items():
            for ind in eslesen_list:
                t = ind["ticker"].replace(".IS","")
                sayac[t] = sayac.get(t, 0) + 1
                hangi.setdefault(t, []).append(TARA_STRATEJILER.get(kod, (kod,""))[0])

        # 3+ eşleşen hisseler
        super_sinyaller = {t: (sayac[t], hangi[t]) for t in sayac if sayac[t] >= 3}
        if not super_sinyaller:
            return

        satirlar = ["🚀 SÜPER SİNYAL — Çoklu Strateji Uyarısı!", "━━━━━━━━━━━━━━━━━━━",
                    "Bu hisseler 3+ stratejide aynı anda eşleşti:", ""]
        for t, (sayi, stratejiler) in sorted(super_sinyaller.items(), key=lambda x: -x[1][0]):
            satirlar.append(f"⭐ *{t}* — {sayi} strateji")
            for s in stratejiler:
                satirlar.append(f"   ✓ {s}")
            satirlar.append(f"   📊 https://tr.tradingview.com/chart/?symbol=BIST:{t}")
            satirlar.append("")

        satirlar.append("⚡ Bu hisseler için /analiz HISSE komutuyla AI yorumu al!")
        safe_send(chat_id, "\n".join(satirlar))
    except Exception as e:
        debug_log("WARN", "coklu_strateji", str(e)[:100])


# ═══════════════════════════════════════════════════════════════
# GEÇMİŞ KARŞILAŞTIRMA — Hisse geçen hafta da eşleşti mi?
# ═══════════════════════════════════════════════════════════════

def gecmis_karsilastir(chat_id, kod, yeni_eslesen):
    """
    Yeni tarama sonuçlarını geçen haftanın kayıtlarıyla karşılaştır.
    Hem geçen haftadan devam edenler hem yeni girenler bildirilir.
    """
    try:
        tr_tz = pytz.timezone("Europe/Istanbul")
        simdi = datetime.now(tr_tz)

        # Son 7 güne ait kayıtları DB'den çek (tarih formatı: dd.mm.YYYY HH:MM)
        veri = tara_load(chat_id, kod)
        if not veri:
            return  # Geçmiş kayıt yok

        tarih_str = veri.get("tarih", "")
        if not tarih_str:
            return

        try:
            gecmis_tarih = datetime.strptime(tarih_str, "%d.%m.%Y %H:%M")
            gecmis_tarih = pytz.timezone("Europe/Istanbul").localize(gecmis_tarih)
        except Exception:
            return

        gun_farki = (simdi - gecmis_tarih).days
        if gun_farki < 1 or gun_farki > 14:
            return  # Çok yeni veya çok eski

        gecmis_setl = set(veri.get("eslesen", []))
        yeni_setl   = {ind["ticker"].replace(".IS","") for ind in yeni_eslesen}

        devam_eden = yeni_setl & gecmis_setl          # geçen hafta da vardı
        yeni_giren = yeni_setl - gecmis_setl          # bu hafta yeni girdi
        cikan      = gecmis_setl - yeni_setl          # bu hafta çıktı

        if not devam_eden and not yeni_giren:
            return

        isim = TARA_STRATEJILER.get(kod, (kod, ""))[0]
        satirlar = [f"📅 GEÇMİŞ KARŞILAŞTIRMA — {isim} ({gun_farki} gün önce)", ""]

        if yeni_giren:
            satirlar.append(f"🆕 YENİ GİREN ({len(yeni_giren)} hisse):")
            satirlar.append("  " + ", ".join(sorted(yeni_giren)))
        if devam_eden:
            satirlar.append(f"🔄 DEVAM EDEN ({len(devam_eden)} hisse):")
            satirlar.append("  " + ", ".join(sorted(devam_eden)))
        if cikan:
            satirlar.append(f"⬇️ ÇIKAN ({len(cikan)} hisse):")
            satirlar.append("  " + ", ".join(sorted(cikan)))

        bot.send_message(chat_id, "\n".join(satirlar))
    except Exception as e:
        debug_log("WARN", "gecmis_karsilastir", str(e)[:100])

def tara_save(chat_id, kod, eslesen, ai_yorum=""):
    """Tara sonuçlarını DB'ye kaydet."""
    tr_tz = pytz.timezone("Europe/Istanbul")
    simdi = datetime.now(tr_tz).strftime("%d.%m.%Y %H:%M")
    veri = {
        "tarih": simdi,
        "eslesen": [e["ticker"].replace(".IS","") for e in eslesen],
        "detay": [
            {
                "ticker": e["ticker"].replace(".IS",""),
                "rsi": round(e.get("rsi") or 0, 1),
                "rel_vol": round(e.get("rel_vol") or 0, 2),
                "perf_1d": round(e.get("perf_1d") or 0, 2),
                "perf_5d": round(e.get("perf_5d") or 0, 2),
                "perf_21d": round(e.get("perf_21d") or 0, 2),
            }
            for e in eslesen
        ],
        "ai_yorum": ai_yorum,
    }
    db_set(f"tara_sonuc_{chat_id}_{kod}", veri)
    # Özet tablosunu da güncelle
    ozet = db_get(f"tara_ozet_{chat_id}", {})
    isim, _ = TARA_STRATEJILER.get(kod, (kod, ""))
    ozet[kod] = {"isim": isim, "tarih": simdi, "sayi": len(eslesen)}
    db_set(f"tara_ozet_{chat_id}", ozet)

def tara_load(chat_id, kod):
    """Tek strateji sonucunu DB'den yükle."""
    return db_get(f"tara_sonuc_{chat_id}_{kod}")

def tara_load_ozet(chat_id):
    """Tüm stratejilerin özet tablosunu yükle."""
    return db_get(f"tara_ozet_{chat_id}", {})

def pc_save(ticker, df):
    conn = db_connect()
    if not conn:
        return
    try:
        data = df.reset_index()
        data['datetime'] = data['datetime'].astype(str)
        payload = data.to_json(orient='records')
        today_tr = datetime.now(pytz.timezone('Europe/Istanbul')).date()  # FIX: UTC farkı sorunu
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO price_cache(ticker, fetched_at, data)
                VALUES(%s, %s, %s)
                ON CONFLICT(ticker) DO UPDATE
                    SET fetched_at=EXCLUDED.fetched_at, data=EXCLUDED.data
            """, (ticker, today_tr, payload))
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
        telebot.types.BotCommand("tara",         "16 strateji: /tara 1..14 | /tara A/B/C | /tara all"),
        telebot.types.BotCommand("al",           "Trade kitabı — alış: /al THYAO 145.50 100 sebep"),
        telebot.types.BotCommand("sat",          "Trade kitabı — satış + AI yorum: /sat THYAO 162.00 100 sebep"),
        telebot.types.BotCommand("kitap",        "Trade geçmişi: /kitap | /kitap ozet | /kitap acik | /kitap THYAO"),
        telebot.types.BotCommand("check",        "Tara: /check all | /check 50 | /check THYAO"),
        telebot.types.BotCommand("addall",        "BIST hisselerini ekle"),
        telebot.types.BotCommand("refreshlist",  "Yahoo'dan güncel listeyi çek"),
        telebot.types.BotCommand("add",          "Tek hisse ekle: /add THYAO"),
        telebot.types.BotCommand("remove",       "Tek hisse çıkar: /remove THYAO"),
        telebot.types.BotCommand("watchlist",    "İzleme listesini gör"),
        telebot.types.BotCommand("sinyal",       "Bugünkü sinyaller: /sinyal al | /sinyal sat"),
        telebot.types.BotCommand("analiz",       "Gemini AI analizi: /analiz THYAO"),
        telebot.types.BotCommand("kredi",        "AI kullanım ve kredi durumu"),
        telebot.types.BotCommand("haber",        "Haberler: /haber | /haber THYAO"),
        telebot.types.BotCommand("bulten",       "Bülten: /bulten sabah | /bulten aksam"),
        telebot.types.BotCommand("optimize",     "Tek hisse EMA optimize"),
        telebot.types.BotCommand("optimizeall",  "Tüm listeyi optimize et"),
        telebot.types.BotCommand("backup",       "Veriyi JSON olarak yedekle"),
        telebot.types.BotCommand("loadbackup",   "JSON yedekten geri yükle"),
        telebot.types.BotCommand("kontrolbot",   "Tüm sistemleri test et ve hata bul"),
        telebot.types.BotCommand("status",       "Bot sağlık durumu"),
        telebot.types.BotCommand("iptal",        "İşlemi durdur: /iptal tara | /iptal check"),
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

    _cached_set = _get_cached_tickers_today()
    cached  = [t for t in tickers if t in _cached_set]
    missing = [t for t in tickers if t not in _cached_set]

    bot.send_message(chat_id,
        f"🔍 *{total} hisse taranacak*\n"
        f"💾 Cache: {len(cached)} hazır | 📡 İndirilecek: {len(missing)}\n"
        f"{'⏱ Tahmini: ~'+str(int(len(missing)*TD_DELAY//60))+' dk' if missing else '⚡ Anında basliyor!'}\n"
        f"🚫 İptal için: /iptal check"
    )

    fetched = 0
    for i, ticker in enumerate(missing):
        if is_cancelled(chat_id, "check"):
            bot.send_message(chat_id, "İndirme iptal edildi."); return
        df_d, df_w = get_data(ticker)
        if (isinstance(df_d, pd.DataFrame) and not df_d.empty) or \
           (isinstance(df_w, pd.DataFrame) and not df_w.empty):
            fetched += 1
        if (i+1) % 20 == 0:
            bot.send_message(chat_id, f"📥 İndiriliyor: {i+1}/{len(missing)} ({fetched} başarılı)")
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
                today_str = datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%d %b %Y')

                # ── Başlık ──
                vol_tag = " 🔥 Yüksek Vol" if vol > 2 else ""
                header = f"{'🔥' if vol>2 else '📌'} *{ticker}*{vol_tag} | {today_str}"

                # ── Sinyallere hacim/momentum onayı ekle ──
                buy_sigs  = []
                sell_sigs = []
                other_sigs = []

                for sig in signals:
                    is_buy  = any(k in sig for k in ["AL","YUKARI","POZİTİF"])
                    is_sell = any(k in sig for k in ["SAT","AŞAĞI","NEGATİF"])

                    # Onay satırını belirle
                    onay = ""
                    if vm and is_buy:
                        if vm.get("confirm_buy"):
                            onay = "│  ✅ Hacim & Momentum Destekliyor"
                        else:
                            warns = []
                            if vm.get("vol_ratio") is not None and vm["vol_ratio"] < 0.8:
                                warns.append("hacim düşük")
                            if vm.get("buy_pressure") is not None and vm["buy_pressure"] < 0.5:
                                warns.append("satım baskısı var")
                            if vm.get("mom_fast") is not None and vm["mom_fast"] < 0:
                                warns.append(f"momentum negatif ({vm['mom_fast']:+.1f}%)")
                            if vm.get("obv_trend") == "ASAGI":
                                warns.append("OBV aşağı")
                            onay = f"│  ⚠️ Zayıf: {', '.join(warns)}" if warns else "│  🔶 Hacim/Momentum nötr"
                    elif vm and is_sell:
                        if vm.get("confirm_sell"):
                            onay = "│  ✅ Hacim & Momentum Destekliyor"
                        else:
                            warns = []
                            if vm.get("vol_ratio") is not None and vm["vol_ratio"] < 0.8:
                                warns.append("hacim düşük")
                            if vm.get("buy_pressure") is not None and vm["buy_pressure"] > 0.5:
                                warns.append("alım baskısı hâlâ var")
                            if vm.get("mom_fast") is not None and vm["mom_fast"] > 0:
                                warns.append(f"momentum pozitif ({vm['mom_fast']:+.1f}%)")
                            if vm.get("obv_trend") == "YUKARI":
                                warns.append("OBV yukarı")
                            onay = f"│  ⚠️ Zayıf: {', '.join(warns)}" if warns else "│  🔶 Hacim/Momentum nötr"

                    entry = (sig, onay)
                    if is_buy:
                        buy_sigs.append(entry)
                    elif is_sell:
                        sell_sigs.append(entry)
                    else:
                        other_sigs.append(entry)

                # ── Mesaj bloklarını oluştur ──
                parts_msg = [header, ""]

                # Analiz (RSI)
                if rsi_lines:
                    parts_msg.append("📊 *ANALİZ*")
                    for i, r in enumerate(rsi_lines):
                        prefix = "└" if i == len(rsi_lines)-1 else "├"
                        parts_msg.append(f"{prefix} {r}")
                    parts_msg.append("")

                # AL sinyalleri
                if buy_sigs:
                    parts_msg.append("🟢 *AL SİNYALLERİ*")
                    for i, (sig, onay) in enumerate(buy_sigs):
                        prefix = "└" if (i == len(buy_sigs)-1 and not onay) else "├"
                        parts_msg.append(f"{prefix} {sig}")
                        if onay:
                            parts_msg.append(onay)
                    parts_msg.append("")

                # SAT sinyalleri
                if sell_sigs:
                    parts_msg.append("🔴 *SAT SİNYALLERİ*")
                    for i, (sig, onay) in enumerate(sell_sigs):
                        prefix = "└" if (i == len(sell_sigs)-1 and not onay) else "├"
                        parts_msg.append(f"{prefix} {sig}")
                        if onay:
                            parts_msg.append(onay)
                    parts_msg.append("")

                # Diğer (diverjans vb)
                if other_sigs:
                    parts_msg.append("🔎 *DİĞER*")
                    for i, (sig, onay) in enumerate(other_sigs):
                        prefix = "└" if i == len(other_sigs)-1 else "├"
                        parts_msg.append(f"{prefix} {sig}")
                    parts_msg.append("")

                parts_msg.append(f"📐 EMA → G:{ep_d[0]}-{ep_d[1]} | H:{ep_w[0]}-{ep_w[1]}")
                parts_msg.append(f"📈 [TradingView](https://tr.tradingview.com/chart/?symbol=BIST:{ticker})")

                final_msg = "\n".join(parts_msg)

                # Sinyal tipini belirle
                has_buy  = bool(buy_sigs)
                has_sell = bool(sell_sigs)
                sig_type = "KARISIK" if (has_buy and has_sell) else ("AL" if has_buy else ("SAT" if has_sell else "DIGER"))

                # DB'ye kaydet (/sinyal al/sat için)
                try:
                    today_key = datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%Y-%m-%d')
                    db_set(f"sinyal:{today_key}:{ticker}", {"msg": final_msg, "type": sig_type, "chat_id": chat_id})
                except Exception:
                    pass

                messages.append(final_msg)

        except Exception as e:
            print(f"Scan hata {ticker}: {e}")
            continue

    # Her sinyali ayrı mesaj olarak gönder
    if messages:
        bot.send_message(chat_id, f"🔔 *{len(messages)} sinyal bulundu!* Ayrıntılar geliyor...", parse_mode='Markdown')
        for msg in messages:
            if is_cancelled(chat_id, "check"):
                break
            try:
                bot.send_message(chat_id, msg, parse_mode='Markdown')
                time.sleep(0.3)
            except Exception as e:
                print(f"Mesaj gönderme hatası: {e}")
        bot.send_message(chat_id, f"✅ Tarama tamamlandı. {total} hisse tarandı, {len(messages)} sinyal.")
    else:
        bot.send_message(chat_id,
            f"✅ Tarama bitti. {total} hisse tarandı ({no_data} veri yok).\n"
            f"📭 Sinyal bulunamadı.")

    # Bellek cache setini sıfırla — /tara artık güncel DB'yi görsün
    _invalidate_cache_set()


def _db_cached_today(ticker):
    """Tek hisse cache kontrolü — _get_cached_tickers_today kullan, bu yavaş."""
    return ticker in _get_cached_tickers_today()

_cached_today_set = None
_cached_today_date = None

def _get_cached_tickers_today():
    """Bugün cache'de olan TÜM tickerları tek sorguda çek — sonucu bellekte tut."""
    global _cached_today_set, _cached_today_date
    today = datetime.now(pytz.timezone('Europe/Istanbul')).date()
    if _cached_today_set is not None and _cached_today_date == today:
        return _cached_today_set
    if not DATABASE_URL:
        return set()
    conn = db_connect()
    if not conn:
        return set()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT REPLACE(ticker, 'd:', '') FROM price_cache WHERE fetched_at=%s AND ticker LIKE 'd:%'",
                (today,)
            )
            rows = cur.fetchall()
        _cached_today_set = {r[0] for r in rows}
        _cached_today_date = today
        return _cached_today_set
    except Exception as e:
        print(f"_get_cached_tickers_today DB hata: {e}")
        return set()
    finally:
        conn.close()

def _invalidate_cache_set():
    """Yeni veri indirince cache setini sıfırla."""
    global _cached_today_set
    _cached_today_set = None


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
    metin = (
        "📊 *BIST Teknik Analiz Botu*\n\n"
        "🔍 *── Strateji Tarama ──*\n"
        "/tara 1 — Tek strateji tara\n"
        "/tara 1,2,A — Birden fazla strateji\n"
        "/tara all — Tüm 17 stratejiyi tara ⭐\n"
        "/tarasonuc — Son tarama özetini gör\n"
        "/tarasonuc 2 — Strateji 2 detayı + AI yorum\n\n"
        "📈 *── EMA/RSI Tarama ──*\n"
        "/check all — Tüm listeyi tara ⭐\n"
        "/check THYAO — Tek hisse analiz\n"
        "/check 50 — Rastgele 50 hisse\n\n"
        "📡 *── Sinyaller ──*\n"
        "/sinyal al — Bugünkü AL sinyalleri 🟢\n"
        "/sinyal sat — Bugünkü SAT sinyalleri 🔴\n\n"
        "🤖 *── AI Analiz & Haberler ──*\n"
        "/analiz THYAO — Gemini: yorum + destek/direnç\n"
        "/haber — Sinyal hisselerinin haberleri 📰\n"
        "/haber THYAO — Tek hisse haberleri\n"
        "/bulten sabah — Sabah piyasa bülteni 🌅\n"
        "/bulten aksam — Akşam kapanış bülteni 🌆\n\n"
        "⚡ *── Sıralı Komut ──*\n"
        "/sira cachesil|check all|tara all\n\n"
        "📋 *── Liste Yönetimi ──*\n"
        "/addall — BIST hisselerini ekle (527 hisse)\n"
        "/refreshlist — Yahoo'dan güncel listeyi çek ⭐\n"
        "/add THYAO — Tek hisse ekle\n"
        "/remove THYAO — Tek hisse çıkar\n"
        "/watchlist — İzleme listeni gör\n\n"
        "🔧 *── Optimize ──*\n"
        "/optimize THYAO — Tek hisse EMA optimize\n"
        "/optimizeall — Tüm listeyi optimize et\n\n"
        "🛠 *── Sistem ──*\n"
        "/kontrolbot — Sistem sağlık testi\n"
        "/status — Bot özet durumu\n"
        "/kredi — AI kota durumu (Gemini/Groq)\n"
        "/cachesil — Fiyat önbelleğini temizle\n"
        "/resetgemini — Gemini kota bayraklarını sıfırla\n\n"
        "📒 *── Trade Kitabı ──*\n"
        "/al THYAO 145.50 100 RSI kirilimi — Alis kaydet\n"
        "/sat THYAO 162.00 100 hedefe ulasti — Satis + AI yorum\n"
        "/kitap — Son islemler\n"
        "/kitap ozet — Istatistik ve basari orani\n"
        "/kitap acik — Acik pozisyonlar\n"
        "/kitap THYAO — Tek hisse gecmisi\n\n"
        "🚫 *── İptal ──*\n"
        "/iptal tara — Strateji taramayı durdur\n"
        "/iptal check — EMA taramayı durdur\n"
        "/iptal sira — Sıralı işlemi durdur\n"
        "/iptal optimize — Optimize durdur\n\n"
        "💾 *── Yedek ──*\n"
        "/backup — Veriyi JSON olarak yedekle 💾\n"
        "/loadbackup — JSON yedekten geri yükle 📂\n"
    )
    bot.reply_to(message, metin, parse_mode="Markdown")


# ── /cachesil komutu ─────────────────────────────────────────
@bot.message_handler(commands=['cachesil'])
def cmd_cachesil(message):
    chat_id = str(message.chat.id)
    conn = db_connect()
    if not conn:
        bot.reply_to(message, "❌ DB bağlantısı kurulamadı."); return
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM price_cache")
            silinen = cur.rowcount
        conn.commit()
        _invalidate_cache_set()
        bot.reply_to(message,
            f"✅ Cache temizlendi — {silinen} kayıt silindi.\n"
            f"Bir sonraki /check all veya /tara yeniden indirecek.")
    except Exception as e:
        bot.reply_to(message, f"❌ Hata: {e}")
    finally:
        conn.close()


# ── /sira komutu ─────────────────────────────────────────────
# Kullanım:
#   /sira check all | tara all
#   /sira cachesil | check all | tara all | tarasonuc
# Komutlar | ile ayrılır, sırayla çalıştırılır.
# Her komut bir önceki bitmeden başlamaz.

_sira_running = {}  # chat_id → True/False

SIRA_KOMUTLAR = {
    "check all":    lambda cid: scan_all_stocks(cid),
    "tara all":     lambda cid: _tara_all(cid),
    "cachesil":     lambda cid: _cachesil_sync(cid),
    "optimizeall":  lambda cid: _run_optimizeall(cid),
}

def _cachesil_sync(chat_id):
    conn = db_connect()
    if not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM price_cache")
            silinen = cur.rowcount
        conn.commit()
        _invalidate_cache_set()
        bot.send_message(chat_id, f"✅ Cache temizlendi — {silinen} kayıt silindi.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ cachesil hata: {e}")
    finally:
        conn.close()

def _run_sira(chat_id, komutlar):
    _sira_running[chat_id] = True
    toplam = len(komutlar)
    bot.send_message(chat_id,
        f"⚙️ SIRA BAŞLADI — {toplam} komut\n"
        f"{chr(10).join([f'  {i+1}. {k}' for i,k in enumerate(komutlar)])}")
    try:
        for i, komut in enumerate(komutlar):
            if not _sira_running.get(chat_id, False):
                bot.send_message(chat_id, f"🚫 Sıra iptal edildi. ({i}/{toplam} tamamlandı)"); return
            fn = SIRA_KOMUTLAR.get(komut)
            if not fn:
                bot.send_message(chat_id, f"⚠️ Bilinmeyen komut: '{komut}' — atlanıyor"); continue
            bot.send_message(chat_id, f"▶️ [{i+1}/{toplam}] {komut} başlıyor...")
            try:
                fn(chat_id)
                bot.send_message(chat_id, f"✅ [{i+1}/{toplam}] {komut} tamamlandı.")
            except Exception as e:
                bot.send_message(chat_id, f"❌ [{i+1}/{toplam}] {komut} hata: {str(e)[:80]}")
    finally:
        _sira_running[chat_id] = False
    bot.send_message(chat_id, f"🏁 Tüm sıra tamamlandı!")

@bot.message_handler(commands=['sira'])
def cmd_sira(message):
    chat_id = str(message.chat.id)
    metin = message.text.strip()
    # /sira den sonrasını al
    after = metin[len("/sira"):].strip()

    if not after:
        bot.reply_to(message,
            "⚙️ SIRA KOMUTU\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Kullanım: /sira komut1 | komut2 | komut3\n\n"
            "Mevcut komutlar:\n"
            "  check all   — tüm hisseleri tara\n"
            "  tara all    — 16 strateji taraması\n"
            "  cachesil    — cache temizle\n"
            "  optimizeall — EMA optimizasyon\n\n"
            "Örnek:\n"
            "  /sira cachesil | check all | tara all\n"
            "  /sira check all | tara all\n\n"
            "İptal: /iptal sira"); return

    if _sira_running.get(chat_id, False):
        bot.reply_to(message, "⚠️ Zaten bir sıra çalışıyor.\n/iptal sira ile durdur."); return

    # | ile böl, temizle
    komutlar = [k.strip().lower() for k in after.split("|") if k.strip()]
    gecersiz = [k for k in komutlar if k not in SIRA_KOMUTLAR]
    if gecersiz:
        bot.reply_to(message,
            f"❌ Bilinmeyen komut(lar): {', '.join(gecersiz)}\n"
            f"Geçerli komutlar: {', '.join(SIRA_KOMUTLAR.keys())}"); return

    call_log("sira", chat_id, " | ".join(komutlar))
    threading.Thread(target=_run_sira, args=(chat_id, komutlar), daemon=True).start()

@bot.message_handler(commands=['iptal'])
def iptal(message):
    chat_id = str(message.chat.id)
    parts   = message.text.strip().split()
    op      = parts[1].lower() if len(parts) > 1 else "check"
    if op == "sira":
        _sira_running[chat_id] = False
        bot.reply_to(message, "🚫 Sıra iptal edildi.")
    else:
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


# ═══════════════════════════════════════════════════════════════
# /tara SİSTEMİ — 13 Profesyonel Strateji
# ═══════════════════════════════════════════════════════════════

def calc_sma(series, length):
    return series.rolling(window=length).mean()

def calc_macd(series, fast=12, slow=26, signal=9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line

def calc_adx(df, period=14):
    """ADX hesapla."""
    high = df["High"]; low = df["Low"]; close = df["Close"]
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low  - close.shift()).abs()
    tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()

    up   = high - high.shift()
    down = low.shift() - low
    plus_dm  = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)

    plus_di  = 100 * plus_dm.rolling(period).mean()  / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.rolling(period).mean() / atr.replace(0, np.nan)

    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan))
    adx = dx.rolling(period).mean()
    return adx

def calc_bollinger_width(series, period=20, std_mult=2):
    """Bollinger Band genişliği (düşük = sıkışma)."""
    sma   = series.rolling(period).mean()
    std   = series.rolling(period).std()
    upper = sma + std_mult * std
    lower = sma - std_mult * std
    width = (upper - lower) / sma.replace(0, np.nan) * 100
    return width

def perf_pct(series, days):
    """Son N iş günü performans %."""
    if len(series) < days + 1:
        return None
    base = series.iloc[-(days+1)]
    if base == 0 or pd.isna(base):
        return None
    result = (series.iloc[-1] - base) / base * 100
    return None if pd.isna(result) else float(result)


# ═══════════════════════════════════════════════════════════════
# SPADE HUNTER — Pine Script'ten Python'a tam çeviri
# ═══════════════════════════════════════════════════════════════

# Parametreler (Pine Script default'ları)
SPADE_BUY_LIMIT  = 0
SPADE_VOL_MULT   = 1.8
SPADE_VOL_EARLY  = 1.3
SPADE_ADX_THRESH = 25
SPADE_RSI_LOWER  = 40
SPADE_RSI_UPPER  = 68
SPADE_M1         = 5   # Ana EMA
SPADE_M2         = 5   # Sinyal EMA

def spade_calc_obv(close, volume):
    """OBV hesapla — numpy vektörize (hızlı)."""
    direction = np.sign(close.diff().fillna(0))
    obv = (direction * volume).cumsum()
    return obv

def spade_calc_cmf(high, low, close, volume, period=21):
    """Chaikin Money Flow hesapla."""
    denom = (high - low).replace(0, np.nan)
    mfm = ((close - low) - (high - close)) / denom
    mfv = mfm * volume
    cmf = mfv.rolling(period).sum() / volume.rolling(period).sum().replace(0, np.nan)
    return cmf

def spade_weekly_score(df_w):
    """
    Weekly f_weekly() fonksiyonunu hesapla.
    df_w: weekly DataFrame. Son bar değerlerini döndürür.
    puan >= 4 ise True.
    """
    if df_w is None or df_w.empty or len(df_w) < 50:
        return False
    close  = df_w["Close"]
    high   = df_w["High"]
    low    = df_w["Low"]
    volume = df_w["Volume"]

    w_sma50  = calc_sma(close, 50).iloc[-1]
    w_sma200 = calc_sma(close, 200).iloc[-1] if len(close) >= 200 else None
    w_rsi    = calc_rsi(close, 14)
    w_rsi_v  = float(w_rsi.iloc[-1]) if len(w_rsi.dropna()) >= 1 else None
    w_macd_l, w_macd_s = calc_macd(close)
    w_mh     = (w_macd_l - w_macd_s).iloc[-1]
    w_cmf    = spade_calc_cmf(high, low, close, volume).iloc[-1]
    w_obv    = spade_calc_obv(close, volume)
    w_obv_ema = w_obv.ewm(span=21, adjust=False).mean()
    w_obv_v  = w_obv.iloc[-1]
    w_obv_e  = w_obv_ema.iloc[-1]
    c_price  = float(close.iloc[-1])

    k1 = (not pd.isna(w_sma50))  and c_price > float(w_sma50)
    k2 = (w_rsi_v is not None)   and 35.0 <= w_rsi_v <= 75.0
    k3 = (not pd.isna(w_mh))     and float(w_mh) > 0.0
    k4 = (not pd.isna(w_cmf))    and float(w_cmf) > 0.0
    k5 = (not pd.isna(w_obv_v))  and float(w_obv_v) > float(w_obv_e)
    k6 = (w_sma200 is not None and not pd.isna(w_sma200)) and c_price > float(w_sma200)

    puan = sum([k1, k2, k3, k4, k5, k6])
    return puan >= 4

def spade_indicators(ticker):
    """
    SpadeHunterBot indikatörünü hesapla.
    tamOnay, masterBuy ve sinyal tiplerini döndürür.
    Minimum 120 bar gerekir.
    """
    df_d, df_w = get_data(ticker)
    if df_d is None or df_d.empty or len(df_d) < 120:
        return None

    d      = df_d.copy()
    close  = d["Close"]
    high   = d["High"]
    low    = d["Low"]
    volume = d["Volume"]
    open_  = d["Open"] if "Open" in d.columns else close

    price = float(close.iloc[-1])

    # ── Temel göstergeler ──
    sma20  = calc_sma(close, 20)
    sma50  = calc_sma(close, 50)
    sma200 = calc_sma(close, 200)
    rsi14  = calc_rsi(close, 14)

    macd_line, macd_sig = calc_macd(close)
    macd_hist     = macd_line - macd_sig
    macd_hist_ris = (macd_hist > macd_hist.shift(1))        # Series
    macd_full_bull = (macd_line > macd_sig) & (macd_hist > 0) & macd_hist_ris
    macd_hist_pos  = macd_hist > 0

    vma20 = volume.rolling(20).mean()
    rvol  = volume / vma20.replace(0, np.nan)

    # Hacim: earlyVol = rvol >= 1.3 AND son 3 bar üst üste artıyor
    vol_trend3 = (volume > volume.shift(1)) & (volume.shift(1) > volume.shift(2))
    early_vol  = (rvol >= SPADE_VOL_EARLY) & vol_trend3
    high_vol   = rvol >= SPADE_VOL_MULT    # fakeBreak için

    # ADX
    adx_s      = calc_adx(d)
    strong_trend = adx_s > SPADE_ADX_THRESH

    # VWAP (20 periyot hacim ağırlıklı yaklaşım)
    hlc3       = (high + low + close) / 3
    vwap_s     = (hlc3 * volume).rolling(20).sum() / volume.rolling(20).sum().replace(0, np.nan)
    above_vwap = close > vwap_s

    # Bollinger
    bb_width   = calc_bollinger_width(close, 20, 2)
    bb_wma60   = bb_width.rolling(60).mean()
    bb_sqz     = bb_width < bb_wma60 * 0.70
    bb_tsqz    = bb_width < bb_wma60 * 0.65

    # CMF
    cmf21      = spade_calc_cmf(high, low, close, volume, 21)
    cmf_pos    = cmf21 > 0.05
    cmf_strong = cmf21 > 0.15

    # OBV
    obv_val    = spade_calc_obv(close, volume)
    obv_ema    = obv_val.ewm(span=21, adjust=False).mean()
    obv_bull   = obv_val > obv_ema

    # Performans
    perf_d = (close / close.shift(1)  - 1.0) * 100.0
    perf_w = (close / close.shift(5)  - 1.0) * 100.0
    perf_m = (close / close.shift(21) - 1.0) * 100.0

    # RSI 50 crossover (son 3 bar)
    rsi_x50 = (
        ((rsi14.shift(2) <= 50) & (rsi14.shift(1) > 50)) |
        ((rsi14.shift(1) <= 50) & (rsi14        > 50)) |
        ((rsi14.shift(1) <= 50) & (rsi14        > 50))
    )

    # ── Weekly onay ──
    weekly_ok = spade_weekly_score(df_w)

    # ── Composite skorlar (tüm seri) ──
    c1 = pd.Series(0.0, index=close.index)
    c1 += (close > sma200).fillna(False) * 12.0
    c1 += ((sma50 > 0) & ((close - sma50).abs() / sma50.replace(0,np.nan) * 100 < 5.0)).fillna(False) * 10.0
    c1 += ((rsi14 >= SPADE_RSI_LOWER) & (rsi14 <= SPADE_RSI_UPPER)).fillna(False) * 8.0
    c1 += early_vol.fillna(False) * 12.0
    c1 += bb_sqz.fillna(False) * 12.0
    c1 += ((perf_w >= 0.0) & (perf_w <= 6.0)).fillna(False) * 5.0
    c1 += (perf_m > 11.0).fillna(False) * 8.0
    c1 += strong_trend.fillna(False) * 8.0
    c1 += above_vwap.fillna(False) * 8.0
    c1 += (close > open_).fillna(False) * 5.0
    c1 += macd_hist_ris.fillna(False) * 5.0
    c1 += cmf_pos.fillna(False) * 4.0
    c1 += obv_bull.fillna(False) * 3.0
    n1 = c1.clip(upper=100.0)

    c2 = pd.Series(0.0, index=close.index)
    c2 += (close > sma20).fillna(False) * 8.0
    c2 += (close > sma50).fillna(False) * 8.0
    c2 += ((rsi14 >= 50.0) & (rsi14 <= 74.0)).fillna(False) * 12.0
    c2 += macd_full_bull.fillna(False) * 16.0
    c2 += early_vol.fillna(False) * 12.0
    c2 += (perf_d > 2.0).fillna(False) * 8.0
    c2 += (perf_w > 5.0).fillna(False) * 8.0
    c2 += (perf_m < 11.0).fillna(False) * 3.0
    c2 += strong_trend.fillna(False) * 8.0
    c2 += above_vwap.fillna(False) * 8.0
    c2 += rsi_x50.fillna(False) * 5.0
    c2 += (cmf21 > 0.1).fillna(False) * 4.0
    n2 = c2.clip(upper=100.0)

    c3 = pd.Series(0.0, index=close.index)
    c3 += (close > sma50).fillna(False) * 15.0
    c3 += ((rsi14 >= 44.0) & (rsi14 <= 62.0)).fillna(False) * 15.0
    c3 += (rvol > 1.9).fillna(False) * 18.0
    c3 += ((perf_w >= 0.0) & (perf_w <= 7.0)).fillna(False) * 8.0
    c3 += ((perf_m >= 6.0) & (perf_m <= 22.0)).fillna(False) * 8.0
    c3 += strong_trend.fillna(False) * 10.0
    c3 += above_vwap.fillna(False) * 10.0
    c3 += macd_hist_pos.fillna(False) * 8.0
    c3 += cmf_strong.fillna(False) * 8.0
    n3 = c3.clip(upper=100.0)

    c4 = pd.Series(0.0, index=close.index)
    c4 += ((sma50 > 0) & ((close - sma50).abs() / sma50.replace(0,np.nan) * 100 < 5.0)).fillna(False) * 18.0
    c4 += ((rsi14 >= 38.0) & (rsi14 <= 56.0)).fillna(False) * 18.0
    c4 += (perf_d.abs() <= 1.5).fillna(False) * 14.0
    c4 += (perf_w.abs() <= 4.0).fillna(False) * 14.0
    c4 += (volume < 0.7 * vma20).fillna(False) * 14.0
    c4 += bb_tsqz.fillna(False) * 14.0
    c4 += (adx_s < 20.0).fillna(False) * 8.0
    n4 = c4.clip(upper=100.0)

    # Composite
    composite = n1 * 0.30 + n2 * 0.25 + n3 * 0.25 + n4 * 0.20

    # FakeBreak
    brk_event  = (
        ((close > sma20) & (close.shift(1) <= sma20)) |
        ((close > sma50) & (close.shift(1) <= sma50))
    )
    fake_break = brk_event & (~high_vol | ~strong_trend | ~above_vwap)

    adj_comp   = composite.where(~fake_break, composite * 0.60)
    main_line  = adj_comp.ewm(span=SPADE_M1, adjust=False).mean()
    sig_line   = main_line.ewm(span=SPADE_M2, adjust=False).mean()
    hist_val   = main_line - sig_line

    # ── Son bar değerleri ──
    i = -1  # son bar

    def sv(s):  # safe float
        v = s.iloc[i]
        return None if pd.isna(v) else float(v)

    ml   = sv(main_line)
    sl   = sv(sig_line)
    hv   = sv(hist_val)
    n1v  = sv(n1)
    n2v  = sv(n2)
    n3v  = sv(n3)
    n4v  = sv(n4)
    comp = sv(composite)
    adx_v= sv(adx_s)
    rsi_v= sv(rsi14)
    rvol_v = sv(rvol)

    fb   = bool(fake_break.iloc[i])
    ev   = bool(early_vol.iloc[i])
    st   = bool(strong_trend.iloc[i])
    av   = bool(above_vwap.iloc[i])
    mhr  = bool(macd_hist_ris.iloc[i])
    mfb  = bool(macd_full_bull.iloc[i])
    cmfp = bool(cmf_pos.iloc[i])
    obvb = bool(obv_bull.iloc[i])

    if ml is None or sl is None or hv is None:
        return None

    # Crossover: mainLine 0'ı yukarı kesiyor (önceki bar <= 0, şimdi > 0)
    ml_prev = sv(main_line.shift(1))
    ml_cross_up = (ml_prev is not None) and (ml_prev <= SPADE_BUY_LIMIT) and (ml > SPADE_BUY_LIMIT)
    ml_rising   = (ml_prev is not None) and (ml > ml_prev)

    # Sinyaller
    sig_b  = (n1v is not None and n1v >= 65.0) and ml_cross_up and not fb
    sig_k  = (n2v is not None and n2v >= 65.0) and ev and st and av and mfb and not fb
    sig_kp = (n3v is not None and n3v >= 65.0) and ml_rising and bool(cmf_strong.iloc[i]) and not fb
    sig_s  = (n4v is not None and n4v >= 60.0) and bool(bb_sqz.iloc[i]) and (adx_v is not None and adx_v < 20.0)

    sig_count  = sum([sig_b, sig_k, sig_kp])
    master_buy = (sig_count >= 1 and ml > sl and hv > 0.0
                  and ml > SPADE_BUY_LIMIT and not fb and mhr)

    # Onay koşulları (t1-t8)
    t1 = weekly_ok
    t2 = not fb
    t3 = ev
    t4 = st
    t5 = cmfp
    t6 = av
    t7 = mhr
    t8 = (rsi_v is not None) and 35.0 <= rsi_v <= 75.0

    onay_count = sum([t1, t2, t3, t4, t5, t6, t7, t8])
    tam_onay   = (onay_count >= 7) and master_buy

    if not master_buy:
        return None   # Bizi ilgilendirmiyor

    # Hangi sinyaller tetiklendi
    sinyaller = []
    if sig_b:  sinyaller.append("B")
    if sig_k:  sinyaller.append("K")
    if sig_kp: sinyaller.append("KP")
    if sig_s:  sinyaller.append("S")

    return {
        "ticker":      ticker,
        "price":       price,
        "tam_onay":    tam_onay,
        "master_buy":  master_buy,
        "onay_count":  onay_count,
        "composite":   round(comp, 1) if comp else 0,
        "sinyaller":   "+".join(sinyaller) if sinyaller else "MB",
        "rsi":         round(rsi_v, 1) if rsi_v else 0,
        "adx":         round(adx_v, 1) if adx_v else 0,
        "rvol":        round(rvol_v, 2) if rvol_v else 0,
        "weekly_ok":   t1,
        "fake_break":  fb,
    }


def _tara_spade(chat_id):
    """SpadeHunter taraması — tamOnay ve masterBuy listesi."""
    tickers = wl_get(chat_id)
    if not tickers:
        bot.send_message(chat_id, "📭 Watchlist boş! /addall yaz."); return

    tr_tz = pytz.timezone("Europe/Istanbul")
    simdi = datetime.now(tr_tz).strftime("%d.%m.%Y %H:%M")

    reset_cancel_flag(chat_id, "tara")

    # Cache kontrolü
    cached_set = _get_cached_tickers_today()
    missing = [t for t in tickers if t not in cached_set]

    bot.send_message(chat_id,
        f"♠️ *SPADE HUNTER TARAMASI*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{simdi}\n"
        f"📋 {len(tickers)} hisse | "
        f"💾 {len(tickers)-len(missing)} cache | "
        f"📥 {len(missing)} indirilecek\n"
        f"🚫 İptal: /iptal tara")

    if missing:
        for i, ticker in enumerate(missing):
            if is_cancelled(chat_id, "tara"):
                bot.send_message(chat_id, "🚫 Tara iptal edildi."); return
            get_data(ticker)
            if (i+1) % 20 == 0:
                pct = int((i+1)/len(missing)*100)
                kalan_sn = int((len(missing)-(i+1)) * TD_DELAY)
                kalan_str = f"{kalan_sn//60}dk {kalan_sn%60}sn" if kalan_sn >= 60 else f"{kalan_sn}sn"
                bot.send_message(chat_id,
                    f"📥 {i+1}/{len(missing)} indirildi ({pct}%)\n"
                    f"⏳ Kalan: ~{kalan_str}")
            time.sleep(TD_DELAY)
        _invalidate_cache_set()
        bot.send_message(chat_id, "✅ Veri hazır, SpadeHunter hesaplanıyor...")
    else:
        bot.send_message(chat_id, "⚡ Cache hazır. SpadeHunter hesaplanıyor...")

    # Tüm hisseleri tara
    tam_onay_list  = []
    master_buy_list = []
    hata = 0
    baslangic = time.time()

    try:
        for i, ticker in enumerate(tickers):
            if is_cancelled(chat_id, "tara"):
                bot.send_message(chat_id, "🚫 Tara iptal edildi."); return
            try:
                sonuc = spade_indicators(ticker)
                if sonuc is None:
                    continue
                if sonuc["tam_onay"]:
                    tam_onay_list.append(sonuc)
                elif sonuc["master_buy"]:
                    master_buy_list.append(sonuc)
            except Exception as e:
                hata += 1
                debug_log("WARN", "spade/ind", f"{ticker}: {str(e)[:80]}")

            if (i+1) % 20 == 0:  # Her 20'de bir bildir
                gecen = max(1, int(time.time() - baslangic))
                kalan = int((len(tickers)-(i+1)) * (gecen/(i+1)))
                kalan_str = f"{kalan//60}dk {kalan%60}sn" if kalan >= 60 else f"{kalan}sn"
                bot.send_message(chat_id,
                    f"📊 {i+1}/{len(tickers)} işlendi | "
                    f"🏆 {len(tam_onay_list)} tam | ⚡ {len(master_buy_list)} master\n"
                    f"⏳ Kalan: ~{kalan_str}")

    except Exception as e:
        bot.send_message(chat_id, f"❌ SpadeHunter kritik hata: {str(e)[:120]}")
        debug_log("ERROR", "_tara_spade", str(e))
        return

    # Skora göre sırala
    tam_onay_list.sort(key=lambda x: x["composite"], reverse=True)
    master_buy_list.sort(key=lambda x: x["composite"], reverse=True)

    # ── TAM ONAY mesajı ──
    if tam_onay_list:
        satirlar = [f"🏆 *TAM ONAY* (7/8+ onay) — {len(tam_onay_list)} hisse", "━━━━━━━━━━━━━━━━━━━"]
        for s in tam_onay_list:
            haftalik = "✅W" if s["weekly_ok"] else "❌W"
            satirlar.append(
                f"*{s['ticker']}*  {s['price']:.2f}₺  "
                f"Skor:{s['composite']:.0f}  [{s['sinyaller']}]  "
                f"RSI:{s['rsi']:.0f}  Vol:{s['rvol']:.1f}x  {haftalik}  {s['onay_count']}/8"
            )
        bot.send_message(chat_id, "\n".join(satirlar), parse_mode='Markdown')
    else:
        bot.send_message(chat_id, "🏆 TAM ONAY: Bugün eşleşme yok.")

    # ── MASTER BUY mesajı ──
    if master_buy_list:
        satirlar = [f"⚡ *MASTER BUY* — {len(master_buy_list)} hisse", "━━━━━━━━━━━━━━━━━━━"]
        for s in master_buy_list[:20]:  # max 20
            haftalik = "✅W" if s["weekly_ok"] else "❌W"
            satirlar.append(
                f"*{s['ticker']}*  {s['price']:.2f}₺  "
                f"Skor:{s['composite']:.0f}  [{s['sinyaller']}]  "
                f"RSI:{s['rsi']:.0f}  Vol:{s['rvol']:.1f}x  {haftalik}  {s['onay_count']}/8"
            )
        if len(master_buy_list) > 20:
            satirlar.append(f"...ve {len(master_buy_list)-20} tane daha")
        bot.send_message(chat_id, "\n".join(satirlar), parse_mode='Markdown')
    else:
        bot.send_message(chat_id, "⚡ MASTER BUY: Bugün eşleşme yok.")

    toplam = len(tam_onay_list) + len(master_buy_list)
    bot.send_message(chat_id,
        f"♠️ SpadeHunter tamamlandı ({datetime.now(tr_tz).strftime('%H:%M')})\n"
        f"🏆 Tam Onay: {len(tam_onay_list)}  ⚡ Master Buy: {len(master_buy_list)}\n"
        f"📊 Toplam sinyal: {toplam}/{len(tickers)} hisse"
        + (f"\n⚠️ {hata} hisse hesaplanamadı" if hata else ""))

def tara_indicators(ticker):
    """
    Bir hisse için /tara stratejilerinde kullanılacak
    tüm göstergeleri hesapla. Dict döndürür.
    """
    df_d, _ = get_data(ticker)
    if not isinstance(df_d, pd.DataFrame) or df_d.empty or len(df_d) < 60:
        return None

    d = df_d.copy()
    close  = d["Close"]
    high   = d["High"]
    low    = d["Low"]
    volume = d["Volume"]
    open_  = d["Open"] if "Open" in d.columns else close

    # ── Fiyat ──
    price = float(close.iloc[-1])

    # ── Kapanış > Açılış ──
    _close_last = close.iloc[-1]
    _open_last  = open_.iloc[-1]
    kapanis_yukari = (not pd.isna(_close_last) and not pd.isna(_open_last) and
                      float(_close_last) > float(_open_last))

    # ── SMA ──
    sma20  = calc_sma(close, 20)
    sma50  = calc_sma(close, 50)
    sma200 = calc_sma(close, 200)
    s20  = float(sma20.iloc[-1])  if not sma20.isna().iloc[-1]  else None
    s50  = float(sma50.iloc[-1])  if not sma50.isna().iloc[-1]  else None
    s200 = float(sma200.iloc[-1]) if not sma200.isna().iloc[-1] else None

    # ── RSI ──
    rsi_s    = calc_rsi(close, 14).dropna()
    rsi      = float(rsi_s.iloc[-1])  if len(rsi_s) >= 1 else None
    rsi_prev = float(rsi_s.iloc[-2])  if len(rsi_s) >= 2 else None
    # RSI son 3 günde yukarı kesiyor mu (30 altından 30 üstüne veya sürekli artış)
    rsi_yukari_3g = (len(rsi_s) >= 3 and
                     float(rsi_s.iloc[-1]) > float(rsi_s.iloc[-2]) > float(rsi_s.iloc[-3]))

    # ── MACD + Histogram ──
    macd_line, macd_sig_line = calc_macd(close)
    macd_val      = float(macd_line.iloc[-1])      if not macd_line.isna().iloc[-1]      else None
    macd_sig_val  = float(macd_sig_line.iloc[-1])  if not macd_sig_line.isna().iloc[-1]  else None
    macd_prev     = (float(macd_line.iloc[-2])
                      if len(macd_line) >= 2 and not pd.isna(macd_line.iloc[-2])
                      else None)
    macd_sig_prev = (float(macd_sig_line.iloc[-2])
                     if len(macd_sig_line) >= 2 and not pd.isna(macd_sig_line.iloc[-2])
                     else None)

    # MACD Histogram = MACD - Sinyal
    if macd_val is not None and macd_sig_val is not None:
        hist_cur  = macd_val - macd_sig_val
        hist_prev = (macd_prev - macd_sig_prev) if (macd_prev is not None and macd_sig_prev is not None) else None
        macd_hist_pozitif  = hist_cur > 0
        macd_hist_artiyor  = hist_prev is not None and hist_cur > hist_prev
        # Histogram pozitifleşiyor = önceki negatif, şimdi pozitif
        macd_hist_pozitif_kesim = (hist_prev is not None and hist_prev <= 0 and hist_cur > 0)
    else:
        hist_cur = None
        macd_hist_pozitif = False
        macd_hist_artiyor = False
        macd_hist_pozitif_kesim = False

    # MACD yeni yukarı kesişim (son 1 günde)
    macd_fresh_cross = (macd_val is not None and macd_sig_val is not None and
                        macd_prev is not None and macd_sig_prev is not None and
                        macd_val > macd_sig_val and macd_prev <= macd_sig_prev)

    # ── ADX ──
    adx_s = calc_adx(d)
    adx   = float(adx_s.iloc[-1]) if not adx_s.isna().iloc[-1] else None

    # ── Bollinger Band Genişliği ──
    bb_width_s = calc_bollinger_width(close)
    bb_width   = float(bb_width_s.iloc[-1]) if not bb_width_s.isna().iloc[-1] else None
    # bb_width_low_20: son değer 20g ortalamasının altında
    _bb_avg20_raw = bb_width_s.rolling(20).mean().iloc[-1]
    bb_avg20 = (float(_bb_avg20_raw)
                if len(bb_width_s) > 20 and not pd.isna(_bb_avg20_raw)
                else None)
    bb_width_low = (bb_width is not None and bb_avg20 is not None and bb_width < bb_avg20)
    # bb_width_low_60: son değer 60g ortalamasının %70'inden az (Strateji 1 ve 10)
    bb_avg60_raw = bb_width_s.rolling(60).mean().iloc[-1]
    bb_avg60 = (float(bb_avg60_raw) if (len(bb_width_s) > 60 and not pd.isna(bb_avg60_raw))
                else bb_avg20)
    bb_width_low60 = (bb_width is not None and bb_avg60 is not None and bb_avg60 > 0 and
                      bb_width < bb_avg60 * 0.70)
    bb_width_low65 = (bb_width is not None and bb_avg60 is not None and bb_avg60 > 0 and
                      bb_width < bb_avg60 * 0.65)

    # ── VWAP (günlük yaklaşım: 20 günlük ağırlıklı ortalama) ──
    typical_price = (high + low + close) / 3
    vwap_num  = (typical_price * volume).rolling(20).sum()
    vwap_den  = volume.rolling(20).sum()
    vwap_s    = vwap_num / vwap_den.replace(0, float("nan"))
    vwap_raw  = vwap_s.iloc[-1]
    vwap      = float(vwap_raw) if (vwap_raw is not None and not pd.isna(vwap_raw)) else None
    price_above_vwap = (vwap is not None and price > vwap)

    # ── Hacim ──
    vol_avg20_raw = volume.rolling(20).mean().iloc[-1]
    vol_avg20 = float(vol_avg20_raw) if not pd.isna(vol_avg20_raw) else 0.0
    vol_cur   = float(volume.iloc[-1]) if not pd.isna(volume.iloc[-1]) else 0.0
    rel_vol   = vol_cur / vol_avg20 if vol_avg20 > 0 else 0

    # ── Performans ──
    perf_1d   = perf_pct(close, 1)
    perf_5d   = perf_pct(close, 5)    # ~haftalık
    perf_21d  = perf_pct(close, 21)   # ~aylık
    perf_63d  = perf_pct(close, 63)   # ~3 aylık
    perf_252d = perf_pct(close, 252)  # ~1 yıllık

    return {
        "ticker": ticker,
        "price": price,
        "kapanis_yukari": kapanis_yukari,
        "sma20": s20, "sma50": s50, "sma200": s200,
        "rsi": rsi, "rsi_prev": rsi_prev, "rsi_yukari_3g": rsi_yukari_3g,
        "macd": macd_val, "macd_sig": macd_sig_val,
        "macd_fresh_cross": macd_fresh_cross,
        "macd_hist_pozitif": macd_hist_pozitif,
        "macd_hist_artiyor": macd_hist_artiyor,
        "macd_hist_pozitif_kesim": macd_hist_pozitif_kesim,
        "adx": adx,
        "bb_width": bb_width,
        "bb_width_low": bb_width_low,
        "bb_width_low60": bb_width_low60,
        "bb_width_low65": bb_width_low65,
        "vwap": vwap, "price_above_vwap": price_above_vwap,
        "vol_avg20": vol_avg20, "vol_cur": vol_cur, "rel_vol": rel_vol,
        "perf_1d": perf_1d, "perf_5d": perf_5d,
        "perf_21d": perf_21d, "perf_63d": perf_63d, "perf_252d": perf_252d,
    }

# ── Strateji Filtreleri ──────────────────────────────────────

TARA_STRATEJILER = {
    "1": ("🧠 Smart Money Birikim",      "Patlama öncesi sessiz birikim aşaması"),
    "2": ("📈 Düşen Trend Kırılımı",     "Yeni yükseliş başlangıcı sinyali"),
    "3": ("🔄 Güçlü Dipten Dönüş",       "Destekten güçlü RSI dönüşü"),
    "4": ("🚀 Momentum Patlaması",        "Sert yükseliş — hacim + RSI + MACD uyumu"),
    "6": ("🔥 Tavan Serisi",             "Ardışık tavan yapabilecek hisseler"),
    "7": ("💎 Akümülasyon Çıkışı",       "Büyük hareket başlangıcı"),
    "8": ("🌱 Erken Trend Doğumu",       "Trend henüz başlıyor, SMA dizilimi oluştu"),
    "9": ("🏦 Kurumsal Para Girişi",     "Gizli kurumsal toplama sinyali"),
    "10": ("⚖️ Güçlü Konsolidasyon",    "Sıkışma — yakında büyük hareket"),
    "11": ("⚡ Volatilite Patlaması",    "Ani hacim + fiyat hareketi"),
    "12": ("👑 Sektör Lideri",           "Güçlü hisseler — trend liderliği"),
    "13": ("🌅 Dipten Lider Doğuşu",    "Derin düşüşten güçlü dönüş"),
    "14": ("💰 Büyük Ralli",            "+%50 potansiyel — tüm göstergeler uyumlu"),
    "A":  ("💪 Piyasadan Güçlü",        "Relative strength — endekstten üstün"),
    "B":  ("💸 Büyük Para Girişi",      "Günlük trade fırsatı"),
    "C":  ("🛡️ Endeks Düşerken Güçlü", "Düşen piyasada ayakta kalan hisseler"),
    "15": ("📊 Bilançodan Önce Hareket", "Güçlü büyüme + teknik uyum"),
}

def strateji_filtre(ind, kod):
    """
    Verilen gösterge sözlüğünü strateji koduna göre filtrele.
    True dönerse hisse bu stratejiye giriyor demektir.
    """
    p     = ind
    price = p["price"]

    def gt(val, thr):
        return val is not None and val > thr
    def lt(val, thr):
        return val is not None and val < thr
    def between(val, lo, hi):
        return val is not None and lo <= val <= hi
    def near(val, ref, pct):
        return val is not None and ref is not None and abs(val - ref) / ref * 100 <= pct

    if kod == "1":   # Smart Money ve Patlama Öncesi Birikim
        return (gt(price, p["sma200"] or 0) and
                near(price, p["sma50"], 5) and
                between(p["rsi"], 40, 68) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0) * 1.8) and
                gt(p["rel_vol"], 1.8) and
                p["bb_width_low60"] and
                between(p["perf_5d"], 0, 6) and
                gt(p["perf_21d"], 11) and
                gt(p["adx"], 25) and
                p["price_above_vwap"] and
                p["kapanis_yukari"] and
                p["macd_hist_artiyor"])

    elif kod == "2": # Düşen Trend Kırılımı ve Yeni Yükseliş Başlangıcı
        return (gt(price, p["sma20"] or 0) and
                gt(price, p["sma50"] or 0) and
                between(p["rsi"], 50, 74) and
                gt(p["macd"], p["macd_sig"] if p["macd_sig"] is not None else -999) and
                p["macd_hist_pozitif"] and
                p["macd_hist_artiyor"] and
                gt(p["vol_cur"], (p["vol_avg20"] or 0) * 1.8) and
                gt(p["rel_vol"], 1.8) and
                gt(p["perf_1d"], 2) and
                gt(p["perf_5d"], 5) and
                lt(p["perf_21d"], 11) and
                gt(p["adx"], 25) and
                p["price_above_vwap"] and
                p["rsi_yukari_3g"])

    elif kod == "3": # Güçlü Dip ve Destekten Dönüş
        rsi3_yukari = (p["rsi"] is not None and p["rsi_prev"] is not None and
                       p["rsi"] > p["rsi_prev"])
        return (lt(p["rsi"], 45) and
                rsi3_yukari and
                gt(price, p["sma20"] or 0) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0) * 1.8) and
                gt(p["rel_vol"], 1.8) and
                gt(p["perf_1d"], 2) and
                lt(p["perf_5d"], 0) and
                lt(p["perf_21d"], 0) and
                gt(p["adx"], 25) and
                p["price_above_vwap"] and
                p["macd_hist_pozitif_kesim"])

    elif kod == "4": # Momentum Patlaması ve Sert Yükseliş
        return (gt(price, p["sma50"] or 0) and
                gt(price, p["sma200"] or 0) and
                between(p["rsi"], 56, 74) and
                gt(p["macd"], p["macd_sig"] if p["macd_sig"] is not None else -999) and
                p["macd_hist_pozitif"] and
                p["macd_hist_artiyor"] and
                gt(p["vol_cur"], (p["vol_avg20"] or 0) * 2.0) and
                gt(p["rel_vol"], 1.9) and
                gt(p["perf_5d"], 8) and
                gt(p["perf_21d"], 13) and
                gt(p["adx"], 28))

    elif kod == "6": # Tavan Serisi Başlatabilecek Hisseler
        return (gt(price, p["sma20"] or 0) and
                gt(price, p["sma50"] or 0) and
                between(p["rsi"], 64, 82) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0) * 2.2) and
                gt(p["rel_vol"], 2.2) and
                gt(p["perf_1d"], 5) and
                gt(p["perf_5d"], 16) and
                gt(p["perf_21d"], 26) and
                gt(p["adx"], 32) and
                p["macd_hist_artiyor"] and
                p["price_above_vwap"])

    elif kod == "7": # Akümülasyon Çıkışı ve Büyük Hareket Başlangıcı
        return (gt(price, p["sma50"] or 0) and
                gt(price, p["sma200"] or 0) and
                between(p["rsi"], 54, 70) and
                p["macd_fresh_cross"] and
                p["macd_hist_pozitif"] and
                p["macd_hist_artiyor"] and
                gt(p["vol_cur"], (p["vol_avg20"] or 0) * 1.9) and
                gt(p["rel_vol"], 1.8) and
                gt(p["perf_1d"], 3) and
                gt(p["perf_5d"], 9) and
                between(p["perf_21d"], 0, 11) and
                gt(p["adx"], 25) and
                p["price_above_vwap"])

    elif kod == "8": # Erken Trend Doğumu
        return (gt(price, p["sma20"] or 0) and
                gt(price, p["sma50"] or 0) and
                gt(p["sma20"] or 0, p["sma50"] or 0) and
                between(p["rsi"], 48, 66) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0) * 1.8) and
                gt(p["rel_vol"], 1.8) and
                between(p["perf_5d"], 0, 7) and
                between(p["perf_21d"], 0, 13) and
                gt(p["adx"], 25) and
                p["price_above_vwap"])

    elif kod == "9": # Kurumsal Para Girişi (Gizli Toplama)
        return (gt(price, p["sma50"] or 0) and
                between(p["rsi"], 44, 62) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0) * 1.9) and
                gt(p["rel_vol"], 1.9) and
                between(p["perf_5d"], 0, 7) and
                between(p["perf_21d"], 6, 22) and
                gt(p["adx"], 25) and
                p["price_above_vwap"] and
                p["macd_hist_pozitif"])

    elif kod == "10": # Güçlü Konsolidasyon ve Sıkışma
        return (near(price, p["sma50"], 5) and
                between(p["rsi"], 38, 56) and
                between(p["perf_1d"], -1.5, 1.5) and
                between(p["perf_5d"], -4, 4) and
                lt(p["vol_cur"], (p["vol_avg20"] or 0) * 0.7) and
                p["bb_width_low65"] and
                lt(p["adx"], 20))

    elif kod == "11": # Volatilite Patlaması
        return (gt(price, p["sma20"] or 0) and
                gt(p["rsi"], 56) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0) * 2.2) and
                gt(p["rel_vol"], 2.2) and
                gt(p["perf_1d"], 3) and
                gt(p["perf_5d"], 9) and
                gt(p["adx"], 28) and
                p["price_above_vwap"])

    elif kod == "12": # Sektör Lideri ve Güçlü Hisseler
        return (gt(price, p["sma50"] or 0) and
                gt(price, p["sma200"] or 0) and
                gt(p["rsi"], 59) and
                gt(p["perf_5d"], 11) and
                gt(p["perf_21d"], 19) and
                gt(p["perf_63d"], 32) and
                gt(p["adx"], 26))

    elif kod == "13": # Dipten Lider Doğuşu
        return (lt(p["perf_252d"], -28) and
                lt(p["rsi"], 43) and
                gt(price, p["sma20"] or 0) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0) * 1.8) and
                gt(p["perf_1d"], 2) and
                gt(p["rel_vol"], 1.8) and
                gt(p["adx"], 22))

    elif kod == "14": # Büyük Ralli ve %50+ Potansiyel
        return (gt(price, p["sma50"] or 0) and
                gt(price, p["sma200"] or 0) and
                between(p["rsi"], 61, 78) and
                gt(p["adx"], 32) and
                gt(p["macd"], p["macd_sig"] if p["macd_sig"] is not None else -999) and
                p["macd_hist_artiyor"] and
                gt(p["vol_cur"], (p["vol_avg20"] or 0) * 2.2) and
                gt(p["perf_5d"], 13) and
                gt(p["perf_21d"], 21) and
                p["price_above_vwap"])

    elif kod == "15": # Bilançodan Önce Hareket Eden Hisseler
        # Yahoo Finance temel veri proxy — teknik filtreler
        return (gt(price, p["sma50"] or 0) and
                gt(p["rsi"], 56) and
                between(p["perf_5d"], 6, 30) and
                gt(p["vol_cur"], (p["vol_avg20"] or 0) * 1.8) and
                gt(p["adx"], 25) and
                p["macd_hist_pozitif"] and
                p["price_above_vwap"])

    elif kod == "A": # Piyasadan Güçlü (Relative Strength)
        return (gt(price, p["sma50"] or 0) and
                gt(price, p["sma200"] or 0) and
                gt(p["perf_5d"], 5) and
                gt(p["perf_21d"], 10) and
                gt(p["rsi"], 50))

    elif kod == "B": # Büyük Para Girişi
        return (gt(p["vol_cur"], (p["vol_avg20"] or 0) * 1.5) and
                gt(p["rel_vol"], 1.5) and
                gt(p["perf_1d"], 1) and
                gt(p["rsi"], 45) and
                gt(price, p["sma20"] or 0))

    elif kod == "C": # Endeks Düşerken Güçlü
        return (gt(p["perf_1d"], 0) and
                gt(p["perf_5d"], 3) and
                gt(p["rsi"], 50) and
                gt(price, p["sma50"] or 0))

    return False


def tara_single_strategy(chat_id, tickers, kod):
    """Tek bir strateji için tüm listeyi tara — cache kullan, iptal destekle."""
    isim, aciklama = TARA_STRATEJILER.get(kod, (kod, ""))
    eslesen = []
    hata = 0

    # Taze DB sorgusu
    cached_set = _get_cached_tickers_today()
    missing = [t for t in tickers if t not in cached_set]
    cached  = [t for t in tickers if t in cached_set]

    # Kısmi veya tam eksik — indirerek devam et
    if missing:
        bot.send_message(chat_id,
            f"📥 {len(cached)} hazır | {len(missing)} indiriliyor (~{max(1,len(missing)*TD_DELAY//60):.0f} dk)\n"
            f"İptal: /iptal tara")
        for i, ticker in enumerate(missing):
            if is_cancelled(chat_id, "tara"):
                bot.send_message(chat_id, "🚫 Tara iptal edildi."); return None
            get_data(ticker)
            if (i+1) % 20 == 0:  # FIX: 25→20, daha sık bildirim
                pct = int((i+1)/len(missing)*100)
                kalan_sn = int((len(missing) - (i+1)) * TD_DELAY)
                kalan_str = f"{kalan_sn//60}dk {kalan_sn%60}sn" if kalan_sn >= 60 else f"{kalan_sn}sn"
                bot.send_message(chat_id,
                    f"📥 {i+1}/{len(missing)} indirildi ({pct}%)\n"
                    f"⏳ Kalan tahmini: ~{kalan_str}")
            time.sleep(TD_DELAY)
        _invalidate_cache_set()  # İndirme bittikten sonra bir kez sıfırla
        bot.send_message(chat_id, "✅ Veri hazır, filtreler uygulanıyor...")

    # Tüm hisseleri filtrele
    filtre_baslangic = time.time()
    for i, ticker in enumerate(tickers):
        if is_cancelled(chat_id, "tara"):
            bot.send_message(chat_id, f"🚫 Tara iptal edildi. ({len(eslesen)} eşleşme)"); return None
        try:
            ind = tara_indicators(ticker)
            if ind is None:
                hata += 1
                continue
            if strateji_filtre(ind, kod):
                eslesen.append(ind)
        except Exception as e:
            hata += 1
            debug_log("WARN", f"tara/{kod}", f"{ticker}: {str(e)[:60]}")
        # FIX: Her 50 hissede bir progress + kalan süre tahmini
        if (i+1) % 50 == 0:
            gecen = max(1, int(time.time() - filtre_baslangic))
            kalan_tahmini = int((len(tickers) - (i+1)) * (gecen / (i+1)))
            kalan_str = f"{kalan_tahmini//60}dk {kalan_tahmini%60}sn" if kalan_tahmini >= 60 else f"{kalan_tahmini}sn"
            bot.send_message(chat_id,
                f"📊 Analiz: {i+1}/{len(tickers)} ({len(eslesen)} eşleşme)\n"
                f"⏳ Kalan: ~{kalan_str}")

    return eslesen

def tara_format_results(kod, eslesen, ai_yorum=True):
    """Tarama sonuçlarını formatlı mesaj olarak döndür."""
    isim, aciklama = TARA_STRATEJILER.get(kod, (kod, ""))
    satirlar = [
        f"{isim}",
        f"{aciklama}",
        f"Eslesme: {len(eslesen)} hisse",
        "━━━━━━━━━━━━━━━━━━━",
    ]

    if not eslesen:
        satirlar.append("Bu kriterle esleyen hisse bulunamadi.")
        return "\n".join(satirlar), []

    # RSI'ya göre sırala (en güçlü önce)
    eslesen_sorted = sorted(eslesen, key=lambda x: x.get("rsi") or 0, reverse=True)

    for ind in eslesen_sorted[:20]:  # Max 20 hisse göster
        t    = ind["ticker"].replace(".IS","")
        rsi  = ind["rsi"]
        rv   = ind["rel_vol"]
        p1d  = ind["perf_1d"]
        p5d  = ind["perf_5d"]
        p21d = ind["perf_21d"]
        fiyat = ind.get("price")
        tv_link = f"https://tr.tradingview.com/chart/?symbol=BIST:{t}"

        if (rsi is not None and rv is not None and p1d is not None and p5d is not None and p21d is not None):
            fiyat_str = f"{fiyat:.2f}₺ | " if fiyat else ""
            satirlar.append(
                f"{t} | {fiyat_str}RSI:{rsi:.0f} | RV:{rv:.1f}x | "
                f"G:{'+' if p1d>0 else ''}{p1d:.1f}% "
                f"H:{'+' if p5d>0 else ''}{p5d:.1f}% "
                f"A:{'+' if p21d>0 else ''}{p21d:.1f}% | "
                f"📊 {tv_link}"
            )
        else:
            satirlar.append(f"{t} | 📊 {tv_link}")

    if len(eslesen) > 20:
        satirlar.append(f"... ve {len(eslesen)-20} hisse daha")

    return "\n".join(satirlar), eslesen_sorted[:5]  # Top 5'i AI yorumu için döndür

def tara_ai_yorum(kod, eslesen_top5):
    """Top 5 hisse için tek bir Gemini yorumu."""
    if not eslesen_top5:
        return None
    isim, aciklama = TARA_STRATEJILER.get(kod, (kod, ""))
    hisse_listesi = []
    for ind in eslesen_top5:
        t = ind["ticker"].replace(".IS","")
        rsi_str  = f"{ind['rsi']:.1f}"   if ind['rsi']    is not None else "?"
        p5d_str  = f"{ind['perf_5d']:.1f}" if ind['perf_5d'] is not None else "?"
        hisse_listesi.append(
            f"{t}: Fiyat={ind['price']:.2f} RSI={rsi_str} "
            f"RelVol={ind['rel_vol']:.1f} Perf5d={p5d_str}%"
        )

    prompt = f"""Sen BIST uzmanı bir teknik analistsin.

Strateji: {isim} — {aciklama}

Bu stratejiye göre taramadan çıkan en güçlü hisseler:
{chr(10).join(hisse_listesi)}

Kısa ve net Türkçe yanıt ver:
🏆 EN GÜÇLÜ: (hangi hisse neden öne çıkıyor, 2 cümle)
⚡ STRATEJİ NOTU: (bu stratejinin bugünkü piyasa koşullarında güvenilirliği)
⚠️ RİSK: (dikkat edilmesi gereken tek şey)"""

    return gemini_ask(prompt, max_tokens=300)

# ── /tara Komutu ──────────────────────────────────────────────
@bot.message_handler(commands=['tara'])
def cmd_tara(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("tara", chat_id, parts[1] if len(parts) > 1 else "menu")

    # Menü göster
    if len(parts) < 2:
        menu = [
            "📊 TARA — Strateji Seçimi",
            "━━━━━━━━━━━━━━━━━━━",
            "Kullanım: /tara [numara]",
            "",
            "🧠 /tara 1  — Smart Money Birikim",
            "📈 /tara 2  — Düşen Trend Kırılımı",
            "🔄 /tara 3  — Güçlü Dipten Dönüş",
            "🚀 /tara 4  — Momentum Patlaması",
            "🔥 /tara 6  — Tavan Serisi",
            "💎 /tara 7  — Akümülasyon Çıkışı",
            "🌱 /tara 8  — Erken Trend Doğumu",
            "🏦 /tara 9  — Kurumsal Para Girişi",
            "⚖️ /tara 10 — Güçlü Konsolidasyon",
            "⚡ /tara 11 — Volatilite Patlaması",
            "👑 /tara 12 — Sektör Lideri",
            "🌅 /tara 13 — Dipten Lider Doğuşu",
            "💰 /tara 14 — Büyük Ralli",
            "",
            "⭐ EKSTRA:",
            "💪 /tara A  — Piyasadan Güçlü",
            "💸 /tara B  — Büyük Para Girişi",
            "🛡️ /tara C  — Endeks Düşerken Güçlü",
            "📊 /tara 15 — Bilançodan Önce Hareket",
            "",
            "🔍 /tara all — Tüm 17 strateji",
            "",
            "♠️ /tara spade — SpadeHunter (Tam Onay + Master Buy)",
        ]
        bot.send_message(chat_id, "\n".join(menu))
        return

    kod = parts[1].upper()

    # all — tüm stratejiler
    if kod == "ALL":
        threading.Thread(target=_tara_all, args=(chat_id,), daemon=True).start()
        return

    # spade — SpadeHunter taraması
    if kod == "SPADE":
        threading.Thread(target=_tara_spade, args=(chat_id,), daemon=True).start()
        return

    # Geçerli strateji mi?
    kod = parts[1]  # Orijinal hali koru (A/B/C büyük, sayılar sayı)
    if kod.upper() in [k.upper() for k in TARA_STRATEJILER]:
        # Büyük harf normalizasyonu
        for k in TARA_STRATEJILER:
            if k.upper() == kod.upper():
                kod = k
                break
        threading.Thread(target=_tara_single, args=(chat_id, kod), daemon=True).start()
    else:
        bot.send_message(chat_id, f"❌ Geçersiz strateji: {kod}\n/tara yazarak listeyi gör.")

def _tara_single(chat_id, kod):
    """Tek strateji taraması — thread içinde çalışır."""
    try:
        isim, aciklama = TARA_STRATEJILER[kod]
        tickers = wl_get(chat_id)
        if not tickers:
            bot.send_message(chat_id, "Watchlist bos! /addall yaz."); return

        tr_tz = pytz.timezone("Europe/Istanbul")
        simdi = datetime.now(tr_tz).strftime("%H:%M")

        reset_cancel_flag(chat_id, "tara")
        bot.send_message(chat_id,
            f"Strateji: {isim}\n"
            f"Toplam: {len(tickers)} hisse\n"
            f"Iptal: /iptal tara")

        eslesen = tara_single_strategy(chat_id, tickers, kod)

        if eslesen is None:  # iptal edildi veya cache uyarısı
            return

        # Sonuç mesajı
        mesaj, top5 = tara_format_results(kod, eslesen)
        bot.send_message(chat_id, mesaj)

        # AI yorumu devre dışı
        yorum_txt = ""

        # DB'ye kaydet
        tara_save(chat_id, kod, eslesen, yorum_txt)

        # Geçmiş karşılaştırma
        if eslesen:
            gecmis_karsilastir(chat_id, kod, eslesen)

        simdi = datetime.now(tr_tz).strftime("%H:%M")
        bot.send_message(chat_id, f"Tarama tamamlandi ({simdi}) — {len(eslesen)} eslesme\n/tarasonuc {kod} ile tekrar goruntule")

    except Exception as e:
        debug_log("ERROR", "_tara_single", str(e)[:150])
        bot.send_message(chat_id, f"Tara hatasi: {str(e)[:100]}\n/kontrolbot ile detay goruntule")

def _tara_all(chat_id):
    """Tüm stratejileri sırayla tara — veriyi BİR KEZ indir, 17 stratejiyi filtrele."""
    tickers = wl_get(chat_id)
    if not tickers:
        bot.send_message(chat_id, "📭 Watchlist boş! /addall yaz."); return

    tr_tz = pytz.timezone("Europe/Istanbul")
    simdi = datetime.now(tr_tz).strftime("%d.%m.%Y %H:%M")
    kodlar = list(TARA_STRATEJILER.keys())

    reset_cancel_flag(chat_id, "tara")
    bot.send_message(chat_id,
        f"📊 TAM STRATEJİ TARAMASI\n"
        f"{simdi}\n"
        f"{len(tickers)} hisse x {len(kodlar)} strateji\n"
        f"🚫 İptal: /iptal tara\n"
        f"━━━━━━━━━━━━━━━━━━━")

    # ── ADIM 1: Eksik verileri BİR KEZ indir ──
    # FIX: _invalidate_cache_set() kaldırıldı — her /tara all'da cache boşaltıyordu
    cached_set = _get_cached_tickers_today()
    missing = [t for t in tickers if t not in cached_set]

    if missing:
        bot.send_message(chat_id,
            f"📥 {len(cached_set)} hazır | {len(missing)} indiriliyor "
            f"(~{max(1, len(missing)*TD_DELAY//60):.0f} dk)\n"
            f"İptal: /iptal tara")
        for i, ticker in enumerate(missing):
            if is_cancelled(chat_id, "tara"):
                bot.send_message(chat_id, "🚫 Tara iptal edildi."); return
            get_data(ticker)
            if (i+1) % 20 == 0:  # FIX: 50→20, daha sık bildirim
                pct = int((i+1)/len(missing)*100)
                kalan_sn = int((len(missing) - (i+1)) * TD_DELAY)
                kalan_str = f"{kalan_sn//60}dk {kalan_sn%60}sn" if kalan_sn >= 60 else f"{kalan_sn}sn"
                bot.send_message(chat_id,
                    f"📥 {i+1}/{len(missing)} indirildi ({pct}%)\n"
                    f"⏳ Kalan tahmini: ~{kalan_str}")
            time.sleep(TD_DELAY)
        _invalidate_cache_set()
        bot.send_message(chat_id, f"✅ {len(tickers)} hisse hazır. Göstergeler hesaplanıyor...")
    else:
        bot.send_message(chat_id, f"⚡ Cache hazır ({len(tickers)} hisse). Göstergeler hesaplanıyor...")

    # ── ADIM 2: Tüm hisseler için göstergeleri BİR KEZ hesapla ──
    if is_cancelled(chat_id, "tara"):
        bot.send_message(chat_id, "🚫 Tara iptal edildi."); return

    tum_ind = {}
    hata = 0
    adim2_baslangic = time.time()
    for i, ticker in enumerate(tickers):
        if is_cancelled(chat_id, "tara"):
            bot.send_message(chat_id, "🚫 Tara iptal edildi."); return
        try:
            ind = tara_indicators(ticker)
            if ind is not None:
                tum_ind[ticker] = ind
            else:
                hata += 1
        except Exception as e:
            hata += 1
            debug_log("WARN", "tara_all/ind", f"{ticker}: {str(e)[:60]}")
        # FIX: 100→50, progress daha sık + kalan süre tahmini
        if (i+1) % 50 == 0:
            gecen = max(1, int(time.time() - adim2_baslangic))
            kalan_tahmini = int((len(tickers) - (i+1)) * (gecen / (i+1)))
            kalan_str = f"{kalan_tahmini//60}dk {kalan_tahmini%60}sn" if kalan_tahmini >= 60 else f"{kalan_tahmini}sn"
            bot.send_message(chat_id,
                f"📊 Göstergeler: {i+1}/{len(tickers)} işlendi "
                f"({len(tum_ind)} geçerli)\n"
                f"⏳ Kalan tahmini: ~{kalan_str}")

    bot.send_message(chat_id,
        f"✅ Göstergeler hazır: {len(tum_ind)}/{len(tickers)} hisse "
        f"({hata} veri yok)\n"
        f"17 strateji filtreleniyor...")

    # ── ADIM 3: 17 stratejiyi filtrele (hızlı — sadece hesap) ──
    ozet_satirlar = [f"📊 TARAMA OZETI — {simdi}", ""]
    tum_eslesen = {}

    for i, kod in enumerate(kodlar):
        if is_cancelled(chat_id, "tara"):
            bot.send_message(chat_id, "🚫 Tara iptal edildi."); return

        isim, _ = TARA_STRATEJILER[kod]

        eslesen = []
        for ticker, ind in tum_ind.items():
            try:
                if strateji_filtre(ind, kod):
                    eslesen.append(ind)
            except Exception as e:
                debug_log("WARN", f"tara_all/filtre/{kod}", f"{ticker}: {str(e)[:60]}")

        tum_eslesen[kod] = eslesen
        ozet_satirlar.append(f"{isim}: {len(eslesen)} hisse")

        # Sonuç mesajı
        mesaj, top5 = tara_format_results(kod, eslesen, ai_yorum=False)
        bot.send_message(chat_id, mesaj)

        # DB'ye kaydet
        tara_save(chat_id, kod, eslesen)

        # Geçmiş karşılaştırma
        if eslesen:
            gecmis_karsilastir(chat_id, kod, eslesen)

        # AI yorumu devre dışı

        time.sleep(0.3)  # FIX: 0.5→0.3 flood limiti için yeterli


    # ── Genel özet ──
    en_cok = sorted(tum_eslesen.items(), key=lambda x: len(x[1]), reverse=True)[:3]
    ozet_satirlar += ["", "━━━━━━━━━━━━━━━━━━━", "EN FAZLA ESLESEN:"]
    for kod, eslesen in en_cok:
        isim, _ = TARA_STRATEJILER[kod]
        ozet_satirlar.append(f"  {isim}: {len(eslesen)} hisse")

    bot.send_message(chat_id, "\n".join(ozet_satirlar))

    # Çoklu strateji uyarısı
    coklu_strateji_kontrol(chat_id, tum_eslesen)

    bot.send_message(chat_id, "✅ Tüm stratejiler tamamlandı!")


@bot.message_handler(commands=['tarasonuc'])
def cmd_tarasonuc(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("tarasonuc", chat_id, parts[1] if len(parts) > 1 else "ozet")

    if len(parts) < 2:
        # Özet — tüm stratejilerin son tarama sonucu
        ozet = tara_load_ozet(chat_id)
        if not ozet:
            bot.reply_to(message,
                "Henüz tarama yapılmamış.\n"
                "/tara 1 veya /tara all ile tarama başlat."); return

        satirlar = ["📊 SON TARAMA SONUÇLARI", ""]
        for kod in sorted(ozet.keys()):
            veri = ozet[kod]
            isim = veri.get("isim", kod)
            tarih = veri.get("tarih", "?")
            sayi = veri.get("sayi", 0)
            emoji = "🟢" if sayi > 0 else "⬜"
            satirlar.append(f"{emoji} /{kod} — {isim}: {sayi} hisse ({tarih})")

        satirlar += ["", "Detay için: /tarasonuc 2  veya  /tarasonuc A"]
        bot.reply_to(message, "\n".join(satirlar))

    else:
        # Tek strateji detayı
        kod = parts[1].upper()
        if kod not in TARA_STRATEJILER:
            bot.reply_to(message, f"Geçersiz strateji: {kod}\nKodlar: 1-14, A, B, C"); return

        veri = tara_load(chat_id, kod)
        if not veri:
            bot.reply_to(message,
                f"Strateji {kod} için kayıtlı sonuç yok.\n"
                f"/tara {kod} ile tarama yap."); return

        isim, aciklama = TARA_STRATEJILER[kod]
        tarih = veri.get("tarih", "?")
        detay = veri.get("detay", [])
        ai_yorum = veri.get("ai_yorum", "")

        satirlar = [
            f"📊 {isim}",
            f"{aciklama}",
            f"Tarama: {tarih} — {len(detay)} eşleşme",
            "━━━━━━━━━━━━━━━━━━━",
        ]

        if not detay:
            satirlar.append("Bu kritere uyan hisse bulunamadı.")
        else:
            for e in detay[:20]:
                t    = e.get("ticker","?")
                rsi  = e.get("rsi", 0)
                rv   = e.get("rel_vol", 0)
                p1d  = e.get("perf_1d", 0)
                p5d  = e.get("perf_5d", 0)
                p21d = e.get("perf_21d", 0)
                satirlar.append(
                    f"{t} | RSI:{rsi:.1f} | RV:{rv:.2f}x | "
                    f"G:{'+' if (p1d or 0)>0 else ''}{(p1d or 0):.1f}% "
                    f"H:{'+' if (p5d or 0)>0 else ''}{(p5d or 0):.1f}% "
                    f"A:{'+' if (p21d or 0)>0 else ''}{(p21d or 0):.1f}%"
                )

        if ai_yorum:
            satirlar += ["", "━━━━━━━━━━━━━━━━━━━", "🤖 AI YORUMU", ai_yorum]

        bot.reply_to(message, "\n".join(satirlar))

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


@bot.message_handler(commands=['sinyal'])
def sinyal_handler(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()

    if len(parts) < 2 or parts[1].lower() not in ("al","sat"):
        bot.reply_to(message,
            "📋 Kullanım:\n"
            "/sinyal al — bugünkü AL sinyalleri\n"
            "/sinyal sat — bugünkü SAT sinyalleri"
        )
        return

    filtre = parts[1].upper()  # "AL" veya "SAT"
    today_key = datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%Y-%m-%d')

    # DB'den bugünkü sinyalleri çek
    try:
        prefix = f"sinyal:{today_key}:"
        conn = db_connect()
        if not conn:
            bot.reply_to(message, "❌ DB bağlantısı kurulamadı."); return
        with conn.cursor() as cur:
            cur.execute("SELECT key, value FROM store WHERE key LIKE %s", (prefix + "%",))
            rows = cur.fetchall()
        conn.close()
    except Exception as e:
        bot.reply_to(message, f"❌ Hata: {e}"); return

    bulunanlar = []
    for key, val in rows:
        try:
            data = json.loads(val)
            tip = data.get("type","")
            if filtre == "AL" and tip in ("AL","KARISIK"):
                bulunanlar.append(data["msg"])
            elif filtre == "SAT" and tip in ("SAT","KARISIK"):
                bulunanlar.append(data["msg"])
        except Exception:
            continue

    if not bulunanlar:
        emoji = "🟢" if filtre == "AL" else "🔴"
        bot.reply_to(message,
            f"{emoji} Bugün için *{filtre}* sinyali bulunamadı.\n"
            f"💡 Önce /check all ile tarama yap.",
            parse_mode='Markdown'
        )
        return

    emoji = "🟢" if filtre == "AL" else "🔴"
    bot.send_message(chat_id,
        f"{emoji} *{len(bulunanlar)} {filtre} sinyali* bulundu — geliyor...",
        parse_mode='Markdown'
    )
    for msg in bulunanlar:
        try:
            bot.send_message(chat_id, msg, parse_mode='Markdown')
            time.sleep(0.3)
        except Exception as e:
            print(f"Sinyal gönderme hatası: {e}")

@bot.message_handler(commands=['watchlist'])
def show_list(message):
    chat_id = str(message.chat.id)
    lst = wl_get(chat_id)
    if lst:
        send_long_message(chat_id, f"📋 *İzleme Listen* ({len(lst)} hisse):\n" + "\n".join(lst))
    else:
        bot.reply_to(message, "📭 Liste boş — /addall yaz")


# ═══════════════════════════════════════════════════════════════
# AI MODÜLÜ — Gemini (Analiz) + Groq (Haber)
# ═══════════════════════════════════════════════════════════════


# ── RSS Haber Kaynakları ──────────────────────────────────────
RSS_FEEDS = {
    # Global piyasa haberleri
    "global": [
        ("Reuters İş Dünyası",   "https://feeds.reuters.com/reuters/businessNews"),
        ("Reuters Genel",        "https://feeds.reuters.com/reuters/topNews"),
        ("CNBC Piyasalar",       "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135"),
        ("MarketWatch",          "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
        ("Nasdaq Haberleri",     "https://www.nasdaq.com/feed/rssoutbound?category=Markets"),
    ],
    # BIST & Türkiye ekonomi haberleri
    "bist": [
        ("Google Borsa TR",      "https://news.google.com/rss/search?q=borsa+istanbul+BIST+hisse&hl=tr&gl=TR&ceid=TR:tr"),
        ("Google Ekonomi TR",    "https://news.google.com/rss/search?q=türkiye+ekonomi+piyasa&hl=tr&gl=TR&ceid=TR:tr"),
        ("Google TCMB",         "https://news.google.com/rss/search?q=TCMB+merkez+bankası+faiz&hl=tr&gl=TR&ceid=TR:tr"),
        ("Bloomberg HT",         "https://www.bloomberght.com/rss"),
        ("Dünya Gazetesi",       "https://www.dunya.com/rss/anasayfa.xml"),
    ],
    # Makro ekonomi - Fed, faiz, dolar
    "macro": [
        ("Google Fed Faiz",      "https://news.google.com/rss/search?q=fed+interest+rate+inflation&hl=en&gl=US&ceid=US:en"),
        ("Google Dolar TL",      "https://news.google.com/rss/search?q=dolar+türk+lirası+kur&hl=tr&gl=TR&ceid=TR:tr"),
        ("Google Petrol Altın",  "https://news.google.com/rss/search?q=oil+gold+price+markets&hl=en&gl=US&ceid=US:en"),
        ("Reuters Piyasa",       "https://feeds.reuters.com/reuters/businessNews"),
    ]
}

# Önemli global anahtar kelimeler
CRISIS_KEYWORDS = [
    "fed", "faiz", "rate", "interest rate", "inflation", "enflasyon",
    "recession", "resesyon", "war", "savaş", "kriz", "crisis",
    "sanctions", "yaptırım", "oil", "petrol", "gold", "altın",
    "dollar", "dolar", "euro", "tcmb", "merkez bankası",
    "savaş", "deprem", "earthquake", "pandemic", "salgın",
    "israel", "ukraine", "rusya", "çin", "china", "trump"
]

def fetch_rss(url, max_items=5, timeout=8):
    """RSS feed'den haber başlıklarını çek."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; BISTBot/1.0)"}
        resp = requests.get(url, timeout=timeout, headers=headers)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        items = []
        # RSS 2.0
        for item in root.findall(".//item")[:max_items]:
            title = item.findtext("title", "").strip()
            desc  = item.findtext("description", "").strip()
            pub   = item.findtext("pubDate", "").strip()
            link  = item.findtext("link", "").strip()
            if title:
                items.append({"title": title, "desc": desc[:200], "pub": pub, "link": link})
        return items
    except Exception as e:
        print(f"RSS hata {url}: {e}")
        return []

def collect_news(categories=None, max_per_feed=5, ticker=None):
    """Belirtilen kategorilerden haberleri topla. ticker varsa filtrele."""
    if categories is None:
        categories = ["global", "bist", "macro"]
    all_news = []
    for cat in categories:
        for name, url in RSS_FEEDS.get(cat, []):
            items = fetch_rss(url, max_items=max_per_feed)
            for item in items:
                item["source"] = name
                item["category"] = cat
                all_news.append(item)
    if ticker:
        # Ticker ile ilgili haberleri filtrele
        filtered = [n for n in all_news if ticker.lower() in (n["title"]+n["desc"]).lower()]
        return filtered if filtered else []
    return all_news

def is_crisis_news(news_list):
    """Kriz haberi mi? Anahtar kelime kontrolü."""
    for n in news_list:
        text = (n["title"] + " " + n["desc"]).lower()
        if any(kw in text for kw in CRISIS_KEYWORDS):
            return True
    return False

def news_to_text(news_list, max_items=10):
    """Haber listesini metin formatına çevir."""
    lines = []
    for i, n in enumerate(news_list[:max_items]):
        lines.append(f"{i+1}. [{n['source']}] {n['title']}")
        if n.get("desc"):
            lines.append(f"   {n['desc'][:120]}...")
    return "\n".join(lines) if lines else "Haber bulunamadı."

# ── Gemini API ────────────────────────────────────────────────
def safe_send(chat_id, text, parse_mode='Markdown'):
    """Markdown parse hatasında düz metin olarak gönder."""
    try:
        bot.send_message(chat_id, text, parse_mode=parse_mode)
    except Exception:
        # Markdown bozuksa tüm özel karakterleri temizle ve düz gönder
        clean = text.replace('*','').replace('_','').replace('`','').replace('[','').replace(']','')
        try:
            bot.send_message(chat_id, clean)
        except Exception as e:
            print(f"safe_send hata: {e}")

# ── Kullanım sayaçları (günlük) ──────────────────────────────
_ai_usage = {
    "gemini_today": 0,
    "gemini_date": "",
    "gemini_last_model": "henuz kullanilmadi",
    "groq_today": 0,
    "groq_date": "",
    "groq_remaining_req": "?",
    "groq_remaining_tokens": "?",
    "groq_reset_req": "?",
    "groq_reset_tokens": "?",
    "gemini_last_error": None,
    "gemini_quota_date": "",   # 429 yaşanan tarih (DB'den yüklenir)
    "gemini_active_key": 1,       # Şu an hangi key kullanılıyor (1 veya 2)
    "groq_active_key": 1,         # Şu an hangi Groq key kullanılıyor
    "groq_last_error": None,
}

def _ai_count(service):
    """Günlük sayacı artır."""
    today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
    if _ai_usage[f"{service}_date"] != today:
        _ai_usage[f"{service}_today"] = 0
        _ai_usage[f"{service}_date"] = today
    _ai_usage[f"{service}_today"] += 1

# Sırayla denenecek Gemini modelleri (en güncel önce)
GEMINI_MODELS = [
    "gemini-2.0-flash-lite",
    "gemini-2.0-flash",
    "gemini-2.0-flash-001",
]

def _gemini_keys():
    """Aktif key listesini döndür — boş olmayanlar."""
    keys = []
    if GEMINI_KEY:  keys.append((1, GEMINI_KEY))
    if GEMINI_KEY2: keys.append((2, GEMINI_KEY2))
    if GEMINI_KEY3: keys.append((3, GEMINI_KEY3))
    return keys

def _groq_keys():
    """Aktif Groq key listesini döndür — boş olmayanlar."""
    keys = []
    if GROQ_KEY:  keys.append((1, GROQ_KEY))
    if GROQ_KEY2: keys.append((2, GROQ_KEY2))
    if GROQ_KEY3: keys.append((3, GROQ_KEY3))
    return keys

def _gemini_key_exhausted(key_no):
    """Bir key'in bugün kota dolup dolmadığını kontrol et."""
    today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
    try:
        return db_get(f"gemini_quota_exhausted_{key_no}") == today
    except Exception:
        return False

def _gemini_mark_exhausted(key_no):
    """Key'i bugün için tükenmiş olarak işaretle."""
    today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
    try:
        db_set(f"gemini_quota_exhausted_{key_no}", today)
    except Exception:
        pass
    _ai_usage["gemini_quota_date"] = today

def gemini_ask(prompt, max_tokens=600):
    """Gemini key1 → key2 → Groq sırasıyla dene."""
    keys = _gemini_keys()
    if not keys:
        if GROQ_KEY:
            _ai_usage["gemini_last_model"] = "groq-fallback(no-key)"
            return groq_ask(prompt, max_tokens)
        return "⚠️ GEMINI_KEY tanımlı değil."

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": max_tokens, "temperature": 0.4}
    }

    for key_no, api_key in keys:
        if _gemini_key_exhausted(key_no):
            print(f"Gemini Key{key_no} bugün tükenmiş, sıradaki deneniyor...")
            continue
        for model in GEMINI_MODELS:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
            try:
                resp = requests.post(url, json=payload, timeout=20)
                if resp.status_code == 404:
                    continue
                if resp.status_code == 429:
                    print(f"Gemini Key{key_no} 429 kota doldu — sıradaki key deneniyor...")
                    _gemini_mark_exhausted(key_no)
                    break
                if resp.status_code == 403:
                    print(f"Gemini Key{key_no} 403 geçersiz")
                    break
                resp.raise_for_status()
                data = resp.json()
                _ai_count("gemini")
                _ai_usage["gemini_last_model"] = f"Key{key_no}/{model}"
                _ai_usage["gemini_active_key"] = key_no
                _ai_usage["gemini_last_error"] = None
                return data["candidates"][0]["content"]["parts"][0]["text"].strip()
            except Exception as e:
                print(f"Gemini hata (Key{key_no}/{model}): {e}")
                continue

    # Tüm keyler tükendi — Groq fallback
    if GROQ_KEY:
        print("Tüm Gemini keyleri başarısız, Groq devreye giriyor...")
        _ai_usage["gemini_last_model"] = "groq-fallback(tum-keyler-tukendi)"
        _ai_usage["gemini_last_error"] = "Tüm keyler tükendi"
        groq_yanit = groq_ask(prompt, max_tokens)
        if not groq_yanit.startswith("⚠️"):
            return "[Groq]\n" + groq_yanit
        return groq_yanit

    debug_log("ERROR", "gemini_ask", "Tüm Gemini keyleri ve Groq başarısız")
    return "⚠️ Tüm AI servisleri yanıt vermedi."


def gemini_analyze_signal(ticker, signals, rsi_d, rsi_w, close_price, ema_d, ema_w):
    """Bir hisse için Gemini analizi: yorum + destek/direnç + karakter."""
    sig_text = "\n".join(signals) if signals else "Sinyal yok"
    prompt = f"""Sen profesyonel bir BIST teknik analisti olarak kısa ve net yorum yapıyorsun.

Hisse: {ticker}
Güncel Fiyat: {close_price:.2f} TL
Günlük EMA: {ema_d[0]}/{ema_d[1]}
Haftalık EMA: {ema_w[0]}/{ema_w[1]}
Günlük RSI: {rsi_d:.1f}
Haftalık RSI: {rsi_w:.1f}
Teknik Sinyaller:
{sig_text}

Lütfen şu formatta yanıt ver (Türkçe, kısa ve net):

📊 YORUM: (2 cümle max — sinyalin güvenilirliği ve genel durum)
🎯 DESTEK: X.XX TL / X.XX TL
🚀 DİRENÇ: X.XX TL / X.XX TL
🧠 HİSSE KARAKTERİ: (volatil mi, trend mi takip ediyor, hacim davranışı — 1 cümle)
⚡ ÖZET: AL / SAT / BEKLE + kısa neden"""

    return gemini_ask(prompt, max_tokens=350)

# ── Groq API ─────────────────────────────────────────────────
def _groq_key_exhausted(key_no):
    """Groq key'inin bugün rate limit yiyip yemediğini kontrol et."""
    today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
    try:
        return db_get(f"groq_ratelimit_{key_no}") == today
    except Exception:
        return False

def _groq_mark_exhausted(key_no):
    """Groq key'ini bugün için tükenmiş işaretle."""
    today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
    try:
        db_set(f"groq_ratelimit_{key_no}", today)
    except Exception:
        pass
    _ai_usage["groq_last_error"] = f"Key{key_no} rate limit"

def groq_ask(prompt, max_tokens=800, model="llama-3.1-8b-instant"):
    """Groq Key1 → Key2 → Key3 sırasıyla dene, rate limit yerse sıradakine geç."""
    keys = _groq_keys()
    if not keys:
        return "⚠️ GROQ_KEY tanımlı değil."

    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": 0.4
    }

    for key_no, api_key in keys:
        if _groq_key_exhausted(key_no):
            print(f"Groq Key{key_no} bugün tükenmiş, sıradaki deneniyor...")
            continue

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=20)

            if resp.status_code == 429:
                rst = resp.headers.get("x-ratelimit-reset-requests", "?")
                print(f"Groq Key{key_no} 429 rate limit (reset: {rst}) — sıradaki key deneniyor...")
                _groq_mark_exhausted(key_no)
                continue

            if resp.status_code == 401:
                print(f"Groq Key{key_no} 401 geçersiz — sıradaki key deneniyor...")
                _ai_usage["groq_last_error"] = f"Key{key_no} 401 geçersiz"
                continue

            resp.raise_for_status()

            # Başarılı — header'ları kaydet
            h = resp.headers
            _ai_usage["groq_remaining_req"]    = h.get("x-ratelimit-remaining-requests", "?")
            _ai_usage["groq_remaining_tokens"] = h.get("x-ratelimit-remaining-tokens", "?")
            _ai_usage["groq_reset_req"]        = h.get("x-ratelimit-reset-requests", "?")
            _ai_usage["groq_reset_tokens"]     = h.get("x-ratelimit-reset-tokens", "?")
            _ai_usage["groq_active_key"]       = key_no
            _ai_count("groq")
            _ai_usage["groq_last_error"] = None
            return resp.json()["choices"][0]["message"]["content"].strip()

        except Exception as e:
            print(f"Groq hata (Key{key_no}): {e}")
            _ai_usage["groq_last_error"] = str(e)[:100]
            debug_log("ERROR", "groq_ask", str(e)[:150], traceback.format_exc()[:200])
            continue

    return "⚠️ Tüm Groq keyleri yanıt vermedi."

def groq_news_summary(news_text, context="genel piyasa"):
    """Groq ile haber özetle ve yorum yap."""
    prompt = f"""Sen BIST ve global piyasalarda uzman bir analistsin. Aşağıdaki haberleri analiz et ve BIST yatırımcısı için net sinyal ver.

Konu: {context}
Haberler:
{news_text}

Türkçe yanıt ver. Her satır emoji ile başlasın. Al/Sat/Bekle önerisini net belirt:

📰 ÖZET: (en önemli 1-2 gelişme, kısa)
📊 BIST ETKİSİ: 🟢 POZİTİF / 🔴 NEGATİF / ⚪ NÖTR — (1 cümle neden)
⚡ SİNYAL: 🟢 AL FIRSATI / 🔴 SAT / ⏸ BEKLE — (kısa gerekçe)
⚠️ RİSK: (varsa — yoksa "Risk görünmüyor")
💡 TAKTİK: (yatırımcı bugün ne yapmalı, 1 cümle)"""

    return groq_ask(prompt, max_tokens=350)

def groq_ticker_news(ticker, news_text):
    """Belirli bir hisse için haber yorumu."""
    prompt = f"""BIST'te işlem gören {ticker} hissesi için haber analizi yap. Yatırımcıya net AL/SAT/BEKLE önerisi ver.

Haberler:
{news_text}

Türkçe, kısa ve net. Emojilerle:

📰 HABER: (en kritik gelişme — 1-2 cümle)
📊 ETKİ: 🟢 POZİTİF / 🔴 NEGATİF / ⚪ NÖTR — (kısa neden)
⚡ ÖNERİ: 🟢 AL / 🔴 SAT / ⏸ BEKLE — (kısa gerekçe)
💡 NOT: (dikkat edilmesi gereken 1 şey)"""

    return groq_ask(prompt, max_tokens=280)

def groq_crisis_check(news_text):
    """Global kriz haberi var mı? Alarm üret."""
    prompt = f"""Aşağıdaki global haberleri incele ve çok kritik kriz/alarm durumu var mı değerlendir.

Haberler:
{news_text}

Sadece GERÇEKTEn ÖNEMLİ kriz/şok haberler için yanıt ver (normal haberler için "YOK" yaz):

Kritik haber varsa:
🚨 KRİZ ALARMI: (ne oldu — 1 cümle)
📉 BIST ETKİSİ: (beklenen etki)
🔴 ÖNERİ: (yatırımcı ne yapmalı)

Önemli kriz yoksa sadece şunu yaz: YOK"""

    result = groq_ask(prompt, max_tokens=250)
    if result.strip().upper() == "YOK" or result.strip().startswith("YOK"):
        return None
    return result

# ── /analiz komutu ───────────────────────────────────────────
@bot.message_handler(commands=['analiz'])
def cmd_analiz(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("analiz", chat_id, parts[1] if len(parts)>1 else "")
    if len(parts) < 2:
        bot.reply_to(message, "Kullanım: /analiz THYAO"); return
    ticker = parts[1].upper().replace(".IS","")

    if not _gemini_keys():
        bot.reply_to(message, "❌ Hiç GEMINI_KEY tanımlı değil. Render'a ekleyin."); return

    bot.send_message(chat_id, f"🤖 *{ticker}* için Gemini analizi yapılıyor...", parse_mode='Markdown')

    def _run():
        try:
            df_d, df_w = get_data(ticker)
            if df_d is None or df_d.empty or len(df_d) < 30:
                bot.send_message(chat_id, f"❌ {ticker} için yeterli veri yok."); return

            # Mevcut EMA ayarlarını al
            ep = ema_get(ticker)
            ep_d = ep.get("daily", (9,21))
            ep_w = ep.get("weekly", (9,21))

            close_price = float(df_d["Close"].iloc[-1])
            rsi_d = float(calc_rsi(df_d["Close"], 14).iloc[-1]) if len(df_d) > 14 else 50.0

            rsi_w = 50.0
            if df_w is not None and len(df_w) > 14:
                rsi_w = float(calc_rsi(df_w["Close"], 14).iloc[-1])

            # Mevcut sinyalleri topla
            signals = []
            ema_s = calc_ema(df_d["Close"], ep_d[0])
            ema_l = calc_ema(df_d["Close"], ep_d[1])
            if ema_s is not None and ema_l is not None:
                if ema_s.iloc[-1] > ema_l.iloc[-1]:
                    signals.append(f"Günlük YUKARI TREND EMA({ep_d[0]}>{ep_d[1]})")
                else:
                    signals.append(f"Günlük AŞAĞI TREND EMA({ep_d[0]}<{ep_d[1]})")

            result = gemini_analyze_signal(ticker, signals, rsi_d, rsi_w, close_price, ep_d, ep_w)

            msg = (
                f"🤖 *{ticker} — Gemini Analizi*\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"{result}\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"📈 [TradingView](https://tr.tradingview.com/chart/?symbol=BIST:{ticker})"
            )
            safe_send(chat_id, msg)
        except Exception as e:
            bot.send_message(chat_id, f"❌ Analiz hatası: {e}")

    threading.Thread(target=_run, daemon=True).start()

# ── /haber komutu ─────────────────────────────────────────────
@bot.message_handler(commands=['haber'])
def cmd_haber(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    call_log("haber", chat_id, parts[1] if len(parts)>1 else "genel")

    if not _groq_keys():
        bot.reply_to(message, "❌ Hiç GROQ_KEY tanımlı değil. Render'a ekleyin."); return

    def _run_haber(ticker=None):
        try:
            now_str = datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%d.%m.%Y %H:%M')

            if ticker:
                # ── Tek hisse haberi ──
                bot.send_message(chat_id, f"📰 *{ticker}* için haberler çekiliyor...")
                all_news = collect_news(["global","bist"], max_per_feed=8, ticker=ticker)
                genel = False
                if not all_news:
                    all_news = collect_news(["bist"], max_per_feed=5)
                    genel = True

                # Ham haberleri önce ayrı ayrı gönder
                bot.send_message(chat_id,
                    f"📋 *{ticker} — {'Genel BIST' if genel else 'İlgili'} Haberler*",
                    parse_mode='Markdown')
                for n in all_news[:5]:
                    safe_send(chat_id,
                        f"📌 *{n['source']}*\n{n['title']}\n_{n['desc'][:120] if n.get('desc') else ''}..._")
                    time.sleep(0.3)

                # Sonra AI yorumu
                time.sleep(0.5)
                news_text = news_to_text(all_news, max_items=8)
                yorum = groq_ticker_news(ticker, news_text)
                safe_send(chat_id,
                    f"🤖 *{ticker} — AI Yorumu*\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"{yorum}\n"
                    f"🕐 {now_str}")

            else:
                # ── Genel haber taraması ──
                today_key = datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%Y-%m-%d')
                signal_tickers = []
                try:
                    conn = db_connect()
                    if conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT key FROM store WHERE key LIKE %s",
                                        (f"sinyal:{today_key}:%",))
                            rows = cur.fetchall()
                        conn.close()
                        signal_tickers = [r[0].split(":")[-1] for r in rows]
                except Exception:
                    pass

                bot.send_message(chat_id, "📡 Haberler çekiliyor, sırayla geliyor...")
                time.sleep(0.5)

                # ── BIST Haberleri (önce) ──
                bist_news = collect_news(["bist"], max_per_feed=4)
                if bist_news:
                    bot.send_message(chat_id, "📊 *BIST & Türkiye Ekonomi Haberleri*", parse_mode='Markdown')
                    for n in bist_news[:4]:
                        safe_send(chat_id,
                            f"🇹🇷 *{n['source']}*\n{n['title']}\n_{n['desc'][:100] if n.get('desc') else ''}..._")
                        time.sleep(0.3)
                    time.sleep(0.5)
                    bist_yorum = groq_news_summary(news_to_text(bist_news, 6), "BIST ve Türk piyasaları")
                    safe_send(chat_id, f"🤖 *BIST Yorumu*\n━━━━━━━━━━━━━━━━━━━\n{bist_yorum}")
                    time.sleep(1)

                # ── Global Haberler ──
                global_news = collect_news(["global"], max_per_feed=4)
                macro_news  = collect_news(["macro"],  max_per_feed=3)
                all_global  = global_news + macro_news

                if all_global:
                    bot.send_message(chat_id, "🌍 *Global Piyasa & Makro Haberler*", parse_mode='Markdown')
                    for n in all_global[:5]:
                        safe_send(chat_id,
                            f"🌐 *{n['source']}*\n{n['title']}\n_{n['desc'][:100] if n.get('desc') else ''}..._")
                        time.sleep(0.3)
                    time.sleep(0.5)

                    # Kriz kontrolü
                    crisis = groq_crisis_check(news_to_text(all_global, 8))
                    if crisis:
                        safe_send(chat_id, f"🚨 *GLOBAL KRİZ ALARMI*\n━━━━━━━━━━━━━━━━━━━\n{crisis}")
                        time.sleep(0.5)

                    global_yorum = groq_news_summary(news_to_text(all_global, 6), "global piyasalar, Fed, dolar, petrol")
                    safe_send(chat_id, f"🤖 *Global Yorum*\n━━━━━━━━━━━━━━━━━━━\n{global_yorum}")
                    time.sleep(1)

                # ── Sinyal hisseleri ──
                if signal_tickers:
                    bot.send_message(chat_id,
                        f"🔍 *{len(signal_tickers)} sinyal hissesi için haberler...*",
                        parse_mode='Markdown')
                    time.sleep(0.3)
                    for t in signal_tickers[:8]:
                        ticker_news = collect_news(["global","bist"], max_per_feed=5, ticker=t)
                        if ticker_news:
                            bot.send_message(chat_id, f"📰 *{t}*", parse_mode='Markdown')
                            for n in ticker_news[:2]:
                                safe_send(chat_id, f"  • {n['title']}")
                                time.sleep(0.2)
                            yorum = groq_ticker_news(t, news_to_text(ticker_news, 4))
                            safe_send(chat_id, f"🤖 *{t} Yorumu*\n{yorum}")
                            time.sleep(0.8)
                else:
                    bot.send_message(chat_id,
                        "💡 Bugün henüz tarama yapılmadı. Önce /check all çalıştır.")

        except Exception as e:
            bot.send_message(chat_id, f"❌ Haber hatası: {e}")

    ticker = parts[1].upper().replace(".IS","") if len(parts) > 1 else None
    threading.Thread(target=_run_haber, args=(ticker,), daemon=True).start()

# ── /kredi komutu ────────────────────────────────────────────
@bot.message_handler(commands=['resetgemini'])
def cmd_resetgemini(message):
    """Gemini kota bayraklarını DB'den temizle — yeni key ekleyince kullan."""
    chat_id = str(message.chat.id)
    try:
        db_del("gemini_quota_exhausted_1")
        db_del("gemini_quota_exhausted_2")
        db_del("gemini_quota_exhausted_3")
        bot.reply_to(message,
            "✅ Gemini kota bayrakları temizlendi!\n"
            "Tüm keyler tekrar aktif sayılacak.\n"
            "/kredi ile kontrol et.")
    except Exception as e:
        bot.reply_to(message, f"❌ Hata: {e}")

@bot.message_handler(commands=['kredi'])
def cmd_kredi(message):
    chat_id = str(message.chat.id)
    tr_tz = pytz.timezone("Europe/Istanbul")
    now_str = datetime.now(tr_tz).strftime("%d.%m.%Y %H:%M")

    today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
    gemini_limit = 1500
    wl_count = len(wl_get(chat_id) or [])

    # --- Gemini key durumları ---
    g_keys = _gemini_keys()
    gemini_used = _ai_usage.get("gemini_today", 0)
    gemini_aktif_no = _ai_usage.get("gemini_active_key", "-")
    gemini_last_model = _ai_usage.get("gemini_last_model", "henuz kullanilmadi")
    gemini_err = _ai_usage.get("gemini_last_error")
    gemini_bar = "█" * min(20, int(gemini_used/gemini_limit*20)) + "░" * max(0, 20-int(gemini_used/gemini_limit*20))
    gemini_pct = int(gemini_used/gemini_limit*100) if gemini_limit > 0 else 0

    g_key_satirlar = []
    for kno, _ in [(1,GEMINI_KEY),(2,GEMINI_KEY2),(3,GEMINI_KEY3)]:
        k_var = [GEMINI_KEY, GEMINI_KEY2, GEMINI_KEY3][kno-1]
        if not k_var:
            g_key_satirlar.append(f"  Key{kno}: ❌ Eklenmemis")
            continue
        doldu = db_get(f"gemini_quota_exhausted_{kno}") == today
        aktif = "← AKTIF" if kno == gemini_aktif_no else ""
        durum = "🔴 KOTA DOLDU" if doldu else "🟢 Hazir"
        g_key_satirlar.append(f"  Key{kno}: {durum} {aktif}")

    if gemini_err and "429" in str(gemini_err):
        gemini_hata = "🔴 Kota sorunu — diger key'e gecildi"
    elif gemini_err:
        gemini_hata = f"Son hata: {gemini_err[:60]}"
    else:
        gemini_hata = "✅ Hata yok"

    # --- Groq key durumları ---
    groq_used = _ai_usage.get("groq_today", 0)
    groq_rem_r = _ai_usage.get("groq_remaining_req", "?")
    groq_rem_t = _ai_usage.get("groq_remaining_tokens", "?")
    groq_rst_r = _ai_usage.get("groq_reset_req", "?")
    groq_err = _ai_usage.get("groq_last_error")
    groq_aktif_no = _ai_usage.get("groq_active_key", "-")

    gr_key_satirlar = []
    for kno, kval in [(1,GROQ_KEY),(2,GROQ_KEY2),(3,GROQ_KEY3)]:
        if not kval:
            gr_key_satirlar.append(f"  Key{kno}: ❌ Eklenmemis")
            continue
        doldu = db_get(f"groq_ratelimit_{kno}") == today
        aktif = "← AKTIF" if kno == groq_aktif_no else ""
        durum = "🔴 RATE LIMIT" if doldu else "🟢 Hazir"
        gr_key_satirlar.append(f"  Key{kno}: {durum} {aktif}")

    groq_hata = f"Son hata: {groq_err[:60]}" if groq_err else "✅ Hata yok"

    lines = [
        "🤖 AI Kredi ve Kullanim Durumu",
        f"Tarih: {now_str}",
        "━━━━━━━━━━━━━━━━━━━",
        "",
        f"🟣 GEMINI — {len(g_keys)}/3 key aktif",
        f"  Bugun: {gemini_used} istek / {gemini_limit} limit ({gemini_pct}%)",
        f"  Doluluk: {gemini_bar}",
        f"  Aktif model: {gemini_last_model}",
    ] + g_key_satirlar + [
        f"  {gemini_hata}",
        "",
        f"🟠 GROQ — {len(_groq_keys())}/3 key aktif",
        f"  Bugun: {groq_used} istek",
        f"  Anlik kalan: {groq_rem_r} istek / {groq_rem_t} token",
        f"  Limit sifirlanma: {groq_rst_r}",
    ] + gr_key_satirlar + [
        f"  {groq_hata}",
        "",
        "━━━━━━━━━━━━━━━━━━━",
        f"Watchlist: {wl_count} hisse",
        "Gemini gece 00:00 / Groq dakika bazli sifirlaniyor",
    ]
    msg = "\n".join(lines)
    bot.send_message(chat_id, msg)


# ── Hata Günlüğü (benim için — Claude debug sistemi) ─────────

_debug_log = collections.deque(maxlen=30)   # Son 30 olay
_call_log  = collections.deque(maxlen=20)   # Son 20 komut çağrısı

def debug_log(seviye, kaynak, mesaj, extra=None):
    """Her önemli olayı kaydet. Seviye: INFO / WARN / ERROR / CRASH"""
    tr_tz = pytz.timezone('Europe/Istanbul')
    zaman = datetime.now(tr_tz).strftime('%H:%M:%S')
    girdi = {
        "t": zaman,
        "lvl": seviye,
        "src": kaynak,
        "msg": mesaj[:300],
        "extra": str(extra)[:200] if extra else None
    }
    _debug_log.append(girdi)
    if seviye in ("ERROR","CRASH"):
        print(f"[{seviye}] {kaynak}: {mesaj}")

def call_log(komut, chat_id, params=""):
    """Komut çağrısını kaydet."""
    tr_tz = pytz.timezone('Europe/Istanbul')
    zaman = datetime.now(tr_tz).strftime('%H:%M:%S')
    _call_log.append({"t": zaman, "cmd": komut, "uid": str(chat_id)[-4:], "p": params[:40]})

# ── /kontrolbot komutu ───────────────────────────────────────
@bot.message_handler(commands=['kontrolbot'])
def cmd_kontrolbot(message):
    chat_id = str(message.chat.id)
    call_log("kontrolbot", chat_id)
    bot.send_message(chat_id, "🔬 Tanılama başlıyor — Claude için detaylı rapor hazırlanıyor...")

    def _run_kontrol():
        sonuclar = []   # (sistem_adi, ok, detay, tam_hata)
        tr_tz = pytz.timezone('Europe/Istanbul')

        def test(ad, fn):
            try:
                ok, detay, tam = fn()
            except Exception as e:
                ok, detay, tam = False, str(e)[:120], traceback.format_exc()
            sonuclar.append((ad, ok, detay, tam))
            debug_log("INFO" if ok else "ERROR", f"kontrolbot/{ad}", detay)

        # ══ 1. TELEGRAM ══
        def t_telegram():
            info = bot.get_me()
            wh   = bot.get_webhook_info()
            wh_url = wh.url[-35:] if wh.url else "BOŞ — polling modu"
            pending = wh.pending_update_count
            return True, f"@{info.username} | Webhook: {wh_url} | Kuyruk: {pending}", None
        test("Telegram + Webhook", t_telegram)

        # ══ 2. VERİTABANI ══
        def t_db():
            if not DATABASE_URL:
                return False, "DATABASE_URL YOK → Render ENV eksik", None
            conn = db_connect()
            if not conn:
                return False, "Bağlantı kurulamadı → DB silinmiş/kapalı olabilir", None
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM store")
                store_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM price_cache")
            conn.close()
            return True, f"store: {store_count} kayıt | Bağlantı: OK", None
        test("PostgreSQL", t_db)

        # ══ 3. WATCHLİST ══
        def t_wl():
            wl = wl_get(chat_id)
            if not wl:
                return False, "Liste boş → /addall yap", None
            return True, f"{len(wl)} hisse | Örnek: {', '.join(wl[:3])}", None
        test("Watchlist", t_wl)

        # ══ 4. YAHOO FİNANCE ══
        def t_yahoo():
            df_d, df_w = get_data("THYAO")
            if df_d is None or df_d.empty:
                return False, "THYAO verisi gelmedi → Yahoo erişim sorunu", \
                       "get_data('THYAO') boş DataFrame döndürdü"
            son_fiyat = df_d['Close'].iloc[-1]
            son_tarih = str(df_d.index[-1])[:10]
            return True, f"THYAO {son_fiyat:.2f} TL | {len(df_d)} günlük veri | Son: {son_tarih}", None
        test("Yahoo Finance", t_yahoo)

        # ══ 5. GEMİNİ API ══
        def t_gemini():
            keys = _gemini_keys()
            if not keys:
                return False, "Hic GEMINI_KEY yok → Render ENV ekle", None
            # gemini_ask() kullanalim — tum key/model fallback otomatik
            yanit = gemini_ask("1+1=? Sadece rakam yaz.", max_tokens=5)
            aktif_model = _ai_usage.get("gemini_last_model", "?")
            aktif_key   = _ai_usage.get("gemini_active_key", "?")
            if yanit.startswith("⚠️") or yanit.startswith("[Groq]"):
                # Groq'a dustuysa gemini basarisiz demektir
                if "[Groq]" in yanit:
                    return False, f"Tum Gemini keyleri 429 — Groq fallback aktif | Yanit: {yanit[:40]}", None
                return False, yanit[:120], None
            return True, f"Key{aktif_key}/{aktif_model} | Yanit: '{yanit[:20]}'", None
        test("Gemini API", t_gemini)

        # ══ 6. GROQ API ══
        def t_groq():
            if not GROQ_KEY:
                return False, "GROQ_KEY YOK → Render ENV'e ekle", None
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"}
            payload = {"model": "llama-3.1-8b-instant",
                       "messages": [{"role":"user","content":"1+1=?"}], "max_tokens": 5}
            r = requests.post(url, json=payload, headers=headers, timeout=12)
            if r.status_code == 401:
                return False, "401 GROQ KEY GEÇERSİZ → Yeni key al", \
                       f"HTTP 401: {r.text[:200]}"
            if r.status_code == 429:
                rst = r.headers.get("x-ratelimit-reset-requests","?")
                return False, f"429 RATE LIMIT — {rst} sonra sıfırlanır", \
                       f"HTTP 429: {r.text[:200]}"
            r.raise_for_status()
            yanit = r.json()["choices"][0]["message"]["content"].strip()
            rem_r = r.headers.get("x-ratelimit-remaining-requests","?")
            rem_t = r.headers.get("x-ratelimit-remaining-tokens","?")
            return True, f"Yanıt: '{yanit}' | Kalan: {rem_r} istek / {rem_t} token | Key: ...{GROQ_KEY[-6:]}", None
        test("Groq API", t_groq)

        # ══ 7. RSS KAYNAKLARI ══
        def t_rss():
            calisan, bozuk = [], []
            for cat, feedler in RSS_FEEDS.items():
                for name, url in feedler:
                    items = fetch_rss(url, max_items=1, timeout=5)
                    if items:
                        calisan.append(name)
                    else:
                        bozuk.append(f"{name}({cat})")
            if not calisan:
                return False, f"HİÇBİR KAYNAK ÇALIŞMIYOR: {', '.join(bozuk[:4])}", \
                       f"Bozuk: {bozuk}"
            ok = len(calisan) >= len(bozuk)
            detay = f"{len(calisan)} aktif / {len(bozuk)} bozuk"
            if bozuk:
                detay += f" | Bozuk: {', '.join(bozuk[:3])}"
            return ok, detay, f"Aktif: {calisan}\nBozuk: {bozuk}" if bozuk else None
        test("RSS Kaynakları", t_rss)

        # ══ RAPOR OLUŞTUR ══
        tr_tz = pytz.timezone('Europe/Istanbul')
        simdi = datetime.now(tr_tz).strftime('%d.%m.%Y %H:%M:%S')
        hatali = [(a,d,t) for a,ok,d,t in sonuclar if not ok]
        tamam  = [(a,d)   for a,ok,d,t in sonuclar if ok]

        # — Özet mesajı (herkese okunabilir) —
        ozet = [f"🔬 KONTROLBOT — {simdi}", ""]
        for ad, ok, detay, _ in sonuclar:
            ozet.append(f"{'✅' if ok else '❌'} {ad}: {detay}")
        ozet += ["",
                 f"{'🟢 TÜM SİSTEMLER NORMAL' if not hatali else f'🔴 {len(hatali)} SORUN BULUNDU'}",
                 f"Uptime: {str(datetime.now(tr_tz) - _bot_start_time).split('.')[0]}"]
        bot.send_message(chat_id, "\n".join(ozet))

        # — Claude için detaylı debug raporu (sadece hata varsa) —
        if hatali:
            debug_mesajlar = [
                "━━━━━━━━━━━━━━━━━━━",
                "🤖 CLAUDE DEBUG RAPORU",
                "Aşağıdakileri kopyalayıp Claude'a yapıştır:",
                "━━━━━━━━━━━━━━━━━━━", ""
            ]

            for ad, detay, tam_hata in hatali:
                debug_mesajlar.append(f"[HATA] {ad}")
                debug_mesajlar.append(f"Mesaj: {detay}")
                if tam_hata:
                    debug_mesajlar.append(f"Detay: {tam_hata[:400]}")
                debug_mesajlar.append("")

            # Son 10 debug log olayı
            debug_mesajlar.append("[SON OLAYLAR]")
            for g in list(_debug_log)[-10:]:
                satir = f"{g['t']} [{g['lvl']}] {g['src']}: {g['msg']}"
                if g.get('extra'):
                    satir += f" | {g['extra']}"
                debug_mesajlar.append(satir)

            # Son 5 komut çağrısı
            debug_mesajlar.append("")
            debug_mesajlar.append("[SON KOMUTLAR]")
            for c in list(_call_log)[-5:]:
                debug_mesajlar.append(f"{c['t']} /{c['cmd']} uid:..{c['uid']} {c['p']}")

            # Konfigürasyon snapshot
            today_str = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")

            def key_durum(key, no, servis):
                if not key: return f"{servis}_KEY{no}: MISSING — Render ENV ekle"
                if servis == "GEMINI":
                    doldu = ""
                    try:
                        doldu = " [KOTA DOLDU]" if db_get(f"gemini_quota_exhausted_{no}") == today_str else " [OK]"
                    except: pass
                    return f"GEMINI_KEY{no}: SET (..{key[-4:]}){doldu}"
                else:
                    doldu = ""
                    try:
                        doldu = " [RATE LIMIT]" if db_get(f"groq_ratelimit_{no}") == today_str else " [OK]"
                    except: pass
                    return f"GROQ_KEY{no}: SET (..{key[-4:]}){doldu}"

            debug_mesajlar += [
                "",
                "[KONFİGÜRASYON]",
                key_durum(GEMINI_KEY,  1, "GEMINI"),
                key_durum(GEMINI_KEY2, 2, "GEMINI"),
                key_durum(GEMINI_KEY3, 3, "GEMINI"),
                key_durum(GROQ_KEY,    1, "GROQ"),
                key_durum(GROQ_KEY2,   2, "GROQ"),
                key_durum(GROQ_KEY3,   3, "GROQ"),
                f"DATABASE_URL: {'SET' if DATABASE_URL else 'MISSING'}",
                f"RENDER_URL: {RENDER_URL[-30:] if RENDER_URL else 'MISSING'}",
                f"Aktif Gemini model: {_ai_usage.get('gemini_last_model','henuz yok')}",
                f"Aktif Groq key: {_ai_usage.get('groq_active_key','?')}",
                f"Bugun Gemini: {_ai_usage.get('gemini_today',0)} istek",
                f"Bugun Groq: {_ai_usage.get('groq_today',0)} istek",
            ]

            bot.send_message(chat_id, "\n".join(debug_mesajlar))

    threading.Thread(target=_run_kontrol, daemon=True).start()


# ── /bulten komutu ───────────────────────────────────────────
@bot.message_handler(commands=['bulten'])
def cmd_bulten(message):
    chat_id = str(message.chat.id)
    parts = message.text.strip().split()
    tip = parts[1].lower() if len(parts) > 1 else "sabah"
    call_log("bulten", chat_id, tip)

    if not GROQ_KEY:
        bot.reply_to(message, "❌ GROQ_KEY tanımlı değil."); return

    def _run_bulten():
        try:
            _send_bulten(chat_id, tip)
        except Exception as e:
            bot.send_message(chat_id, f"❌ Bülten hatası: {e}")

    threading.Thread(target=_run_bulten, daemon=True).start()

def _send_bulten(chat_id, tip="sabah"):
    """Sabah veya akşam bülteni gönder."""
    tr_tz = pytz.timezone('Europe/Istanbul')
    now_str = datetime.now(tr_tz).strftime('%d.%m.%Y %H:%M')

    all_news  = collect_news(["global","bist","macro"], max_per_feed=5)
    news_text = news_to_text(all_news, 12)

    if tip == "sabah":
        prompt = f"""Sen BIST uzmanı bir analistsin. Bugün {now_str} sabahı piyasaları için bülten hazırla.

Haberler:
{news_text}

Türkçe, profesyonel sabah bülteni formatında yaz:

🌅 SABAH BÜLTENİ — {now_str}
━━━━━━━━━━━━━━━━━━━
📰 GÜNÜN ÖNE ÇIKANLARI: (2-3 madde)
🌍 GLOBAL DURUM: (kısa özet)
📊 BIST BEKLENTİSİ: (bugün için genel beklenti)
⚠️ DİKKAT EDİLECEKLER: (risk faktörleri)
💡 STRATEJİ: (bugün için kısa öneri)"""
    else:
        # Akşam bülteni
        today_key = datetime.now(tr_tz).strftime('%Y-%m-%d')
        # Bugünkü sinyalleri özetle
        signal_summary = ""
        try:
            conn = db_connect()
            if conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT key, value FROM store WHERE key LIKE %s",
                                (f"sinyal:{today_key}:%",))
                    rows = cur.fetchall()
                conn.close()
                al_list  = [r[0].split(":")[-1] for r in rows if json.loads(r[1]).get("type") in ("AL","KARISIK")]
                sat_list = [r[0].split(":")[-1] for r in rows if json.loads(r[1]).get("type") in ("SAT","KARISIK")]
                if al_list:
                    signal_summary += f"AL Sinyali: {', '.join(al_list[:10])}\n"
                if sat_list:
                    signal_summary += f"SAT Sinyali: {', '.join(sat_list[:10])}\n"
        except Exception:
            pass

        prompt = f"""Sen BIST uzmanı bir analistsin. Bugün {now_str} kapanış sonrası akşam bülteni hazırla.

Haberler:
{news_text}

Bugünkü Teknik Sinyaller:
{signal_summary if signal_summary else 'Henüz tarama yapılmadı'}

Türkçe akşam bülteni:

🌆 AKŞAM BÜLTENİ — {now_str}
━━━━━━━━━━━━━━━━━━━
📊 GÜNÜN ÖZETİ: (kısa kapanış yorumu)
📰 ÖNEMLİ HABERLER: (2-3 madde)
🔍 TEKNİK GÖRÜNÜM: (bugünkü sinyallere göre yorum)
🔮 YARIN BEKLENTİSİ: (kısa öngörü)
💡 STRATEJİ: (yarın için öneri)"""

    result = groq_ask(prompt, max_tokens=600)
    safe_send(chat_id, result)


# ═══════════════════════════════════════════════
# Otomatik tarama – her gün 18:05
# ═══════════════════════════════════════════════
def auto_scan():
    tr_tz = pytz.timezone('Europe/Istanbul')
    check_date    = None  # 18:05 teknik tarama tarihi
    cuma_date     = None  # Cuma strateji taraması
    bulten_s_date = None  # sabah bülteni
    bulten_a_date = None  # akşam bülteni
    kriz_date     = None  # kriz kontrolü
    while True:
        try:
            now = datetime.now(tr_tz)

            # 09:00 — Sabah bülteni
            if now.hour == 9 and now.minute < 5 and bulten_s_date != now.date():
                bulten_s_date = now.date()
                if GROQ_KEY:
                    for chat_id in wl_all_ids():
                        try:
                            _send_bulten(chat_id, "sabah")
                        except Exception as e:
                            print(f"sabah bülten hata {chat_id}: {e}")

            # 10:00, 14:00, 17:30 — Global kriz alarm kontrolü
            if now.minute < 2 and now.hour in (10, 14, 17) and kriz_date != (now.date(), now.hour):
                kriz_date = (now.date(), now.hour)
                if GROQ_KEY:
                    def _kriz_check():
                        try:
                            global_news = collect_news(["global","macro"], max_per_feed=5)
                            crisis = groq_crisis_check(news_to_text(global_news, 10))
                            if crisis:
                                for chat_id in wl_all_ids():
                                    try:
                                        safe_send(chat_id, f"🚨 *GLOBAL KRİZ ALARMI*\n━━━━━━━━━━━━━━━━━━━\n{crisis}")
                                    except Exception:
                                        pass
                        except Exception as e:
                            print(f"kriz check hata: {e}")
                    threading.Thread(target=_kriz_check, daemon=True).start()

            # Cuma 17:00 — Tam strateji taraması
            is_friday = now.weekday() == 4  # 4 = Cuma
            if is_friday and now.hour == 17 and now.minute < 10 and cuma_date != now.date():
                cuma_date = now.date()
                ids = wl_all_ids()
                print(f"[CUMA_TARA] Haftalık tam strateji taraması başlıyor — {len(ids)} kullanıcı")
                for chat_id in ids:
                    try:
                        bot.send_message(chat_id,
                            "📊 CUMA HAFTALIK STRATEJİ TARAMASI\n"
                            "Tüm 16 strateji çalıştırılıyor...\n"
                            "Tahmini süre: ~15-20 dk")
                        threading.Thread(target=_tara_all, args=(chat_id,), daemon=True).start()
                    except Exception as e:
                        debug_log("ERROR", "cuma_tara", str(e)[:100])

            # 18:05 — Teknik tarama
            if now.hour == 18 and now.minute >= 5 and now.minute < 45 and check_date != now.date():
                check_date = now.date()
                ids = wl_all_ids()
                print(f"[AUTO_SCAN] 18:05 taraması başlıyor — {len(ids)} kullanıcı")
                for chat_id in ids:
                    try:
                        bot.send_message(chat_id,
                            f"🕕 *18:05 Otomatik Tarama Başladı*\n"
                            f"📋 Watchlist taranıyor...\n"
                            f"🚫 Durdurmak için: /iptal check",
                            parse_mode='Markdown')
                        scan_all_stocks(chat_id)
                    except Exception as e:
                        print(f"auto_scan hisse hata {chat_id}: {e}")
                        debug_log("ERROR", "auto_scan", str(e)[:150])

            # 18:30 — Akşam bülteni (tarama bittikten sonra)
            if now.hour == 18 and now.minute >= 30 and now.minute < 35 and bulten_a_date != now.date():
                bulten_a_date = now.date()
                if GROQ_KEY:
                    for chat_id in wl_all_ids():
                        try:
                            _send_bulten(chat_id, "aksam")
                        except Exception as e:
                            print(f"aksam bülten hata {chat_id}: {e}")

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

    # Gemini kota durumunu DB'den yükle
    try:
        today = datetime.now(pytz.timezone("Europe/Istanbul")).strftime("%Y-%m-%d")
        k1_doldu = db_get("gemini_quota_exhausted_1") == today
        k2_doldu = db_get("gemini_quota_exhausted_2") == today
        k3_doldu = db_get("gemini_quota_exhausted_3") == today
        if k1_doldu:
            print(f"[STARTUP] Gemini Key1 kotası bugün dolmuş")
            _ai_usage["gemini_quota_date"] = today
        if k2_doldu:
            print(f"[STARTUP] Gemini Key2 kotası bugün dolmuş")
        if k3_doldu:
            print(f"[STARTUP] Gemini Key3 kotası bugün dolmuş")
        if k1_doldu and k2_doldu and k3_doldu:
            _ai_usage["gemini_last_error"] = "Tüm Gemini keyler kota doldu, Groq aktif"
            print(f"[STARTUP] Tüm Gemini keyleri tükenmiş, Groq fallback aktif")
        elif not k1_doldu and not k2_doldu and not k3_doldu:
            print(f"[STARTUP] Tüm Gemini keyleri normal")
    except Exception as e:
        print(f"Kota yükleme hata (devam ediliyor): {e}")

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
