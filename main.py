# ================================================================
# BERKAY TERMINATOR v3.3 — RAILWAY
# v3.2 fixes + Radar + Watchlist + Dynamic Thresh + FA PreCache
# ================================================================
#
# v3.2 YENILIKLER:
#   Timezone fix   — pytz ile tum zamanlar Europe/Istanbul
#   Calendar fix   — genis pencere, hardcoded TR takvim, actual=0 bug
#   FA hizi        — bare ticker ile calisir ("TCELL" yeter)
#   SQLite pool    — with pattern, connection leak yok
#   RSS timeout    — requests ile timeout, feedparser'a text ver
#   Finnhub rate   — semaphore ile rate limit koruması
#   ML persist     — joblib ile model kaydet/yukle
#   Claude model   — CLAUDE_MODEL env var
#   Telethon       — TELEGRAM_KANALLARI env var
# v3.1 YENILIKLER:
#   Kalici dedup, Source tiering, Event ID, Macro Engine,
#   Feedback butonlari, 3 stream, Multi-horizon ML,
#   Brent Radar (%0.5/%1.0), Brent Teknik (30dk), Petrol +2
# ================================================================

import asyncio
import feedparser
import requests
import hashlib
import json
import logging
import time
import re
import os
import random
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
from contextlib import contextmanager
import threading
from bs4 import BeautifulSoup
import anthropic
from telethon import TelegramClient, events as tg_events
from telethon.sessions import StringSession
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

import math
from html import escape as html_escape

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

IST_TZ = ZoneInfo("Europe/Istanbul")

def ist_now():
    """Her zaman Istanbul saati doner"""
    return datetime.now(IST_TZ)

try:
    import joblib
    JOBLIB_AVAILABLE = True
except ImportError:
    JOBLIB_AVAILABLE = False

try:
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False

# FA botu ayri dosyada (fa_bot.py) — buradan kaldirildi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ================================================================
# KEYLER
# ================================================================
TELEGRAM_TOKEN    = os.environ["TELEGRAM_TOKEN"]
CHAT_ID           = os.environ["CHAT_ID"]
ANTHROPIC_KEY     = os.environ["ANTHROPIC_KEY"]
FINNHUB_KEY       = os.environ["FINNHUB_KEY"]
MARKETAUX_KEY     = os.environ["MARKETAUX_KEY"]
TELEGRAM_API_ID   = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELETHON_SESSION  = os.environ.get("TELETHON_SESSION", "")
CLAUDE_MODEL      = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")

# ================================================================
# AYARLAR
# ================================================================
POLL_INTERVAL     = 12
AI_INTERVAL       = 35
MAX_GLOBAL        = 3
MAX_TURKEY        = 8
TURKEY_THRESH     = 7
GLOBAL_THRESH     = 7
NOVELTY_HOURS     = 24
PRICE_INTERVAL    = 120   # Brent fiyat kontrolu her 2 dk
MACRO_INTERVAL    = 60
FEEDBACK_INTERVAL = 60
BRENT_TEKNIK_INTERVAL = 21600  # 6 saat

# Telethon kanallari — env'den virgul ile ayrilmis, bossa bos liste
_kanals = os.environ.get("TELEGRAM_KANALLARI", "")
TELEGRAM_KANALLARI = [k.strip() for k in _kanals.split(",") if k.strip()]

# ================================================================
# BIST30 + ENTITY
# ================================================================
BIST30 = [
    "THYAO","GARAN","AKBNK","ISCTR","YKBNK","TUPRS","EREGL","ASELS",
    "KCHOL","SAHOL","SISE","TOASO","FROTO","PGSUS","KOZAL","EKGYO",
    "BIMAS","MGROS","ULKER","TCELL","TTKOM","ARCLK","VESTL","DOHOL",
    "PETKM","AYGAZ","ENKAI","TAVHL","LOGO","SOKM"
]

ENTITY_DICT = {
    "THYAO": ["turk hava yollari","thy","turkish airlines"],
    "GARAN": ["garanti","garanti bbva"],
    "AKBNK": ["akbank"],
    "ISCTR": ["is bankasi","isbank"],
    "YKBNK": ["yapi kredi"],
    "TUPRS": ["tupras","tupras"],
    "EREGL": ["eregli","erdemir","isdemir"],
    "ASELS": ["aselsan"],
    "KCHOL": ["koc holding","koc grubu"],
    "SAHOL": ["sabanci holding","sabanci"],
    "SISE":  ["sisecam"],
    "TOASO": ["tofas"],
    "FROTO": ["ford otosan"],
    "PGSUS": ["pegasus"],
    "TCELL": ["turkcell"],
    "TTKOM": ["turk telekom","ttnet"],
    "BIMAS": ["bim","bim magazalari"],
    "MGROS": ["migros"],
    "ARCLK": ["arcelik","beko"],
    "PETKM": ["petkim"],
    "ENKAI": ["enka insaat","enka"],
    "TAVHL": ["tav havalimanlari","tav airports"],
    "KOZAL": ["koza altin","koza anadolu"],
    "EKGYO": ["emlak konut","emlak gyo"],
    "ULKER": ["ulker"],
    "VESTL": ["vestel"],
    "DOHOL": ["dogus holding"],
    "AYGAZ": ["aygaz"],
    "LOGO":  ["logo yazilim"],
    "SOKM":  ["sok market"],
}

# ================================================================
# SOURCE TIERING
# ================================================================
TIER1_SOURCES = [
    "tcmb","spk","bddk","hazine","resmi gazete","borsa istanbul",
    "kap","btk","epdk","ssb","rekabet kurumu","borsaistanbul"
]
TIER2_SOURCES = [
    "reuters","bloomberg","ap news","fed","ecb","iaea","nato",
    "abd disisleri","aa ekonomi","aa gundem","dha","isw",
    "al monitor","finnhub","marketaux","usgs"
]

def kaynak_tier(kaynak):
    k = kaynak.lower()
    if any(s in k for s in TIER1_SOURCES): return 1
    if any(s in k for s in TIER2_SOURCES): return 2
    return 3

def tier_skor_ayarla(skor, tier):
    if tier == 1: return min(10, skor + 1)
    if tier == 3: return max(0, skor - 1)
    return skor

# ================================================================
# PETROL ONCELIKLI HABERLER
# ================================================================
PETROL_KEYWORDS = [
    # OPEC
    "opec toplantisi","opec karari","opec uretim","opec+ kesinti","opec acil",
    "opec meeting","opec cuts","opec output","opec decision",
    # Hormuz / savaş
    "hurmuz bogazi","strait of hormuz","hormuz closed","hormuz tensions",
    "hurmuz kapandi","tanker saldiri","tanker attack",
    # Iran
    "iran petrol","iran oil","iran sanctions","iran yaptirimi",
    "iran nükleer","iran nuclear deal","iran embargo",
    # Rusya enerji
    "rusya petrol","russia oil","russia energy","russian oil",
    "petrol tavan fiyat","oil price cap",
    # Yemen/Husiler
    "husiler","houthi","kizildeniz","red sea tanker","red sea attack",
    # Suudi Arabistan
    "suudi arabistan","saudi arabia","aramco","saudi output",
    # Libya/Irak/Nijerya
    "libya petrol","iraq oil","nigeria oil","libyan output",
    # ABD stok
    "eia petrol stok","eia crude","crude inventory","oil inventory",
    "spr release","strategic petroleum reserve",
    # Genel fiyat
    "brent +","wti +","ham petrol","crude oil rally","oil spike",
    "petrol firlamasi","petrol coktu","petrol sert",
    # Talep
    "china oil demand","cin petrol talebi","global oil demand",
]

def petrol_haberi_mi(baslik):
    b = baslik.lower()
    return any(kw in b for kw in PETROL_KEYWORDS)

# ================================================================
# SEKTOR MATRISI
# ================================================================
SEKTOR_MATRISI = [
    (["petrol fiyat artti","brent yukseldi","opec kesinti","hurmuz riski","iran petrol ambargo"],
     ["TUPRS","PETKM","AYGAZ"], "BEARISH", "Petrol maliyeti artar"),
    (["petrol dustu","brent geriledi","opec uretim artti"],
     ["TUPRS","PETKM","AYGAZ"], "BULLISH", "Petrol dusuyor, maliyet rahatlar"),
    (["savunma ihalesi","ssb sozlesme","siha","insansiz hava","fuze sozlesme"],
     ["ASELS"], "BULLISH", "Savunma siparisi"),
    (["savaş ilan","kara harekat","bombardiman basladi","catisma tirmandi"],
     ["ASELS","KOZAL"], "MIXED", "Jeopolitik risk"),
    (["altin yukseldi","altin rekor","gold rally","xau yukseldi"],
     ["KOZAL"], "BULLISH", "Altin yukseliyor"),
    (["altin dustu","gold drops","altin geriledi"],
     ["KOZAL"], "BEARISH", "Altin dusuyor"),
    (["celik fiyat artti","demir fiyat artti","hrc artti","steel up"],
     ["EREGL"], "BULLISH", "Celik fiyati artiyor"),
    (["celik dustu","steel drops","demir dustu"],
     ["EREGL"], "BEARISH", "Celik fiyati dusuyor"),
    (["faiz artirdi","faiz artis surprizi","tcmb sikilasti","baz puan artis"],
     ["GARAN","AKBNK","ISCTR","YKBNK"], "BEARISH", "Faiz artisi, banka marji sikisiyor"),
    (["faiz indirdi","tcmb gevşedi","baz puan indirim","faiz indirim"],
     ["GARAN","AKBNK","ISCTR","YKBNK"], "BULLISH", "Faiz indirimi, banka karliligi artar"),
    (["dolar sert yukseldi","kur tirmandi","tl deger kaybetti","tl cokustu"],
     ["THYAO","FROTO","EREGL","TUPRS"], "BULLISH", "TL zayfliyor, ihracatci kazaniyor"),
    (["dolar sert yukseldi","kur tirmandi","tl deger kaybetti"],
     ["BIMAS","MGROS","ARCLK"], "BEARISH", "TL zayfliyor, ithalatci maliyet artiyor"),
    (["turizm rekoru","turist sayisi artti","yolcu rekoru"],
     ["THYAO","PGSUS","TAVHL"], "BULLISH", "Turizm guclu"),
    (["buyuk deprem","siddetli deprem","yikici deprem"],
     ["EKGYO","ENKAI"], "BEARISH", "Deprem riski"),
    (["ihracat rekoru","ihracat guclu"],
     ["FROTO","TOASO","EREGL","ARCLK"], "BULLISH", "Ihracat guclu"),
]

def sektor_analiz(baslik):
    b = baslik.lower()
    hisseler, yon, ozet = [], "NOTR", ""
    for keywords, syms, _yon, _oz in SEKTOR_MATRISI:
        if any(kw in b for kw in keywords):
            for s in syms:
                if s not in hisseler:
                    hisseler.append(s)
            if not ozet:
                yon, ozet = _yon, _oz
    return hisseler[:5], yon, ozet

# ================================================================
# ACIL KEYWORDLER
# ================================================================
ACIL_TR = [
    "tcmb acil toplanti","merkez bankasi acil","olaganustu para kurulu",
    "faiz karari aciklandi","faiz karari bugun",
    "buyuk deprem","siddetli deprem","depremin buyuklugu 6","depremin buyuklugu 7",
    "iflas basvurusu","konkordato ilan","spk islem durdurdu",
    "turkiye'ye yaptirim","yaptirim paketi turkiye",
    "sikiyonetim ilan","olaganustu hal ilan","sokaga cikma yasagi",
    "tl kriz","kur 40","kur 45","lira cokustu",
    "bedelsiz sermaye artirimi","temettu dagitim tarihi","pay geri alim programi",
    "satin alma anlasmasi imzalandi","devralma tamamlandi","bilesme onayi",
]
ACIL_GLOBAL = [
    "fed emergency meeting","emergency rate cut",
    "war declared","nuclear strike","nato article 5 invoked",
    "strait of hormuz closed","oil embargo","opec emergency",
    "market circuit breaker","trading halted nyse",
    "sanctions on turkey","iran nuclear deal collapse",
    "sovereign default","imf emergency bailout",
]

def acil_mi(baslik, mod):
    b = baslik.lower()
    return any(kw in b for kw in (ACIL_TR if mod == "turkey" else ACIL_GLOBAL))

# ================================================================
# EVENT ID — Ayni olaydan spam engeli
# ================================================================
EVENT_CLUSTERS = {
    "mideast":    ["israel","iran","tel aviv","tehran","idf","hamas","hizbullah","gaza"],
    "ukraine":    ["ukrayna","rusya","ukraine","russia","putin","zelensky","kyiv"],
    "fed_rate":   ["fomc","powell","federal reserve rate decision","fed rate"],
    "tcmb_rate":  ["tcmb faiz","ppk karari","para politikasi toplanti"],
    "oil_shock":  ["brent +","petrol fiyat artti","opec acil","hurmuz kapandi"],
    "tl_crisis":  ["tl kriz","dolar tavan","kur +","lira cokus"],
    "earthquake": ["buyuk deprem","siddetli deprem","earthquake m","sismik"],
    "china":      ["cin ekonomi","china gdp","pboc","yuan devaluation"],
}

event_log = defaultdict(list)
EVENT_WINDOW_MIN = 20

def event_id_bul(baslik):
    b = baslik.lower()
    for eid, keywords in EVENT_CLUSTERS.items():
        if any(kw in b for kw in keywords):
            return eid
    return None

def event_tekrar_mi(baslik):
    eid = event_id_bul(baslik)
    if not eid:
        return False
    sinir = ist_now() - timedelta(minutes=EVENT_WINDOW_MIN)
    event_log[eid] = [(b, t) for b, t in event_log[eid] if t > sinir]
    duplicate = len(event_log[eid]) >= 2
    event_log[eid].append((baslik, ist_now()))
    return duplicate

# ================================================================
# STREAM BELIRLEME
# ================================================================
def stream_belirle(h):
    b   = h["baslik"].lower()
    k   = h["kaynak"].lower()
    tip = h.get("kaynak_tip", "")
    t   = h.get("tier", 2)

    if t == 1 and any(s in k for s in ["tcmb","spk","bddk","hazine","resmi"]):
        return "MACRO"
    if (tip == "sirket" or
        any(kw in b for kw in ["temettü","bedelsiz","geri alim","kontrat",
                                "sozlesme imza","bilanco","kap "])):
        return "COMPANY"
    if petrol_haberi_mi(b):
        return "PETROL"
    if any(kw in b for kw in ["petrol","brent","altin","gold","savas","war",
                               "fuze","deprem","earthquake","jeopolitik","opec","hurmuz"]):
        return "GEO"
    return "NEWS"

# ================================================================
# SQLITE — Tam schema + Connection Helper
# ================================================================
DB_PATH = "/app/terminator.db"
ML_MODEL_PATH = "/app/ml_model.joblib"

@contextmanager
def db_connect():
    """SQLite connection - with pattern ile leak yok"""
    con = sqlite3.connect(DB_PATH, timeout=10)
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

# ================================================================
# FINNHUB RATE LIMITER
# ================================================================
class FinnhubLimiter:
    """Basit semaphore — max 55 req/dk (limit 60, 5 tampon)"""
    def __init__(self, max_per_min=55):
        self._lock   = threading.Lock()
        self._times  = []
        self._max    = max_per_min

    def wait(self):
        with self._lock:
            now = time.time()
            self._times = [t for t in self._times if now - t < 60]
            if len(self._times) >= self._max:
                sleep_for = 60 - (now - self._times[0]) + 0.1
                time.sleep(max(sleep_for, 0.1))
                self._times = [t for t in self._times if time.time() - t < 60]
            self._times.append(time.time())

    def get(self, url, **kwargs):
        self.wait()
        return requests.get(url, **kwargs)

fh_limiter = FinnhubLimiter()

# ================================================================
# BOT STATE — global degiskenler yerine tek class
# ================================================================
class BotState:
    def __init__(self):
        self.bekl_global      = []
        self.bekl_turkey      = []
        self.son_ai           = 0.0
        self.son_brent_fiyat  = 0.0
        self.son_brent_teknik = 0.0
        self.son_macro        = 0.0
        self.son_ml_guncelle  = 0.0
        self.son_feedback     = 0.0
        self.son_durum        = ist_now()
        self.son_sabah        = None
        self.son_oglen        = None
        self.son_aksam        = None
        self.son_radar        = 0.0
        self.dongu_sayac      = 0
        self.toplam_gonderilen = 0
        self.baslangic        = ist_now()
        self.telegram_kuyruk  = []
        # Dynamic threshold
        self.son_yuksek_skor_zamani = ist_now()
        self.turkey_thresh    = TURKEY_THRESH
        self.global_thresh    = GLOBAL_THRESH

    def dinamik_esik_guncelle(self, gonderilenler):
        """Son 30dk'da skor>=8 yoksa esik dus, varsa yuksel"""
        if gonderilenler:
            self.son_yuksek_skor_zamani = ist_now()
            self.turkey_thresh = TURKEY_THRESH
            self.global_thresh = GLOBAL_THRESH
        else:
            dakika = (ist_now() - self.son_yuksek_skor_zamani).total_seconds() / 60
            if dakika > 60:
                self.turkey_thresh = max(6, TURKEY_THRESH - 2)
                self.global_thresh = max(5, GLOBAL_THRESH - 2)
            elif dakika > 30:
                self.turkey_thresh = max(7, TURKEY_THRESH - 1)
                self.global_thresh = max(6, GLOBAL_THRESH - 1)

state = BotState()

# ================================================================
# WATCHLIST — /watch THYAO GARAN, /unwatch THYAO, /watchlist
# ================================================================
class Watchlist:
    def __init__(self):
        self._load()

    def _load(self):
        try:
            with db_connect() as con:
                con.execute("""
                    CREATE TABLE IF NOT EXISTS watchlist (
                        ticker TEXT PRIMARY KEY
                    )
                """)
                rows = con.execute("SELECT ticker FROM watchlist").fetchall()
            self.tickers = {r[0] for r in rows}
        except Exception:
            self.tickers = set()

    def add(self, tickers):
        added = []
        for t in tickers:
            t = t.upper().replace(".IS","")
            if t and t not in self.tickers:
                self.tickers.add(t)
                try:
                    with db_connect() as con:
                        con.execute("INSERT OR IGNORE INTO watchlist (ticker) VALUES (?)", (t,))
                except Exception:
                    pass
                added.append(t)
        return added

    def remove(self, tickers):
        removed = []
        for t in tickers:
            t = t.upper().replace(".IS","")
            if t in self.tickers:
                self.tickers.discard(t)
                try:
                    with db_connect() as con:
                        con.execute("DELETE FROM watchlist WHERE ticker=?", (t,))
                except Exception:
                    pass
                removed.append(t)
        return removed

    def liste(self):
        return sorted(self.tickers)

    def haberde_var_mi(self, semboller):
        """Watchlist hissesi haberde geciyorsa True"""
        return bool(self.tickers & set(semboller))

watchlist = Watchlist()

# ================================================================
# MARKET RADAR — DXY, US10Y, VIX, XU030 + Brent (mevcut)
# ================================================================
class MarketRadar:
    """DXY/VIX/US10Y/XU030 icin Brent-tarzi alarm sistemi"""

    INSTRUMENTS = {
        "DXY":   {"symbol": "OANDA:SPX500USD", "finnhub": True, "esik_sari": 0.5, "esik_kirmizi": 1.0,
                   "label": "DOLAR ENDEKSI", "etki": "TL, bankalar, EM geneli"},
        "VIX":   {"symbol": "CBOE:VIX",        "finnhub": True, "esik_sari": 5.0, "esik_kirmizi": 10.0,
                   "label": "KORKU ENDEKSI",  "etki": "Risk-off, BIST geneli"},
        "XU030": {"symbol": "XIST:XU030",      "finnhub": True, "esik_sari": 1.0, "esik_kirmizi": 2.0,
                   "label": "BIST30",          "etki": "Endeks geneli"},
        "USDTRY":{"symbol": "OANDA:USDTRY",    "finnhub": True, "esik_sari": 0.5, "esik_kirmizi": 1.0,
                   "label": "USD/TRY",         "etki": "TL, ithalat/ihracat"},
    }

    def __init__(self):
        self.baz = {}  # instrument -> baz fiyat

    def _fiyat_cek(self, inst):
        cfg = self.INSTRUMENTS[inst]
        if cfg["finnhub"]:
            try:
                r = fh_limiter.get(
                    "https://finnhub.io/api/v1/quote",
                    params={"symbol": cfg["symbol"], "token": FINNHUB_KEY},
                    timeout=7
                ).json()
                return r.get("c")  # current price
            except Exception:
                return None
        return None

    def kontrol(self):
        """Tum enstrumanlari kontrol et, alarm listesi don"""
        alarms = []
        for inst, cfg in self.INSTRUMENTS.items():
            fiyat = self._fiyat_cek(inst)
            if not fiyat:
                continue
            if inst not in self.baz:
                self.baz[inst] = fiyat
                continue
            degisim = (fiyat - self.baz[inst]) / self.baz[inst] * 100
            abs_deg = abs(degisim)
            if abs_deg >= cfg["esik_kirmizi"]:
                yon = "📈" if degisim > 0 else "📉"
                alarms.append((inst, cfg, fiyat, self.baz[inst], degisim, "🔴", yon))
                self.baz[inst] = fiyat
            elif abs_deg >= cfg["esik_sari"]:
                yon = "📈" if degisim > 0 else "📉"
                alarms.append((inst, cfg, fiyat, self.baz[inst], degisim, "🟡", yon))
                self.baz[inst] = fiyat
        return alarms

    def alarm_mesaj(self, inst, cfg, fiyat, baz, degisim, seviye, yon):
        return (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{seviye} <b>{cfg['label']} HAREKET</b> {yon}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{baz:.2f}  →  <b>{fiyat:.2f}</b>\n"
            f"Hareket: {yon} <b>%{abs(degisim):.2f}</b>\n\n"
            f"Etki: {cfg['etki']}\n"
            f"⏰ {ist_now().strftime('%H:%M:%S')}"
        )

    def market_context_line(self):
        """Mesajlara eklenecek 1 satirlik piyasa ozeti"""
        parts = []
        for inst in ["XU030", "USDTRY"]:
            cfg = self.INSTRUMENTS[inst]
            try:
                r = fh_limiter.get(
                    "https://finnhub.io/api/v1/quote",
                    params={"symbol": cfg["symbol"], "token": FINNHUB_KEY},
                    timeout=5
                ).json()
                dp = r.get("dp")
                if dp is not None:
                    parts.append(f"{inst} {dp:+.1f}%")
            except Exception:
                pass
        # Brent
        bf = brent_radar._brent_fiyat_cek()
        if bf:
            parts.append(f"Brent {bf:.1f}")
        return "  ·  ".join(parts) if parts else ""

market_radar = MarketRadar()

# ================================================================
# X SENTIMENT ENGINE (Twitter Heat) — twscrape ile
# ================================================================
try:
    from twscrape import API as TwAPI, gather as tw_gather
    TWSCRAPE_AVAILABLE = True
except ImportError:
    TWSCRAPE_AVAILABLE = False

class XSentimentEngine:
    def __init__(self):
        self.api = None
        self.last_scan = 0
        self.SCAN_INTERVAL = 180          # 3 dakika
        self.HEAT_THRESHOLD = 75          # %75 ustu alarm
        self.TWEET_LIMIT = 35
        self.heat_cache = {}              # ticker -> (skor, timestamp)
        self._no_accounts = False         # hesap yoksa tekrar deneme
        self._init_tried = False

        self.TICKER_QUERIES = {
            "THYAO": "THYAO OR thy hava yollari",
            "GARAN": "GARAN OR garanti bbva",
            "AKBNK": "AKBNK OR akbank",
            "TUPRS": "TUPRS OR tupras",
            "ASELS": "ASELS OR aselsan",
            "KOZAL": "KOZAL OR koza altin",
            "EREGL": "EREGL OR eregli demir",
            "ISCTR": "ISCTR OR is bankasi",
            "YKBNK": "YKBNK OR yapi kredi",
            "KCHOL": "KCHOL OR koc holding",
            "SAHOL": "SAHOL OR sabanci",
            "PGSUS": "PGSUS OR pegasus",
            "TCELL": "TCELL OR turkcell",
            "FROTO": "FROTO OR ford otosan",
            "SISE":  "SISE OR sisecam",
            "BIMAS": "BIMAS OR bim market",
            "PETKM": "PETKM OR petkim",
        }
        for t in BIST30:
            if t not in self.TICKER_QUERIES:
                self.TICKER_QUERIES[t] = t

    async def init_api(self):
        if not TWSCRAPE_AVAILABLE:
            return False
        if self.api is None:
            try:
                self.api = TwAPI()
                await self.api.pool.login_all()
                log.info("twscrape API baslatildi — X Sentiment aktif!")
                return True
            except Exception as e:
                log.warning(f"twscrape init: {e}")
                self.api = None
                return False
        return True

    async def heat_kontrol(self, bot):
        if self._no_accounts:
            return  # hesap yok, sessizce atla
        if time.time() - self.last_scan < self.SCAN_INTERVAL:
            return
        ok = await self.init_api()
        if not ok:
            return
        self.last_scan = time.time()

        # Hesap var mi kontrol — ilk taramada belli olur
        if not self._init_tried:
            self._init_tried = True
            try:
                test_tweets = await tw_gather(self.api.search("BIST", limit=1))
            except Exception:
                log.info("X Sentiment: hesap yok, devre disi. /heat komutu da calismiyor.")
                self._no_accounts = True
                return

        # Her seferde 8 ticker tara (rate limit korumasi)
        tickers = list(self.TICKER_QUERIES.keys())
        # Watchlist oncelikli
        wl = watchlist.liste()
        priority = [t for t in wl if t in self.TICKER_QUERIES]
        others   = [t for t in tickers if t not in priority]
        scan_list = (priority + others)[:8]

        for ticker in scan_list:
            query = self.TICKER_QUERIES[ticker]
            try:
                tweets = await tw_gather(self.api.search(query, limit=self.TWEET_LIMIT))
                if len(tweets) < 5:
                    continue
                tweet_texts = [t.rawContent for t in tweets if not t.rawContent.startswith("RT @")]
                if len(tweet_texts) < 3:
                    continue
                text_combined = "\n".join(tweet_texts[:20])
                skor, yon, ozet = await self._claude_heat(ticker, text_combined, len(tweet_texts))
                self.heat_cache[ticker] = (skor, yon, ozet, time.time())
                if skor >= self.HEAT_THRESHOLD:
                    await self._alarm_gonder(bot, ticker, skor, yon, ozet, len(tweet_texts))
                await asyncio.sleep(1.5)
            except Exception as e:
                log.debug(f"X heat {ticker}: {e}")

    async def _claude_heat(self, ticker, tweets_text, count):
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
            prompt = (
                f"Sen BIST traderisin. Son {count} tweet'i analiz et.\n"
                f"Hisse: {ticker}\n\nTweetler:\n{tweets_text[:3000]}\n\n"
                f"0-100 arasi HEAT SKORU ver:\n"
                f"90+ = Cilgin ates (short squeeze, pump)\n"
                f"75-89 = Guclu pozitif momentum\n"
                f"60-74 = Normal konusuluyor\n"
                f"40-59 = Soguk\n"
                f"<40 = Negatif baski\n\n"
                f"SADECE JSON:\n"
                f'{{"skor":87,"yon":"BULLISH","ozet":"max 12 kelime Turkce"}}'
            )
            msg = client.messages.create(
                model=CLAUDE_MODEL, max_tokens=200,
                messages=[{"role": "user", "content": prompt}]
            )
            text = msg.content[0].text.strip()
            if not text.startswith("{"):
                m = re.search(r'\{.*\}', text, re.DOTALL)
                text = m.group(0) if m else '{"skor":50,"yon":"NOTR","ozet":""}'
            data = json.loads(text)
            return int(data.get("skor", 50)), data.get("yon", "NOTR"), data.get("ozet", "")
        except Exception:
            return 40, "NOTR", ""

    async def _alarm_gonder(self, bot, ticker, skor, yon, ozet, tweet_sayisi):
        yon_icon = {"BULLISH": "📈", "BEARISH": "📉"}.get(yon, "➡️")
        seviye = "CILGIN ATES" if skor >= 90 else "GUCLU MOMENTUM"
        msg = (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔥 <b>X HEAT — {seviye}</b> {yon_icon}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>{ticker}</b>  —  <b>{skor}/100</b>\n\n"
            f"{ozet}\n"
            f"Son {tweet_sayisi} tweet analiz edildi\n\n"
            f"⏰ {ist_now().strftime('%H:%M:%S')}"
        )
        try:
            await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.HTML)
            log.info(f"X HEAT: {ticker} ({skor}) {yon}")
        except Exception as e:
            log.error(f"X alarm: {e}")

    async def tek_ticker_heat(self, ticker):
        """Tek ticker icin heat skoru — /heat komutu"""
        ok = await self.init_api()
        if not ok:
            return None
        ticker = ticker.upper().replace(".IS","")
        query = self.TICKER_QUERIES.get(ticker, ticker)
        try:
            tweets = await tw_gather(self.api.search(query, limit=30))
            tweet_texts = [t.rawContent for t in tweets if not t.rawContent.startswith("RT @")]
            if len(tweet_texts) < 3:
                return {"skor": 0, "yon": "NOTR", "ozet": "Yeterli tweet yok", "count": len(tweet_texts)}
            text_combined = "\n".join(tweet_texts[:20])
            skor, yon, ozet = await self._claude_heat(ticker, text_combined, len(tweet_texts))
            return {"skor": skor, "yon": yon, "ozet": ozet, "count": len(tweet_texts)}
        except Exception as e:
            return {"skor": 0, "yon": "NOTR", "ozet": str(e), "count": 0}

x_sentiment = XSentimentEngine()

def db_init():
    with db_connect() as con:
        con.executescript("""
    CREATE TABLE IF NOT EXISTS haberler (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id        TEXT,
        timestamp       TEXT,
        stream          TEXT DEFAULT 'NEWS',
        kaynak          TEXT,
        tier            INTEGER DEFAULT 2,
        baslik          TEXT,
        url             TEXT,
        ai_skor         INTEGER DEFAULT 0,
        ml_skor         REAL DEFAULT 0.5,
        yon             TEXT DEFAULT 'NOTR',
        semboller       TEXT DEFAULT '[]',
        novelty         REAL DEFAULT 1.0,
        mod             TEXT,
        gonderildi      INTEGER DEFAULT 0,
        scheduled_event INTEGER DEFAULT 0,
        surprise        REAL,
        feedback        TEXT,
        telegram_msg_id INTEGER,
        move_5m         REAL,
        move_15m        REAL,
        move_60m        REAL,
        move_close      REAL,
        relative_move   REAL
    );
    CREATE TABLE IF NOT EXISTS dedup_hashes (
        hash        TEXT PRIMARY KEY,
        timestamp   TEXT
    );
    CREATE TABLE IF NOT EXISTS macro_events (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        event_key   TEXT UNIQUE,
        timestamp   TEXT,
        country     TEXT,
        event_name  TEXT,
        importance  TEXT,
        forecast    REAL,
        previous    REAL,
        actual      REAL,
        surprise    REAL,
        phase       TEXT,
        symbols     TEXT
    );
    CREATE TABLE IF NOT EXISTS brent_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp   TEXT,
        price       REAL,
        change_pct  REAL
    );
    """)

# Kalici dedup
gonderilen = set()

def dedup_yukle():
    global gonderilen
    try:
        sinir = (ist_now() - timedelta(hours=24)).isoformat()
        with db_connect() as con:
            rows = con.execute(
                "SELECT hash FROM dedup_hashes WHERE timestamp > ?", (sinir,)
            ).fetchall()
        gonderilen = {r[0] for r in rows}
        log.info(f"Dedup: {len(gonderilen)} hash yuklendi")
    except Exception as e:
        log.warning(f"dedup_yukle: {e}")

def dedup_kaydet(h):
    try:
        with db_connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO dedup_hashes (hash,timestamp) VALUES (?,?)",
                (h, ist_now().isoformat())
            )
    except Exception as e:
        log.debug(f"dedup_kaydet: {e}")

def db_kaydet(h, mod, gonderildi_flag=0):
    try:
        with db_connect() as con:
            cur = con.execute("""
                INSERT INTO haberler
                (event_id,timestamp,stream,kaynak,tier,baslik,url,
                 ai_skor,ml_skor,yon,semboller,novelty,mod,gonderildi,
                 scheduled_event,surprise)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                event_id_bul(h.get("baslik", "")),
                ist_now().isoformat(),
                h.get("stream", "NEWS"),
                h.get("kaynak", ""),
                h.get("tier", 2),
                h.get("baslik", ""),
                h.get("url", ""),
                h.get("skor", 0),
                h.get("ml_skor", 0.5),
                h.get("yon", "NOTR"),
                json.dumps(h.get("semboller", [])),
                h.get("novelty", 1.0),
                mod,
                gonderildi_flag,
                1 if h.get("scheduled_event") else 0,
                h.get("surprise"),
            ))
            haber_id = cur.lastrowid
        return haber_id
    except Exception as e:
        log.debug(f"db_kaydet: {e}")
        return 0

def db_msg_id_guncelle(haber_id, msg_id):
    try:
        with db_connect() as con:
            con.execute(
                "UPDATE haberler SET telegram_msg_id=? WHERE id=?", (msg_id, haber_id)
            )
    except Exception as e:
        log.debug(f"db_msg_id_guncelle: {e}")

# ================================================================
# FEEDBACK BUTONU
# ================================================================
def feedback_keyboard(haber_id):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Iyi",      callback_data=f"fb_good_{haber_id}"),
        InlineKeyboardButton("Gurultu",  callback_data=f"fb_noise_{haber_id}"),
        InlineKeyboardButton("Gec kaldi",callback_data=f"fb_late_{haber_id}"),
    ]])


update_offset = 0

async def feedback_kontrol(bot):
    global update_offset
    try:
        updates = await bot.get_updates(
            offset=update_offset, timeout=0,
            allowed_updates=["callback_query", "message"]
        )
        for upd in updates:
            update_offset = upd.update_id + 1

            # --- KOMUTLAR ---
            msg = upd.message
            if msg and msg.text:
                txt = (msg.text or "").strip()
                chat_id = msg.chat.id

                # /help
                if txt.strip() == "/help":
                    await bot.send_message(chat_id=chat_id, parse_mode=ParseMode.HTML, text=(
                        "<b>BERKAY TERMINATOR v3.3</b>\n\n"
                        "<b>Piyasa:</b>\n"
                        "/radar — canli fiyatlar\n"
                        "/heat THYAO — X heat skoru\n"
                        "/durum — sistem durumu\n\n"
                        "<b>Watchlist:</b>\n"
                        "/watch THYAO GARAN — ekle\n"
                        "/unwatch THYAO — cikar\n"
                        "/watchlist — listeyi gor\n\n"
                        "<b>Otomatik:</b>\n"
                        "Haberler, Brent/DXY/VIX alarm,\n"
                        "X Sentiment, Macro takvim,\n"
                        "09:45 / 13:00 / 17:45 raporlar"
                    ))
                    continue

                # /durum
                elif txt.strip() in ("/durum", "/status"):
                    try:
                        with db_connect() as con:
                            tk  = con.execute("SELECT COUNT(*) FROM haberler").fetchone()[0]
                            mlv = con.execute("SELECT COUNT(*) FROM haberler WHERE relative_move IS NOT NULL").fetchone()[0]
                            fbc = con.execute("SELECT COUNT(*) FROM haberler WHERE feedback IS NOT NULL").fetchone()[0]
                    except Exception:
                        tk, mlv, fbc = 0, 0, 0
                    uptime_h = int((ist_now() - state.baslangic).total_seconds() // 3600)
                    brent_su = brent_radar._brent_fiyat_cek()
                    await bot.send_message(chat_id=chat_id, parse_mode=ParseMode.HTML, text=(
                        f"<b>SISTEM DURUMU</b>\n\n"
                        f"Calisma: {uptime_h} saat\n"
                        f"Gonderilen: {state.toplam_gonderilen} haber\n"
                        f"DB: {tk} kayit | ML: {mlv}/{ml.MIN_VERI}\n"
                        f"TR Esik: {state.turkey_thresh} | GL Esik: {state.global_thresh}\n"
                        f"Brent: {brent_su:.2f if brent_su else '—'}\n"
                        f"Watchlist: {', '.join(watchlist.liste()) or '—'}\n"
                        f"{ist_now().strftime('%d.%m.%Y %H:%M')}"
                    ))
                    continue

                # /radar
                elif txt.strip() == "/radar":
                    parts_r = []
                    for inst in ["XU030", "USDTRY"]:
                        cfg = market_radar.INSTRUMENTS[inst]
                        try:
                            r = fh_limiter.get("https://finnhub.io/api/v1/quote",
                                params={"symbol": cfg["symbol"], "token": FINNHUB_KEY}, timeout=5).json()
                            c, dp = r.get("c"), r.get("dp")
                            if c:
                                parts_r.append(f"{inst}: <b>{c:.2f}</b> ({dp:+.1f}%)" if dp else f"{inst}: <b>{c:.2f}</b>")
                        except Exception:
                            pass
                    bf = brent_radar._brent_fiyat_cek()
                    if bf:
                        parts_r.append(f"Brent: <b>{bf:.2f}</b>")
                    await bot.send_message(chat_id=chat_id, parse_mode=ParseMode.HTML,
                        text=f"<b>CANLI RADAR</b>\n\n" + "\n".join(parts_r) + f"\n\n{ist_now().strftime('%H:%M:%S')}")
                    continue

                # /watch
                elif txt.startswith("/watch") and not txt.startswith("/watchlist"):
                    parts_w = txt.split()[1:]
                    if parts_w:
                        added = watchlist.add(parts_w)
                        await bot.send_message(chat_id=chat_id, parse_mode=ParseMode.HTML,
                            text=f"Eklendi: {', '.join(added) if added else 'zaten var'}")
                    else:
                        await bot.send_message(chat_id=chat_id, text="/watch THYAO GARAN")
                    continue

                # /unwatch
                elif txt.startswith("/unwatch"):
                    parts_w = txt.split()[1:]
                    removed = watchlist.remove(parts_w)
                    await bot.send_message(chat_id=chat_id, parse_mode=ParseMode.HTML,
                        text=f"Cikarildi: {', '.join(removed) if removed else 'bulunamadi'}")
                    continue

                # /watchlist
                elif txt.strip() == "/watchlist":
                    wl = watchlist.liste()
                    await bot.send_message(chat_id=chat_id, parse_mode=ParseMode.HTML,
                        text=f"<b>Watchlist:</b> {', '.join(wl) if wl else 'Bos'}")
                    continue

                # /heat THYAO
                elif txt.startswith("/heat"):
                    parts_h = txt.split()
                    ticker_h = parts_h[1].upper() if len(parts_h) > 1 else ""
                    if not ticker_h:
                        await bot.send_message(chat_id=chat_id, text="/heat THYAO")
                        continue
                    if not TWSCRAPE_AVAILABLE:
                        await bot.send_message(chat_id=chat_id, text="twscrape hesap yok.")
                        continue
                    await bot.send_message(chat_id=chat_id, text=f"X taraniyor: {ticker_h}...")
                    try:
                        result = await x_sentiment.tek_ticker_heat(ticker_h)
                        if result:
                            yon_i = {"BULLISH": "📈", "BEARISH": "📉"}.get(result["yon"], "➡️")
                            await bot.send_message(chat_id=chat_id, parse_mode=ParseMode.HTML, text=(
                                f"🔥 <b>X HEAT — {ticker_h}</b>\n\n"
                                f"Skor: <b>{result['skor']}/100</b> {yon_i}\n"
                                f"{result['ozet']}\n"
                                f"Analiz: {result['count']} tweet"
                            ))
                        else:
                            await bot.send_message(chat_id=chat_id, text="X API baglantisi kurulamadi.")
                    except Exception as e:
                        await bot.send_message(chat_id=chat_id, text=f"Hata: {e}")
                    continue

            # --- CALLBACK (feedback butonlari) ---
            cq = upd.callback_query
            if not cq:
                continue
            await bot.answer_callback_query(callback_query_id=cq.id, text="Kaydedildi")
            parts = cq.data.split("_")
            if len(parts) == 3 and parts[0] == "fb":
                fb_type  = parts[1]
                haber_id = int(parts[2])
                with db_connect() as con:
                    con.execute(
                        "UPDATE haberler SET feedback=? WHERE id=?", (fb_type, haber_id)
                    )
                log.info(f"Feedback {fb_type} -> haber #{haber_id}")
    except Exception as e:
        log.debug(f"feedback_kontrol: {e}")


# FA botu ayri dosyada (fa_bot.py)

# ================================================================
# BRENT RADAR — %0.5 sari, %1.0 kirmizi, her iki yon
# ================================================================
class BrentRadar:
    def __init__(self):
        self.onceki_fiyat   = None
        self.baz_fiyat      = None   # alarm bazi (son alarmdan sonra reset)
        self.son_teknik     = 0.0
        self.ESIK_SARI      = 1.5   # %1.5 sari alarm
        self.ESIK_KIRMIZI   = 3.0   # %3.0 kirmizi alarm
        self.TEKNIK_INTERVAL= BRENT_TEKNIK_INTERVAL

    def _brent_fiyat_cek(self):
        """Yahoo Finance'ten BZ=F (ICE Brent Futures)"""
        try:
            url = "https://query1.finance.yahoo.com/v8/finance/chart/BZ=F"
            r   = requests.get(
                url,
                params={"interval": "1m", "range": "1d"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=8
            )
            data   = r.json()
            meta   = data["chart"]["result"][0]["meta"]
            fiyat  = meta.get("regularMarketPrice") or meta.get("previousClose")
            return round(float(fiyat), 2) if fiyat else None
        except Exception as e:
            log.debug(f"Brent fiyat cek: {e}")
            return None

    def _brent_gecmis_cek(self):
        """20 gunluk gunluk data — pivot hesabi icin"""
        try:
            url = "https://query1.finance.yahoo.com/v8/finance/chart/BZ=F"
            r   = requests.get(
                url,
                params={"interval": "1d", "range": "1mo"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10
            )
            data    = r.json()
            result  = data["chart"]["result"][0]
            quotes  = result["indicators"]["quote"][0]
            highs   = [x for x in quotes.get("high", []) if x]
            lows    = [x for x in quotes.get("low", []) if x]
            closes  = [x for x in quotes.get("close", []) if x]
            opens   = [x for x in quotes.get("open", []) if x]
            return highs, lows, closes, opens
        except Exception as e:
            log.debug(f"Brent gecmis cek: {e}")
            return None, None, None, None

    def _pivot_hesapla(self, h, l, c):
        """Classic Pivot Point hesabi"""
        p  = (h + l + c) / 3
        r1 = (2 * p) - l
        r2 = p + (h - l)
        r3 = h + 2 * (p - l)
        s1 = (2 * p) - h
        s2 = p - (h - l)
        s3 = l - 2 * (h - p)
        return {
            "P": round(p, 2),
            "R1": round(r1, 2), "R2": round(r2, 2), "R3": round(r3, 2),
            "S1": round(s1, 2), "S2": round(s2, 2), "S3": round(s3, 2),
        }

    def _yorum_olustur(self, fiyat, pivot):
        """Kural bazli teknik yorum"""
        p  = pivot["P"]
        r1 = pivot["R1"]
        r2 = pivot["R2"]
        s1 = pivot["S1"]
        s2 = pivot["S2"]

        if fiyat > r2:
            return "Fiyat R2 ustunde — guclu yukselis momentumu, dikkatli izle"
        elif fiyat > r1:
            return "Fiyat R1-R2 arasinda — yukselis devam ediyor, R2 direnç"
        elif fiyat > p:
            return "Fiyat pivot ustunde — pozitif taraf, R1 hedef"
        elif fiyat > s1:
            return "Fiyat pivot altina indi — dikkat, S1 destek test ediliyor"
        elif fiyat > s2:
            return "Fiyat S1-S2 arasinda — zayif seyir, S2 kritik destek"
        else:
            return "Fiyat S2 altinda — guclu asagi baski, trend takip et"

    async def fiyat_kontrol(self, bot):
        """Her 2 dk Brent fiyatini kontrol et, alarm uret"""
        fiyat = self._brent_fiyat_cek()
        if not fiyat:
            return

        # DB'ye kaydet
        try:
            with db_connect() as con:
                con.execute(
                    "INSERT INTO brent_log (timestamp,price) VALUES (?,?)",
                    (ist_now().isoformat(), fiyat)
                )
        except Exception:
            pass

        if self.baz_fiyat is None:
            self.baz_fiyat  = fiyat
            self.onceki_fiyat = fiyat
            return

        degisim = (fiyat - self.baz_fiyat) / self.baz_fiyat * 100
        abs_deg = abs(degisim)

        if abs_deg >= self.ESIK_KIRMIZI:
            # Kirmizi alarm
            yon_icon = "📈" if degisim > 0 else "📉"
            yon_text = "YUKSELIS" if degisim > 0 else "DUSUS"
            msg = (
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔴 <b>BRENT ALARM — {yon_text}</b> {yon_icon}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"<b>{self.baz_fiyat:.2f}  →  {fiyat:.2f}</b>\n"
                f"Hareket: <b>{yon_icon} %{abs_deg:.2f}</b>\n\n"
                f"TUPRS  ·  PETKM  ·  AYGAZ\n\n"
                f"⏰ {ist_now().strftime('%H:%M:%S')}"
            )
            try:
                await bot.send_message(
                    chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.HTML
                )
                log.info(f"BRENT KIRMIZI ALARM: {self.baz_fiyat} -> {fiyat} (%{abs_deg:.2f})")
            except Exception as e:
                log.error(f"Brent alarm: {e}")
            self.baz_fiyat = fiyat  # baz sifirla

        elif abs_deg >= self.ESIK_SARI:
            # Sari alarm
            yon_icon = "📈" if degisim > 0 else "📉"
            msg = (
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🟡 <b>BRENT HAREKET</b> {yon_icon}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{self.baz_fiyat:.2f}  →  <b>{fiyat:.2f}</b>\n"
                f"Hareket: {yon_icon} <b>%{abs_deg:.2f}</b>\n\n"
                f"⏰ {ist_now().strftime('%H:%M:%S')}"
            )
            try:
                await bot.send_message(
                    chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.HTML
                )
                log.info(f"Brent sari alarm: %{abs_deg:.2f}")
            except Exception as e:
                log.error(f"Brent sari: {e}")
            self.baz_fiyat = fiyat

        self.onceki_fiyat = fiyat

    async def teknik_gonder(self, bot):
        """Her 30 dk pivot + MA + teknik seviye mesaji"""
        try:
            fiyat = self._brent_fiyat_cek()
            if not fiyat:
                return

            highs, lows, closes, opens = self._brent_gecmis_cek()
            if not closes or len(closes) < 5:
                return

            # Dunku high/low/close (son kapanan gunun verisi)
            dun_h = highs[-2] if len(highs) >= 2 else highs[-1]
            dun_l = lows[-2]  if len(lows)  >= 2 else lows[-1]
            dun_c = closes[-2] if len(closes) >= 2 else closes[-1]

            # Bugunku intraday high/low
            gun_h = highs[-1]
            gun_l = lows[-1]

            # 20 gunluk MA
            ma20 = round(sum(closes[-20:]) / min(20, len(closes)), 2)

            # 52 haftalik (yaklasik son 260 is gunu — elde en fazla 1 aylik data var)
            all_h = max(highs)
            all_l = min(lows)

            pivot = self._pivot_hesapla(dun_h, dun_l, dun_c)
            yorum = self._yorum_olustur(fiyat, pivot)

            # Fiyatin pivot'a gore konumu
            if fiyat > pivot["P"]:
                konum = "Pivot USTUNDE"
                konum_icon = "+"
            else:
                konum = "Pivot ALTINDA"
                konum_icon = "-"

            msg = (
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🛢️ <b>BRENT TEKNİK</b>  —  {ist_now().strftime('%H:%M')}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💰 Fiyat: <b>{fiyat:.2f}</b>  ({konum_icon} {konum})\n\n"
                f"<b>Pivot Seviyeleri:</b>\n"
                f"   R3:  <code>{pivot['R3']:.2f}</code>\n"
                f"   R2:  <code>{pivot['R2']:.2f}</code>  ← Guclu direnc\n"
                f"   R1:  <code>{pivot['R1']:.2f}</code>  ← Direnc\n"
                f"   P:   <code>{pivot['P']:.2f}</code>   ← Pivot\n"
                f"   S1:  <code>{pivot['S1']:.2f}</code>  ← Destek\n"
                f"   S2:  <code>{pivot['S2']:.2f}</code>  ← Guclu destek\n"
                f"   S3:  <code>{pivot['S3']:.2f}</code>\n\n"
                f"<b>Hareketli Ortalama:</b>\n"
                f"   MA20: <code>{ma20:.2f}</code>  "
                f"{'(fiyat ustunde)' if fiyat > ma20 else '(fiyat altinda)'}\n\n"
                f"<b>Gun ici:</b>\n"
                f"   Yuksek: <code>{gun_h:.2f}</code>  "
                f"Dusuk: <code>{gun_l:.2f}</code>\n\n"
                f"<b>Piyasa araligi:</b>\n"
                f"   Yuksek: <code>{all_h:.2f}</code>  "
                f"Dusuk: <code>{all_l:.2f}</code>\n\n"
                f"💡 {yorum}"
            )
            await bot.send_message(
                chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.HTML
            )
            log.info(f"Brent teknik gonderildi: {fiyat:.2f}")
        except Exception as e:
            log.warning(f"Brent teknik: {e}")

brent_radar = BrentRadar()

# ================================================================
# ML MODELI — Multi-horizon labels
# ================================================================
class MLModel:
    def __init__(self):
        self.model      = None
        self.trained    = False
        self.son_egitim = None
        self.MIN_VERI   = 100
        # Startup'ta kayitli model varsa yukle
        self._load_model()

    def _load_model(self):
        """Kayitli model varsa diskten yukle"""
        if not ML_AVAILABLE or not JOBLIB_AVAILABLE:
            return
        try:
            if os.path.exists(ML_MODEL_PATH):
                self.model   = joblib.load(ML_MODEL_PATH)
                self.trained = True
                log.info("ML model diskten yuklendi!")
        except Exception as e:
            log.warning(f"ML model yukleme: {e}")

    def _save_model(self):
        """Modeli diske kaydet"""
        if not JOBLIB_AVAILABLE or self.model is None:
            return
        try:
            joblib.dump(self.model, ML_MODEL_PATH)
            log.info("ML model diske kaydedildi")
        except Exception as e:
            log.warning(f"ML model kaydetme: {e}")

    def ozellik_cikar(self, h):
        b = h.get("baslik", "").lower()
        t = h.get("tier", 2)
        return [
            h.get("skor", 0) / 10.0,
            h.get("novelty", 1.0),
            len(h.get("semboller", [])) / 5.0,
            1.0 if h.get("yon")=="BULLISH" else (-1.0 if h.get("yon")=="BEARISH" else 0.0),
            1.0 if h.get("kaynak_tip")=="sirket" else 0.0,
            (3 - t) / 2.0,
            1.0 if any(k in b for k in ["tcmb","merkez bankasi","faiz"]) else 0.0,
            1.0 if any(k in b for k in ["deprem","afet","patlama"]) else 0.0,
            1.0 if any(k in b for k in ["temettü","bedelsiz","geri alim","kap"]) else 0.0,
            1.0 if any(k in b for k in ["savas","operasyon","fuze","nato"]) else 0.0,
            1.0 if any(k in b for k in ["petrol","brent","opec","enerji"]) else 0.0,
            1.0 if any(k in b for k in ["dolar","kur","tl","doviz"]) else 0.0,
            1.0 if any(k in b for k in ["fed","ecb","faiz karari","baz puan"]) else 0.0,
            1.0 if any(k in b for k in ["altin","gold","xau"]) else 0.0,
            1.0 if h.get("scheduled_event") else 0.0,
            min(h.get("surprise", 0) or 0, 1.0),
            min(len(h.get("baslik", "")) / 200.0, 1.0),
        ]

    def egit(self):
        if not ML_AVAILABLE:
            return
        try:
            with db_connect() as con:
                rows = con.execute("""
                    SELECT ai_skor,novelty,semboller,yon,baslik,tier,
                           scheduled_event,surprise,relative_move
                    FROM haberler
                    WHERE gonderildi=1 AND relative_move IS NOT NULL
                """).fetchall()
            if len(rows) < self.MIN_VERI:
                log.info(f"ML: {len(rows)}/{self.MIN_VERI} — birikim devam")
                return
            X, y = [], []
            for r in rows:
                h = {
                    "skor": r[0], "novelty": r[1],
                    "semboller": json.loads(r[2] or "[]"),
                    "yon": r[3], "baslik": r[4],
                    "tier": r[5] or 2,
                    "scheduled_event": r[6], "surprise": r[7]
                }
                X.append(self.ozellik_cikar(h))
                y.append(1 if (r[8] or 0) > 0.3 else 0)
            self.model = GradientBoostingClassifier(
                n_estimators=80, max_depth=3, learning_rate=0.1, random_state=42
            )
            self.model.fit(X, y)
            self.trained    = True
            self.son_egitim = ist_now()
            acc = sum(
                1 for i, xi in enumerate(X)
                if self.model.predict([xi])[0] == y[i]
            ) / len(y)
            log.info(f"ML egitildi! {len(rows)} ornek | acc={acc:.2f}")
            self._save_model()
        except Exception as e:
            log.warning(f"ML egit: {e}")

    def skor(self, h):
        if not self.trained or not ML_AVAILABLE:
            return 0.5
        try:
            return round(
                self.model.predict_proba([self.ozellik_cikar(h)])[0][1], 3
            )
        except Exception:
            return 0.5

    def fiyat_guncelle(self, horizons):
        if not ML_AVAILABLE:
            return
        for field, minutes in horizons:
            try:
                sinir_ust = (ist_now() - timedelta(minutes=minutes-2)).isoformat()
                sinir_alt = (ist_now() - timedelta(minutes=minutes+5)).isoformat()
                with db_connect() as con:
                    rows = con.execute(f"""
                        SELECT id, semboller FROM haberler
                        WHERE gonderildi=1 AND {field} IS NULL
                        AND timestamp BETWEEN ? AND ?
                        LIMIT 20
                    """, (sinir_alt, sinir_ust)).fetchall()
                for haber_id, semboller_json in rows:
                    syms = json.loads(semboller_json or "[]")
                    if not syms:
                        continue
                    try:
                        r = requests.get(
                            "https://finnhub.io/api/v1/quote",
                            params={"symbol": f"XIST:{syms[0]}", "token": FINNHUB_KEY},
                            timeout=5
                        ).json()
                        dp = r.get("dp")
                        if dp is None:
                            continue
                        xu030 = requests.get(
                            "https://finnhub.io/api/v1/quote",
                            params={"symbol": "XIST:XU030", "token": FINNHUB_KEY},
                            timeout=5
                        ).json().get("dp", 0) or 0
                        relative = dp - xu030
                        with db_connect() as con2:
                            con2.execute(
                                f"UPDATE haberler SET {field}=?, relative_move=? WHERE id=?",
                                (dp, relative, haber_id)
                            )
                    except Exception:
                        pass
            except Exception as e:
                log.debug(f"fiyat_guncelle {field}: {e}")

ml = MLModel()

# ================================================================
# MACRO ENGINE — 3 Fazli Sistem
# ================================================================
class MacroEngine:
    def __init__(self):
        self.sent_pre      = set()
        self.sent_release  = set()
        self.sent_reaction = set()
        self.reaction_queue = []

        self.EVENT_CONTEXT = {
            "Turkey CPI":               ("TR", ["GARAN","AKBNK","ISCTR","YKBNK","BIMAS"], "TL, bankalar, tuketim"),
            "Turkey PPI":               ("TR", ["TUPRS","EREGL","FROTO"], "Sanayi maliyet"),
            "Turkey Unemployment":      ("TR", ["BIMAS","MGROS","ULKER"], "Tuketim hisseleri"),
            "Turkey Current Account":   ("TR", ["XU030"], "TL ve endeks"),
            "Turkey Industrial Production": ("TR", ["EREGL","FROTO","TOASO"], "Sanayi"),
            "Turkey GDP":               ("TR", ["XU030","GARAN","AKBNK"], "Endeks geneli"),
            "Turkey Trade Balance":     ("TR", ["FROTO","TOASO","EREGL"], "Ihracatcilar"),
            "Turkey Interest Rate":     ("TR", ["GARAN","AKBNK","ISCTR","YKBNK","XU030"], "TUM PIYASA — EN KRITIK"),
            "Turkey Budget Balance":    ("TR", ["XU030"], "Genel endeks"),
            "Turkey Capacity Utilization": ("TR", ["SISE","ARCLK","FROTO"], "Sanayi"),
            "Turkey Consumer Confidence":("TR", ["BIMAS","MGROS","ARCLK"], "Perakende"),
            "Turkey Inflation Rate":    ("TR", ["GARAN","AKBNK","ISCTR","YKBNK"], "Bankalar, TRY"),
            "United States Non Farm Payrolls": ("US", ["XU030"], "Gelisen piyasalar, risk"),
            "United States CPI":        ("US", ["XU030","KOZAL"], "Global risk-off, altin"),
            "United States PPI":        ("US", ["XU030"], "Enflasyon beklentisi"),
            "United States Interest Rate": ("US", ["XU030","GARAN","AKBNK"], "Fed faiz — TUM GELISEN PIYASA"),
            "United States GDP":        ("US", ["XU030"], "Global buyume"),
            "United States Unemployment Rate": ("US", ["XU030"], "ABD isgucu"),
            "United States PMI":        ("US", ["XU030"], "Global aktivite"),
            "United States ADP Employment": ("US", ["XU030"], "NFP oncul gostergesi"),
            "United States Initial Jobless Claims": ("US", ["XU030"], "Haftalik istihdam"),
            "United States Retail Sales": ("US", ["XU030"], "Tuketim"),
        }

        self.SENARYO = {
            "Turkey CPI": {
                "yuksek": "TL uzerinde baski, bankalar negatif, TCMB sikılasma beklentisi artar",
                "dusuk":  "TL'ye destek, bankalar pozitif, TCMB gevşeme yolu acilir"
            },
            "Turkey Interest Rate": {
                "yuksek": "Bankalar baski, TL guclenir, endeks dusuyor olabilir",
                "dusuk":  "Bankalar rallisi, TL zayiflar, endeks yukselir"
            },
            "United States Non Farm Payrolls": {
                "yuksek": "Fed sikiligi, USD guclenir, EM satisi, BIST negatif",
                "dusuk":  "Fed gevşeme beklentisi, EM alim, BIST pozitif"
            },
            "United States CPI": {
                "yuksek": "Fed hawkish, USD guclenir, altin dusuyor, BIST negatif",
                "dusuk":  "Fed dovish, USD zayiflar, altin yukselir, BIST pozitif"
            },
            "United States Interest Rate": {
                "yuksek": "EM cikis, USD guclenir, BIST baski",
                "dusuk":  "EM giris, risk istahi artar, BIST pozitif"
            },
        }

    async def kontrol(self, bot):
        await self._finnhub_kontrol(bot)
        await self._reaction_kontrol(bot)

    async def _finnhub_kontrol(self, bot):
        bugun = ist_now().strftime("%Y-%m-%d")
        yarin = (ist_now() + timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            r = fh_limiter.get(
                "https://finnhub.io/api/v1/calendar/economic",
                params={"from": bugun, "to": yarin, "token": FINNHUB_KEY},
                timeout=10
            ).json()
            for ev in r.get("economicCalendar", []):
                country    = ev.get("country", "")
                impact     = ev.get("impact", "")
                event_name = ev.get("event", "")

                if country == "TR":
                    pass
                elif country == "US" and impact in ["high", "medium"]:
                    pass
                else:
                    continue

                try:
                    # Finnhub saatleri UTC — IST'ye cevir
                    ev_time_utc = datetime.strptime(ev["time"], "%Y-%m-%d %H:%M:%S")
                    ev_time_utc = ev_time_utc.replace(tzinfo=ZoneInfo("UTC"))
                    ev_time = ev_time_utc.astimezone(IST_TZ)
                except Exception:
                    continue

                await self._process_event(bot, ev, ev_time, event_name, country, impact)

            # Hardcoded TR takvim kontrolu
            await self._hardcoded_tr_kontrol(bot)
        except Exception as e:
            log.warning(f"MacroEngine: {e}")

    async def _process_event(self, bot, ev, ev_time, event_name, country, impact):
        now      = ist_now()
        diff_min = (ev_time - now).total_seconds() / 60
        forecast = ev.get("estimate")
        previous = ev.get("prev")
        actual   = ev.get("actual")
        ev_key   = f"{country}_{event_name}_{ev_time.strftime('%Y%m%d')}"

        # Pre-release: 15-45 dk arasi (onceki 25-35 cok dardi)
        if 15 <= diff_min <= 45:
            key = f"pre_{ev_key}"
            if key not in self.sent_pre:
                self.sent_pre.add(key)
                await self._send_pre(bot, ev_time, event_name, country, impact,
                                     forecast, previous)

        # Data release: actual None degilse (0 gecerli veridir!)
        elif diff_min < 5 and actual is not None and str(actual).strip() != "":
            key = f"release_{ev_key}"
            if key not in self.sent_release:
                self.sent_release.add(key)
                try:
                    surprise = None
                    if forecast and float(str(forecast)) != 0:
                        surprise = (float(str(actual)) - float(str(forecast))) / abs(float(str(forecast)))
                    with db_connect() as con:
                        ctx_syms = self.EVENT_CONTEXT.get(event_name, ("","[]",""))[1]
                        con.execute("""
                            INSERT OR REPLACE INTO macro_events
                            (event_key,timestamp,country,event_name,importance,
                             forecast,previous,actual,surprise,phase,symbols)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            ev_key, ist_now().isoformat(), country, event_name,
                            impact, forecast, previous, actual, surprise, "release",
                            json.dumps(ctx_syms)
                        ))
                except Exception:
                    pass
                await self._send_release(bot, ev_time, event_name, country, impact,
                                         actual, forecast, previous)
                ctx = self.EVENT_CONTEXT.get(event_name, ("","[]",""))
                self.reaction_queue.append({
                    "queued_at":  now,
                    "event_name": event_name,
                    "country":    country,
                    "actual":     actual,
                    "forecast":   forecast,
                    "key":        f"reaction_{ev_key}",
                    "symbols":    ctx[1],
                })

    # ================================================================
    # HARDCODED TR TAKVIM — Finnhub'a guvenme, kritik TR event'ler icin
    # TCMB toplanti tarihleri, TUFE, cari denge vs.
    # ================================================================
    TR_HARDCODED_2025 = [
        # (ay, gun, saat, dakika, event_name)
        # TCMB PPK Toplantilari 2025
        (1, 23, 14, 0, "Turkey Interest Rate"),
        (2, 20, 14, 0, "Turkey Interest Rate"),
        (3, 20, 14, 0, "Turkey Interest Rate"),
        (4, 17, 14, 0, "Turkey Interest Rate"),
        (5, 22, 14, 0, "Turkey Interest Rate"),
        (6, 19, 14, 0, "Turkey Interest Rate"),
        (7, 24, 14, 0, "Turkey Interest Rate"),
        (8, 21, 14, 0, "Turkey Interest Rate"),
        (9, 18, 14, 0, "Turkey Interest Rate"),
        (10, 23, 14, 0, "Turkey Interest Rate"),
        (11, 20, 14, 0, "Turkey Interest Rate"),
        (12, 25, 14, 0, "Turkey Interest Rate"),
        # TUFE — genelde ayin 3'u sabah 10:00
        (1, 3, 10, 0, "Turkey CPI"), (2, 3, 10, 0, "Turkey CPI"),
        (3, 3, 10, 0, "Turkey CPI"), (4, 3, 10, 0, "Turkey CPI"),
        (5, 5, 10, 0, "Turkey CPI"), (6, 3, 10, 0, "Turkey CPI"),
        (7, 3, 10, 0, "Turkey CPI"), (8, 4, 10, 0, "Turkey CPI"),
        (9, 3, 10, 0, "Turkey CPI"), (10, 3, 10, 0, "Turkey CPI"),
        (11, 3, 10, 0, "Turkey CPI"), (12, 3, 10, 0, "Turkey CPI"),
    ]
    TR_HARDCODED_2026 = [
        # 2026 TCMB PPK — 2025 patterninden (3. Persembe) tahmini
        (1, 22, 14, 0, "Turkey Interest Rate"),
        (2, 19, 14, 0, "Turkey Interest Rate"),
        (3, 19, 14, 0, "Turkey Interest Rate"),
        (4, 16, 14, 0, "Turkey Interest Rate"),
        (5, 21, 14, 0, "Turkey Interest Rate"),
        (6, 18, 14, 0, "Turkey Interest Rate"),
        (7, 23, 14, 0, "Turkey Interest Rate"),
        (8, 20, 14, 0, "Turkey Interest Rate"),
        (9, 17, 14, 0, "Turkey Interest Rate"),
        (10, 22, 14, 0, "Turkey Interest Rate"),
        (11, 19, 14, 0, "Turkey Interest Rate"),
        (12, 24, 14, 0, "Turkey Interest Rate"),
        # TUFE — ayin 3-5'i arasi 10:00
        (1, 5, 10, 0, "Turkey CPI"), (2, 3, 10, 0, "Turkey CPI"),
        (3, 3, 10, 0, "Turkey CPI"), (4, 3, 10, 0, "Turkey CPI"),
        (5, 4, 10, 0, "Turkey CPI"), (6, 3, 10, 0, "Turkey CPI"),
        (7, 3, 10, 0, "Turkey CPI"), (8, 3, 10, 0, "Turkey CPI"),
        (9, 3, 10, 0, "Turkey CPI"), (10, 5, 10, 0, "Turkey CPI"),
        (11, 3, 10, 0, "Turkey CPI"), (12, 3, 10, 0, "Turkey CPI"),
    ]

    async def _hardcoded_tr_kontrol(self, bot):
        """Hardcoded TR takvimden pre-release alarmi"""
        now   = ist_now()
        yil   = now.year
        events = self.TR_HARDCODED_2025 if yil == 2025 else self.TR_HARDCODED_2026
        for ay, gun, saat, dk, event_name in events:
            if now.month != ay or now.day != gun:
                continue
            ev_time = now.replace(hour=saat, minute=dk, second=0, microsecond=0)
            diff_min = (ev_time - now).total_seconds() / 60
            ev_key = f"TR_{event_name}_{now.strftime('%Y%m%d')}"
            key = f"hc_pre_{ev_key}"

            if 15 <= diff_min <= 45 and key not in self.sent_pre:
                self.sent_pre.add(key)
                ctx     = self.EVENT_CONTEXT.get(event_name, ("TR", [], "—"))
                senaryo = self.SENARYO.get(event_name)
                senaryo_str = ""
                if senaryo:
                    senaryo_str = (
                        f"\n\n<b>Senaryolar:</b>\n"
                        f"Yuksek gelirse: {senaryo['yuksek']}\n"
                        f"Dusuk gelirse:  {senaryo['dusuk']}"
                    )
                syms = "  ".join([f"<code>{s}</code>" for s in ctx[1][:5]]) or "—"
                try:
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=(
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"⏰ <b>{int(diff_min)} DAKIKA SONRA</b>  [TR]\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                            f"📋 <b>{event_name}</b>\n"
                            f"Saat: <code>{ev_time.strftime('%H:%M')}</code>  —  Cok Yuksek\n\n"
                            f"Etkilenecek: {syms}\n"
                            f"{ctx[2]}"
                            f"{senaryo_str}"
                        ),
                        parse_mode=ParseMode.HTML
                    )
                    log.info(f"Hardcoded pre-release: {event_name}")
                except Exception as e:
                    log.error(f"HC pre: {e}")

    async def _reaction_kontrol(self, bot):
        now   = ist_now()
        kalan = []
        for item in self.reaction_queue:
            elapsed = (now - item["queued_at"]).total_seconds() / 60
            if elapsed >= 5 and item["key"] not in self.sent_reaction:
                self.sent_reaction.add(item["key"])
                await self._send_reaction(bot, item)
            elif elapsed < 5:
                kalan.append(item)
        self.reaction_queue = kalan

    async def _send_pre(self, bot, ev_time, event_name, country, impact,
                         forecast, previous):
        ctx         = self.EVENT_CONTEXT.get(event_name, ("", [], "—"))
        flag        = "TR" if country == "TR" else "US"
        syms        = "  ".join([f"<code>{s}</code>" for s in ctx[1][:5]]) or "—"
        ozet        = ctx[2]
        impact_icon = "Cok Yuksek" if impact == "high" else "Orta"
        senaryo     = self.SENARYO.get(event_name)
        senaryo_str = ""
        if senaryo:
            senaryo_str = (
                f"\n\n<b>Senaryolar:</b>\n"
                f"Yuksek gelirse: {senaryo['yuksek']}\n"
                f"Dusuk gelirse:  {senaryo['dusuk']}"
            )
        t_str = f"Beklenti: <code>{forecast}</code>  " if forecast else ""
        p_str = f"Onceki: <code>{previous}</code>"    if previous else ""
        try:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=(
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏰ <b>30 DAKIKA SONRA</b>  [{flag}]\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📋 <b>{event_name}</b>\n"
                    f"Saat: <code>{ev_time.strftime('%H:%M')}</code>  —  {impact_icon}\n\n"
                    f"{t_str}{p_str}\n\n"
                    f"Etkilenecek: {syms}\n"
                    f"{ozet}"
                    f"{senaryo_str}"
                ),
                parse_mode=ParseMode.HTML
            )
            log.info(f"Pre-release: {event_name}")
        except Exception as e:
            log.error(f"send_pre: {e}")

    async def _send_release(self, bot, ev_time, event_name, country, impact,
                             actual, forecast, previous):
        ctx  = self.EVENT_CONTEXT.get(event_name, ("", [], "—"))
        syms = "  ".join([f"<code>{s}</code>" for s in ctx[1][:5]]) or "—"

        surprise_str  = ""
        surprise_icon = ""
        if forecast and str(forecast).strip() not in ("", "0"):
            try:
                surprise = float(str(actual)) - float(str(forecast))
                pct      = surprise / abs(float(str(forecast)))
                if pct > 0.05:
                    surprise_icon = "BEKLENTIDEN SICAK"
                    surprise_str  = f"Surpriz: <b>+{surprise:.3f} (%{pct*100:.1f})</b>"
                elif pct < -0.05:
                    surprise_icon = "BEKLENTIDEN SOGUK"
                    surprise_str  = f"Surpriz: <b>{surprise:.3f} (%{pct*100:.1f})</b>"
                else:
                    surprise_icon = "BEKLENTIYE YAKIN"
                    surprise_str  = f"Surpriz: <b>{surprise:+.3f}</b>"
            except Exception:
                pass

        try:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=(
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 <b>VERI GELDI</b>  [{country}]\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📋 <b>{event_name}</b>\n\n"
                    f"Actual:   <b><code>{actual}</code></b>\n"
                    f"Forecast: <code>{forecast or '—'}</code>\n"
                    f"Previous: <code>{previous or '—'}</code>\n\n"
                    f"{surprise_str}\n"
                    f"<b>{surprise_icon}</b>\n\n"
                    f"Etkilenecek: {syms}\n"
                    f"5 dk icinde piyasa tepkisi geliyor..."
                ),
                parse_mode=ParseMode.HTML
            )
            log.info(f"Release: {event_name} actual={actual}")
        except Exception as e:
            log.error(f"send_release: {e}")

    async def _send_reaction(self, bot, item):
        symbols   = item.get("symbols", [])
        reactions = []
        try:
            r = fh_limiter.get(
                "https://finnhub.io/api/v1/quote",
                params={"symbol": "OANDA:USDTRY", "token": FINNHUB_KEY},
                timeout=5
            ).json()
            if r.get("dp"):
                reactions.append(f"USD/TRY: <b>{r['dp']:+.2f}%</b>")
        except Exception:
            pass
        try:
            r = fh_limiter.get(
                "https://finnhub.io/api/v1/quote",
                params={"symbol": "OANDA:XAUUSD", "token": FINNHUB_KEY},
                timeout=5
            ).json()
            if r.get("dp"):
                reactions.append(f"Altin: <b>{r['dp']:+.2f}%</b>")
        except Exception:
            pass
        if symbols:
            try:
                r = fh_limiter.get(
                    "https://finnhub.io/api/v1/quote",
                    params={"symbol": f"XIST:{symbols[0]}", "token": FINNHUB_KEY},
                    timeout=5
                ).json()
                if r.get("dp"):
                    reactions.append(f"{symbols[0]}: <b>{r['dp']:+.2f}%</b>")
            except Exception:
                pass
        if not reactions:
            return
        try:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=(
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚡ <b>ILK PIYASA TEPKISI</b> (5 dk)\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📋 <b>{item['event_name']}</b>\n\n"
                    + "\n".join(reactions) +
                    f"\n\n⏰ {ist_now().strftime('%H:%M:%S')}"
                ),
                parse_mode=ParseMode.HTML
            )
            log.info(f"Reaction: {item['event_name']}")
        except Exception as e:
            log.error(f"send_reaction: {e}")

macro_engine = MacroEngine()

# ================================================================
# MOMENTUM RADAR
# ================================================================
class MomentumRadar:
    def __init__(self):
        self.kume        = defaultdict(list)
        self.PENCERE_DK  = 15
        self.ESIK_ORTA   = 3
        self.ESIK_KRITIK = 5
        self.gonderilen  = set()
        self.KUMELER = {
            "Israil-Iran":   ["israel","iran","tel aviv","tehran","idf","hamas","hizbullah","gaza"],
            "Ukrayna-Rusya": ["ukrayna","rusya","ukraine","russia","putin","zelensky"],
            "Fed-Faiz":      ["fomc","powell","federal reserve rate","fed rate decision"],
            "TCMB-Faiz":     ["tcmb faiz","ppk karari","para politikasi toplanti"],
            "Petrol-Enerji": ["brent +","petrol fiyat artti","opec acil","hurmuz"],
            "TL-Kriz":       ["tl kriz","dolar tavan","lira cokus"],
            "Altin":         ["altin rekor","gold rally","xau yukseldi"],
            "Deprem":        ["buyuk deprem","siddetli deprem","earthquake m"],
            "Cin-Ekonomi":   ["cin ekonomi","china gdp","pboc","yuan"],
            "Savunma-ASELS": ["aselsan","savunma ihalesi","siha sozlesme","ssb projesi"],
        }
        self.ETKILENENLER = {
            "Israil-Iran":   "ASELS  KOZAL  TUPRS  PETKM",
            "Ukrayna-Rusya": "ASELS  TUPRS  EREGL",
            "Fed-Faiz":      "XU030  Bankalar  USD/TRY",
            "TCMB-Faiz":     "GARAN  AKBNK  ISCTR  YKBNK",
            "Petrol-Enerji": "TUPRS  PETKM  AYGAZ",
            "TL-Kriz":       "THYAO / FROTO / EREGL (pozitif)  |  BIMAS / MGROS (negatif)",
            "Altin":         "KOZAL",
            "Deprem":        "EKGYO  ENKAI",
            "Cin-Ekonomi":   "EREGL  PETKM  ihracatcilar",
            "Savunma-ASELS": "ASELS",
        }

    def ekle(self, baslik):
        b = baslik.lower()
        for kume_adi, keywords in self.KUMELER.items():
            if any(kw in b for kw in keywords):
                self.kume[kume_adi].append((baslik, ist_now()))

    def _temizle(self):
        sinir = ist_now() - timedelta(minutes=self.PENCERE_DK)
        for k in list(self.kume.keys()):
            self.kume[k] = [(b, t) for b, t in self.kume[k] if t > sinir]

    def kontrol(self):
        self._temizle()
        alarmlar = []
        for kume_adi, haberler in self.kume.items():
            n = len(haberler)
            if n < self.ESIK_ORTA:
                continue
            anahtar = f"{kume_adi}_{n // self.ESIK_ORTA}"
            if anahtar in self.gonderilen:
                continue
            self.gonderilen.add(anahtar)
            alarmlar.append((kume_adi, n, haberler[-3:]))
        if len(self.gonderilen) > 300:
            self.gonderilen.clear()  # set sirasiz, dilimlemek guvenilmez
        return alarmlar

    def alarm_mesaj(self, kume_adi, n, son_h):
        ikon   = "KRITIK MOMENTUM" if n >= self.ESIK_KRITIK else "HIZLANIYOR"
        prefix = "🔥" if n >= self.ESIK_KRITIK else "⚡"
        etki   = self.ETKILENENLER.get(kume_adi, "BIST geneli")
        haberler_str = "".join([f"  - {b[:80]}\n" for b, _ in son_h])
        return (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{prefix} <b>{ikon}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Konu: <b>{kume_adi}</b>\n"
            f"Son {self.PENCERE_DK} dk: <b>{n} haber</b>\n\n"
            f"Son haberler:\n{haberler_str}\n"
            f"Etkilenecek: {etki}\n"
            f"⏰ {ist_now().strftime('%H:%M:%S')}"
        )

momentum = MomentumRadar()

# ================================================================
# NOVELTY MEMORY
# ================================================================
class NoveltyMemory:
    def __init__(self):
        self.memory = defaultdict(list)

    def _temizle(self):
        sinir = ist_now() - timedelta(hours=NOVELTY_HOURS)
        for e in list(self.memory.keys()):
            self.memory[e] = [(kws, ts) for kws, ts in self.memory[e] if ts > sinir]

    def skorla(self, entity, baslik):
        self._temizle()
        kws    = set(re.findall(r'\w{4,}', baslik.lower()))
        max_ov = 0.0
        for gecmis_kws, _ in self.memory.get(entity, []):
            if kws and gecmis_kws:
                ov     = len(kws & gecmis_kws) / max(len(kws), len(gecmis_kws))
                max_ov = max(max_ov, ov)
        self.memory[entity].append((kws, ist_now()))
        if max_ov > 0.65: return 0.15
        if max_ov > 0.35: return 0.55
        return 1.0

    def entity_bul(self, baslik):
        b   = baslik.lower()
        out = []
        for t in BIST30:
            if t.lower() in b:
                out.append(t)
        for t, vs in ENTITY_DICT.items():
            if t not in out:
                for v in vs:
                    if v in b:
                        out.append(t)
                        break
        return out[:5]

nov = NoveltyMemory()

# ================================================================
# RSS KAYNAKLARI
# ================================================================
RSS_GLOBAL = [
    # Tier 1 — Haber ajansları (en hızlı, en güvenilir)
    ("Reuters",         "https://feeds.reuters.com/reuters/topNews"),
    ("Reuters Biz",     "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters EM",      "https://feeds.reuters.com/reuters/emergingMarketsNews"),
    ("AP News",         "https://rsshub.app/apnews/topics/ap-top-news"),
    # Tier 1 — Resmi kurumlar
    ("FED",             "https://www.federalreserve.gov/feeds/press_all.xml"),
    ("ECB",             "https://www.ecb.europa.eu/rss/press.html"),
    ("IMF",             "https://www.imf.org/en/News/rss?language=eng"),
    ("ABD Disisleri",   "https://www.state.gov/rss-feed/press-releases/feed/"),
    ("NATO",            "https://www.nato.int/cps/en/natolive/news.rss"),
    ("IAEA",            "https://www.iaea.org/feeds/topstories.xml"),
    # Tier 2 — Kaliteli finans/politika medyası
    ("FT",              "https://www.ft.com/rss/home/us"),
    ("FT Markets",      "https://www.ft.com/rss/home/uk"),
    ("WSJ",             "https://feeds.a.dj.com/rss/RSSMarketsMain.xml"),
    ("WSJ Ekonomi",     "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml"),
    ("Axios",           "https://api.axios.com/feed/"),
    ("Politico",        "https://www.politico.com/rss/politics08.xml"),
    ("Politico Global", "https://www.politico.eu/rss"),
    ("BBC Dunya",       "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("CNBC Markets",    "https://www.cnbc.com/id/15839135/device/rss/rss.html"),
    ("ZeroHedge",       "https://feeds.feedburner.com/zerohedge/feed"),
    # Enerji & jeopolitik
    ("EIA Petrol",      "https://www.eia.gov/rss/todayinenergy.xml"),
    ("OilPrice",        "https://oilprice.com/rss/main"),
    ("ISW",             "https://www.understandingwar.org/rss.xml"),
    ("Al Monitor ME",   "https://www.al-monitor.com/rss/mideast.xml"),
    ("Al Monitor TR",   "https://www.al-monitor.com/rss/turkey.xml"),
    # Deprem
    ("USGS Deprem",     "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.atom"),
]

RSS_TURKEY = [
    # Tier 1 — Resmi kurumlar (mutlaka)
    ("TCMB",            "https://www.tcmb.gov.tr/wps/wcm/connect/TR/rss/"),
    ("SPK",             "https://www.spk.gov.tr/rss/HaberDetay"),
    ("BDDK",            "https://www.bddk.org.tr/Rss/RssDuyuru"),
    ("Hazine",          "https://www.hmb.gov.tr/rss"),
    ("Borsa Istanbul",  "https://www.borsaistanbul.com/tr/rss/haberler"),
    ("Resmi Gazete",    "https://www.resmigazete.gov.tr/rss/tum.xml"),
    # Tier 2 — Güvenilir haber ajansları
    ("AA Ekonomi",      "https://www.aa.com.tr/tr/rss/default?cat=ekonomi"),
    ("AA Gundem",       "https://www.aa.com.tr/tr/rss/default?cat=gundem"),
    ("DHA Ekonomi",     "https://www.dha.com.tr/rss/ekonomi.xml"),
    ("Bloomberg HT",    "https://www.bloomberght.com/rss"),
    ("Dunya",           "https://www.dunya.com/rss/rss.xml"),
    # Tier 2 — Finans odakli
    ("Para Analiz",     "https://www.paraanaliz.com/feed/"),
    ("Ekonomim",        "https://www.ekonomim.com/rss"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":     "application/rss+xml, application/xml, text/xml, */*",
}

# ================================================================
# TOPLAYICILAR
# ================================================================
def yeni_h(kaynak, baslik, url, tip="rss"):
    tier = kaynak_tier(kaynak)
    return {
        "kaynak":          kaynak,
        "baslik":          baslik.strip()[:260],
        "url":             url,
        "zaman":           ist_now().strftime("%H:%M"),
        "skor":            0,
        "ml_skor":         0.5,
        "yon":             "NOTR",
        "semboller":       [],
        "sektor_ozet":     "",
        "stream":          "NEWS",
        "ozet":            "",
        "novelty":         1.0,
        "kaynak_tip":      tip,
        "tier":            tier,
        "scheduled_event": False,
        "surprise":        None,
    }

def rss_cek(feeds, max_age_hours=4):
    """RSS cek. max_age_hours: TR=12, Global=4 (dedup zaten var, age filtre gevsetildi)"""
    out = []
    for kaynak, url in feeds:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            feed = feedparser.parse(resp.text)
            for e in feed.entries[:6]:
                b = (e.get("title") or "").strip()
                if not b or len(b) <= 10:
                    continue
                published = e.get("published_parsed") or e.get("updated_parsed")
                if published:
                    try:
                        age_hours = (time.time() - time.mktime(published)) / 3600
                        if age_hours > max_age_hours:
                            continue
                    except Exception:
                        pass
                out.append(yeni_h(kaynak, b, e.get("link", "")))
        except Exception as e:
            log.debug(f"RSS {kaynak}: {e}")
    return out

def marketaux_cek():
    out = []
    try:
        r = requests.get(
            "https://api.marketaux.com/v1/news/all",
            params={"countries":"tr","filter_entities":"true","language":"tr,en",
                    "limit":10,"api_token":MARKETAUX_KEY},
            timeout=12, headers=HEADERS
        )
        for item in r.json().get("data", []):
            b = item.get("title", "")
            if not b:
                continue
            syms = [e.get("symbol","") for e in item.get("entities",[])
                    if e.get("exchange","") in ["BIST","XIST","IST"] and e.get("symbol")]
            h            = yeni_h("Marketaux", b, item.get("url",""), "api")
            h["semboller"] = syms[:4]
            out.append(h)
    except Exception as e:
        log.warning(f"Marketaux: {e}")
    return out

def finnhub_genel_cek():
    out = []
    try:
        r = fh_limiter.get(
            "https://finnhub.io/api/v1/news",
            params={"category":"general","token":FINNHUB_KEY},
            timeout=10, headers=HEADERS
        )
        for item in r.json()[:10]:
            b = item.get("headline", "")
            if b:
                out.append(yeni_h(item.get("source","Finnhub"), b, item.get("url",""), "api"))
    except Exception as e:
        log.warning(f"Finnhub genel: {e}")
    return out

def finnhub_bist_cek():
    out   = []
    dun   = (ist_now() - timedelta(days=3)).strftime("%Y-%m-%d")
    bugun = ist_now().strftime("%Y-%m-%d")
    for s in random.sample(BIST30, 6):
        try:
            r = fh_limiter.get(
                "https://finnhub.io/api/v1/company-news",
                params={"symbol":f"XIST:{s}","from":dun,"to":bugun,"token":FINNHUB_KEY},
                timeout=7, headers=HEADERS
            )
            for item in r.json()[:2]:
                b = item.get("headline","")
                if b:
                    h              = yeni_h(f"Finnhub [{s}]", f"[{s}] {b}",
                                            item.get("url",""), "sirket")
                    h["semboller"] = [s]
                    out.append(h)
            time.sleep(0.1)
        except Exception:
            pass
    return out

def sirket_haberleri_cek():
    out = []
    for ad, url, domain in [
        ("Foreks",       "https://www.foreks.com/haberler/sirket-haberleri/",  "www.foreks.com"),
        ("BorsaGundem",  "https://www.borsagundem.com.tr/sirket-haberleri",    "www.borsagundem.com.tr"),
        ("Investing TR", "https://tr.investing.com/news/company-news",          "tr.investing.com"),
    ]:
        try:
            r    = requests.get(url, timeout=12, headers=HEADERS)
            soup = BeautifulSoup(r.text, "html.parser")
            seen = set()
            for sel in ["h3 a","h2 a","h4 a",".news-title a","article a"]:
                for item in soup.select(sel)[:12]:
                    b    = item.get_text(strip=True)
                    href = item.get("href","")
                    if b and len(b) > 20 and b not in seen:
                        seen.add(b)
                        full = href if href.startswith("http") else f"https://{domain}{href}"
                        out.append(yeni_h(ad, b[:260], full, "sirket"))
        except Exception as e:
            log.warning(f"{ad}: {e}")
    return out

# ================================================================
# CLAUDE ANALIZ
# ================================================================
def claude_skore_et(haberler, mod):
    if not haberler:
        return []
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        liste  = "\n".join([
            f"{i+1}. [T{h['tier']}] {h['baslik']}"
            for i, h in enumerate(haberler)
        ])

        if mod == "global":
            prompt = (
                f"Sen BIST/VIOP uzmani Turk trader. Global haberleri BIST etkisi ile skorla.\n"
                f"Kaynak kalitesi: T1=FED/ECB/Reuters/AP, T2=FT/WSJ/Axios/Politico/Bloomberg, T3=diger\n\n"
                f"KRITIK SORU: Bu haber BIST'te BUGUN trade edilebilir mi?\n\n"
                f"PUANLAMA:\n"
                f"9-10 = Aninda etki: Fed acil toplanti, buyuk savas, Hurmuz kapaniyor, nukleer kriz\n"
                f"8    = Bugun trade edilir: Fed/ECB surpriz karar, petrol %2+, Iran/Rusya yaptirimi, NATO krizi\n"
                f"7    = Takip edilir: Onemli veri surprizi, jeopolitik gerilim, EM sell-off\n"
                f"4-6  = Zayif etki: Beklenen veri, kucuk gelisme\n"
                f"0-3  = BIST ile ilgisi yok: Yerel politika, magazin, spor\n\n"
                f"T1 kaynak +1, T3 kaynak -1. Petrol/Hurmuz/Iran/OPEC haberleri dikkat.\n\n"
                f"SADECE JSON:\n"
                f'[{{"id":1,"skor":9,"yon":"BEARISH","semboller":["TUPRS","PETKM"],"ozet":"max 10 kelime Turkce"}}]\n\n'
                f"Haberler:\n{liste}"
            )
        else:
            prompt = (
                f"Sen BIST/VIOP uzmani Turk trader. Turkiye haberlerini skorla.\n"
                f"BIST30: {', '.join(BIST30)}\n"
                f"[SEMBOL] ile baslayanlar = KAP/sirket haberi — otomatik +1 puan.\n"
                f"T1 kaynak (TCMB/SPK/BDDK/AA/Bloomberg HT) +1, diger medya -1.\n\n"
                f"KRITIK SORU: Bu haber BIST'te BUGUN trade edilebilir mi?\n\n"
                f"PUANLAMA:\n"
                f"9-10 = Aninda: TCMB surpriz faiz, TL krizi, yaptirimi, buyuk deprem, KAP sermaye artirimi\n"
                f"8    = Bugun trade edilir: BIST30 KAP karari, TUFE/buyume surprizi, BDDK karari, temettü\n"
                f"7    = Takip: Sektor haberi, orta sirket, ekonomi yorumu\n"
                f"4-6  = Zayif: Kucuk sirket, rutin aciklama\n"
                f"0-3  = GURULTU: Siyaset, spor, magazin, tahmin/yorum\n\n"
                f"SADECE JSON:\n"
                f'[{{"id":1,"skor":8,"yon":"BULLISH","semboller":["THYAO"],"ozet":"max 10 kelime Turkce"}}]\n\n'
                f"Haberler:\n{liste}"
            )

        msg  = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=2500,
            messages=[{"role": "user", "content": prompt}]
        )
        text = msg.content[0].text.strip()
        # JSON array'i cikart — daha robust parsing
        try:
            if not text.startswith("["):
                m = re.search(r'\[\s*\{.*?\}\s*\]', text, re.DOTALL)
                text = m.group(0) if m else "[]"
            json.loads(text)  # validate
        except (json.JSONDecodeError, AttributeError):
            log.warning(f"Claude JSON parse hatasi, fallback 0 skor")
            text = "[]"

        for s in json.loads(text):
            idx = s.get("id", 0) - 1
            if 0 <= idx < len(haberler):
                mevcut   = haberler[idx].get("semboller", [])
                raw_skor = max(0, min(10, int(s.get("skor", 0))))
                # Petrol haberi ise +2
                if petrol_haberi_mi(haberler[idx].get("baslik","")):
                    raw_skor = min(10, raw_skor + 2)  # Petrol haberi +2 puan
                adj_skor = tier_skor_ayarla(raw_skor, haberler[idx].get("tier", 2))
                haberler[idx].update({
                    "skor":      adj_skor,
                    "yon":       s.get("yon", "NOTR"),
                    "semboller": mevcut if mevcut else s.get("semboller", []),
                    "ozet":      s.get("ozet", "")
                })
    except Exception as e:
        log.warning(f"Claude ({mod}): {e}")
    return haberler

# ================================================================
# MESAJ FORMATI — 3 Stream
# ================================================================
KAYNAK_IKONLARI = {
    "Reuters":     "Rtr",
    "AP News":     "AP",
    "BBC":         "BBC",
    "CNBC":        "CNBC",
    "ZeroHedge":   "ZH",
    "FED":         "FED",
    "ECB":         "ECB",
    "EIA":         "EIA",
    "OilPrice":    "OIL",
    "ISW":         "ISW",
    "Al Monitor":  "AM",
    "IAEA":        "IAEA",
    "NATO":        "NATO",
    "TCMB":        "TCMB",
    "SPK":         "SPK",
    "BDDK":        "BDDK",
    "Hazine":      "HAZ",
    "BTK":         "BTK",
    "AA":          "AA",
    "DHA":         "DHA",
    "Bloomberg HT":"BHT",
    "Dunya":       "DNYA",
    "Marketaux":   "MKT",
    "Finnhub":     "FNH",
    "Foreks":      "FRK",
}

STREAM_HEADER = {
    "MACRO":   "[MACRO]",
    "COMPANY": "[COMPANY]",
    "PETROL":  "[PETROL]",
    "GEO":     "[GEO]",
    "NEWS":    "[NEWS]",
}

def kaynak_kisa(kaynak):
    for k, v in KAYNAK_IKONLARI.items():
        if k.lower() in kaynak.lower():
            return v
    return kaynak[:6].upper()

def hash_h(t):
    return hashlib.md5(t.encode("utf-8", "ignore")).hexdigest()[:12]

def mesaj_olustur(h, mod, acil=False):
    skor   = h.get("skor", 0)
    yon    = h.get("yon", "NOTR")
    syms   = h.get("semboller", [])
    ozet   = h.get("ozet", "")
    nv     = h.get("novelty", 1.0)
    ml_s   = h.get("ml_skor", 0.5)
    sk_oz  = h.get("sektor_ozet", "")
    stream = h.get("stream", "NEWS")
    tier   = h.get("tier", 2)
    flag   = "TR" if mod == "turkey" else "GL"

    if acil or skor >= 9:
        seviye = "ACIL ALARM"
        bar    = "🔴🔴🔴"
    elif skor >= 8:
        seviye = "ONEMLI"
        bar    = "🟡🟡"
    elif skor >= 7:
        seviye = "TAKİP"
        bar    = "🔵"
    else:
        seviye = "BILGI"
        bar    = "⚪"

    yon_str    = {"BULLISH":"yukselis","BEARISH":"dusus",
                  "NOTR":"yatay","MIXED":"karisik"}.get(yon, yon)
    nb         = "YENİ" if nv >= 0.9 else ("GUNCELLENDI" if nv >= 0.5 else "TEKRAR")
    tier_badge = " [T1]" if tier == 1 else (" [T3]" if tier == 3 else "")
    kk         = kaynak_kisa(h["kaynak"])
    ss         = "  ".join([f"<code>{s}</code>" for s in syms[:5]])
    st_label   = STREAM_HEADER.get(stream, "[NEWS]")

    ml_line = ""
    if ml.trained and ml_s >= 0.7:
        ml_line = f"\nML: %{int(ml_s*100)} hareket ihtimali"
    elif ml.trained and ml_s >= 0.5:
        ml_line = f"\nML: %{int(ml_s*100)}"

    msg  = f"━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"{bar} <b>{seviye}</b>  [{skor}/10]  {st_label}  [{flag}]  {nb}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"<b>{kk}{tier_badge}</b>  ·  <code>{h['zaman']}</code>\n\n"
    msg += f"<b>{h['baslik']}</b>\n"
    if ozet:
        msg += f"\n{yon_str.upper()}  —  {ozet}\n"
    if sk_oz:
        msg += f"{sk_oz}\n"
    if ml_line:
        msg += ml_line + "\n"
    if ss:
        msg += f"\n{ss}\n"
    if h.get("url"):
        safe_url = html_escape(h['url'])
        msg += f"\n<a href='{safe_url}'>Habere Git</a>"
    # Market context — sadece onemli haberlerde (rate limit koruma)
    if skor >= 7:
        ctx = market_radar.market_context_line()
        if ctx:
            msg += f"\n\n<i>{ctx}</i>"
    return msg

# ================================================================
# SABAH BRIFING + AKSAM OZETI
# ================================================================
async def sabah_brifing(bot):
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        sinir  = (ist_now() - timedelta(hours=12)).isoformat()
        with db_connect() as con:
            rows = con.execute("""
                SELECT baslik, ai_skor, yon, semboller, stream
                FROM haberler WHERE gonderildi=1 AND timestamp > ?
                ORDER BY ai_skor DESC LIMIT 15
            """, (sinir,)).fetchall()

        bugun        = ist_now().strftime("%Y-%m-%d")
        takvim_str   = "Bugün onemli veri yok"
        try:
            r = fh_limiter.get(
                "https://finnhub.io/api/v1/calendar/economic",
                params={"from": bugun, "to": bugun, "token": FINNHUB_KEY},
                timeout=8
            ).json()
            evs = [
                f"- {ev['time'][11:16]} {ev['event']} [{ev.get('country','')}]"
                for ev in r.get("economicCalendar", [])
                if ev.get("impact") in ["high","medium"]
                and ev.get("country") in ["TR","US"]
            ]
            if evs:
                takvim_str = "\n".join(evs[:8])
        except Exception:
            pass

        gece_listesi = "\n".join([
            f"- [{r[1]}/10] [{r[4]}] {r[0]}"
            for r in rows[:10]
        ]) or "Gece sakin gecti."

        # Brent anlık
        brent_fiyat = brent_radar._brent_fiyat_cek()
        brent_str   = f"Brent: {brent_fiyat:.2f}" if brent_fiyat else ""

        prompt = (
            f"Sen BIST/VIOP uzmani Turk trader asistanisin. "
            f"Sabah 09:45 brifingini yaz. Kisa, net, aksiyon odakli. Turkce. Max 20 satir.\n\n"
            f"GECE HABERLERI:\n{gece_listesi}\n\n"
            f"BUGUN TAKVIM:\n{takvim_str}\n\n"
            f"BRENT: {brent_str}\n\n"
            f"Yaz:\n"
            f"GECE NE OLDU — 3-4 madde, her birinde hisse ticker\n"
            f"BUGUN TAKVIM — saatlerle\n"
            f"GOZETLEME — bugun dikkat edilecek 3 hisse + neden\n"
            f"GENEL YORUM — tek cumle piyasa tonu"
        )
        msg = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=800,
            messages=[{"role":"user","content":prompt}]
        )
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🌅 <b>SABAH BRIFING</b>  —  {ist_now().strftime('%d.%m.%Y')}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{msg.content[0].text.strip()}"
            ),
            parse_mode=ParseMode.HTML
        )
        log.info("Sabah brifing gonderildi")
    except Exception as e:
        log.warning(f"sabah_brifing: {e}")

async def aksam_ozeti(bot):
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        sinir  = ist_now().replace(hour=8, minute=0, second=0).isoformat()
        with db_connect() as con:
            rows   = con.execute("""
                SELECT baslik, ai_skor, yon, semboller, stream, feedback
                FROM haberler WHERE gonderildi=1 AND timestamp >= ?
                ORDER BY ai_skor DESC
            """, (sinir,)).fetchall()
            ml_r   = con.execute("""
                SELECT COUNT(*),
                       SUM(CASE WHEN relative_move > 0.3 THEN 1 ELSE 0 END)
                FROM haberler WHERE gonderildi=1 AND relative_move IS NOT NULL
                AND timestamp >= ?
            """, (sinir,)).fetchone()
            fb_rows = con.execute("""
                SELECT feedback, COUNT(*) FROM haberler
                WHERE feedback IS NOT NULL AND timestamp >= ?
                GROUP BY feedback
            """, (sinir,)).fetchall()

        acil_s   = sum(1 for r in rows if r[1] >= 9)
        onemli_s = sum(1 for r in rows if 8 <= r[1] < 9)
        takip_s  = sum(1 for r in rows if 7 <= r[1] < 8)
        ml_t     = ml_r[0] or 0
        ml_d     = ml_r[1] or 0
        ml_oran  = f"%{int(ml_d/ml_t*100)}" if ml_t > 0 else "yeterli veri yok"
        fb_str   = "  ".join([f"{k}:{v}" for k, v in fb_rows]) or "—"

        hisse_c = defaultdict(int)
        for r in rows:
            for s in json.loads(r[3] or "[]"):
                hisse_c[s] += 1
        top_h     = sorted(hisse_c.items(), key=lambda x: x[1], reverse=True)[:5]
        hisse_str = "  ".join([f"<code>{s}</code>({n})" for s, n in top_h]) or "—"
        top5      = "\n".join([
            f"- [{r[1]}/10] [{r[4]}] {r[0][:70]}" for r in rows[:5]
        ]) or "—"

        yarin        = (ist_now() + timedelta(days=1)).strftime("%Y-%m-%d")
        yarin_takvim = "—"
        try:
            r2 = fh_limiter.get(
                "https://finnhub.io/api/v1/calendar/economic",
                params={"from":yarin,"to":yarin,"token":FINNHUB_KEY},
                timeout=8
            ).json()
            evs = [
                f"- {e['time'][11:16]} {e['event']}"
                for e in r2.get("economicCalendar",[])
                if e.get("impact") == "high" and e.get("country") in ["TR","US"]
            ]
            if evs:
                yarin_takvim = "\n".join(evs[:5])
        except Exception:
            pass

        prompt = (
            f"Sen BIST/VIOP uzmani. Aksam ozeti yaz. Turkce. Max 12 satir.\n"
            f"EN ONEMLI HABERLER:\n{top5}\n"
            f"Yaz: genel degerlendirme, en kritik haber, yarin dikkat."
        )
        msg = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=500,
            messages=[{"role":"user","content":prompt}]
        )
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>GUNUN OZETI</b>  —  {ist_now().strftime('%d.%m.%Y')}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Bugün: <b>{len(rows)} haber</b>  "
                f"Acil:{acil_s}  Onemli:{onemli_s}  Takip:{takip_s}\n"
                f"En cok: {hisse_str}\n"
                f"ML dogruluk: {ml_oran} ({ml_t} olcum)\n"
                f"Feedback: {fb_str}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{msg.content[0].text.strip()}\n\n"
                f"<b>Yarin:</b>\n{yarin_takvim}"
            ),
            parse_mode=ParseMode.HTML
        )
        log.info("Aksam ozeti gonderildi")
    except Exception as e:
        log.warning(f"aksam_ozeti: {e}")

# ================================================================
# OGLEN BRIFING (13:00)
# ================================================================
async def oglen_brifing(bot):
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        sinir  = ist_now().replace(hour=9, minute=0, second=0).isoformat()
        with db_connect() as con:
            rows = con.execute("""
                SELECT baslik, ai_skor, yon, semboller, stream
                FROM haberler WHERE gonderildi=1 AND timestamp > ?
                ORDER BY ai_skor DESC LIMIT 10
            """, (sinir,)).fetchall()
        if not rows:
            return
        liste = "\n".join([f"- [{r[1]}/10] [{r[4]}] {r[0][:80]}" for r in rows[:8]])
        ctx = market_radar.market_context_line()
        prompt = (
            f"Sen BIST/VIOP uzmani. Oglen 13:00 ara rapor. Turkce, max 10 satir.\n"
            f"SABAHTAN BU YANA:\n{liste}\n\nPIYASA: {ctx}\n\n"
            f"Yaz: En sicak 5 hisse + neden. Ogleden sonra dikkat edilecekler."
        )
        msg = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=500,
            messages=[{"role":"user","content":prompt}]
        )
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"☀️ <b>OGLEN RAPORU</b>  —  {ist_now().strftime('%d.%m.%Y %H:%M')}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{msg.content[0].text.strip()}"
            ),
            parse_mode=ParseMode.HTML
        )
        log.info("Oglen brifing gonderildi")
    except Exception as e:
        log.warning(f"oglen_brifing: {e}")

# ================================================================
# FA PRE-CACHE — arka planda tum universe'u tara
# ================================================================

# ================================================================
# FEEDBACK LONG-POLL TASK — ayri task, surekli dinle
# ================================================================
# (feedback_kontrol artik ana dongude her iterasyonda cagrilir — ayri task 409 hatasi veriyordu)

# ================================================================
# ANA DONGU
# ================================================================
async def ana_dongu():
    db_init()
    dedup_yukle()
    bot            = Bot(token=TELEGRAM_TOKEN)
    telethon_aktif = False

    if TELETHON_SESSION and TELEGRAM_KANALLARI:
        try:
            tg = TelegramClient(
                StringSession(TELETHON_SESSION), TELEGRAM_API_ID, TELEGRAM_API_HASH
            )
            await tg.start()
            telethon_aktif = True

            @tg.on(tg_events.NewMessage(chats=TELEGRAM_KANALLARI))
            async def tg_handler(event):
                m = event.raw_text
                if m and len(m) > 15:
                    um = re.search(r'https?://\S+', m)
                    b  = m[:260].replace("\n", " ").strip()
                    h  = yeni_h(
                        f"TG:{getattr(event.chat,'title','Telegram')}",
                        b, um.group(0) if um else "", "telegram"
                    )
                    h["semboller"] = nov.entity_bul(b)
                    state.telegram_kuyruk.append(h)
        except Exception as e:
            log.warning(f"Telethon: {e}")

    await bot.send_message(
        chat_id=CHAT_ID,
        text=(
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🤖 <b>BERKAY TERMINATOR v3.3</b>\n"
            f"  {ist_now().strftime('%d.%m.%Y %H:%M')} Istanbul\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"

            "<b>📋 KOMUTLAR</b>\n"
            "<code>TCELL</code>  — ticker yaz, temel analiz gelir\n"
            "<code>/analiz THYAO</code>  — ayni sey\n"
            "<code>/top10</code>  — BIST en iyi 10 hisse\n"
            "<code>/radar</code>  — canli Brent + XU030 + USDTRY\n"
            "<code>/durum</code>  — sistem durumu\n"
            "<code>/heat THYAO</code>  — X (Twitter) heat skoru\n"
            "<code>/watch THYAO GARAN</code>  — watchlist ekle\n"
            "<code>/unwatch THYAO</code>  — cikar\n"
            "<code>/watchlist</code>  — listeyi gor\n"
            "<code>/help</code>  — bu menu\n\n"

            "<b>🔔 OTOMATİK ALARMLAR</b>\n"
            "Haber skoru 7+ → otomatik mesaj\n"
            "Brent %1.5+ → sari, %3+ → kirmizi\n"
            "DXY/VIX/XU030/USDTRY radar\n"
            f"X Sentiment {'aktif' if TWSCRAPE_AVAILABLE else 'pasif (twscrape yukle)'}\n"
            "Momentum (15dk 3+ haber → alarm)\n"
            "Macro takvim (30dk once uyari)\n\n"

            "<b>📊 ZAMANLI RAPORLAR</b>\n"
            "09:45  Sabah brifing\n"
            "13:00  Oglen rapor\n"
            "17:45  Aksam ozet + yarin\n"
            "6 saatte bir  Brent teknik\n\n"

            "<b>⚙️ MOTORLAR</b>\n"
            f"RSS: GL{len(RSS_GLOBAL)} + TR{len(RSS_TURKEY)} kaynak\n"
            f"ML: {'Model yuklendi' if ml.trained else 'Birikim'}\n"
            "FA botu ayri calisiyor\n"
            "Dinamik esik (sessizse otomatik duser)\n"
            f"TZ: Istanbul | AI: {CLAUDE_MODEL}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Izleme basladi..."
        ),
        parse_mode=ParseMode.HTML
    )
    log.info("TERMINATOR v3.3 BASLADI!")



    while True:
        try:
            now   = time.time()
            simdi = ist_now()
            state.dongu_sayac += 1

            loop  = asyncio.get_running_loop()
            g_raw = await loop.run_in_executor(None, lambda: rss_cek(RSS_GLOBAL, max_age_hours=4))
            g_raw += await loop.run_in_executor(None, finnhub_genel_cek)
            t_raw = await loop.run_in_executor(None, lambda: rss_cek(RSS_TURKEY, max_age_hours=12))
            t_raw += await loop.run_in_executor(None, marketaux_cek)

            if state.dongu_sayac % 4 == 0:
                sirket  = await loop.run_in_executor(None, sirket_haberleri_cek)
                sirket += await loop.run_in_executor(None, finnhub_bist_cek)
                t_raw  += sirket
                if sirket:
                    log.info(f"{len(sirket)} sirket haberi")

            if state.telegram_kuyruk:
                t_raw += state.telegram_kuyruk[:30]
                state.telegram_kuyruk.clear()

            acil_kuyruk = []

            def isle(haberler, kuyruk, mod):
                for h in haberler:
                    hsh = hash_h(h["baslik"])
                    if (hsh in gonderilen or
                            not h["baslik"] or
                            len(h["baslik"]) < 10):
                        continue
                    if event_tekrar_mi(h["baslik"]):
                        continue
                    gonderilen.add(hsh)
                    dedup_kaydet(hsh)
                    sm_syms, sm_yon, sm_oz = sektor_analiz(h["baslik"])
                    if not h["semboller"]:
                        h["semboller"] = nov.entity_bul(h["baslik"])
                    for s in sm_syms:
                        if s not in h["semboller"]:
                            h["semboller"].append(s)
                    h["semboller"] = h["semboller"][:5]
                    if sm_oz:
                        h["sektor_ozet"] = sm_oz
                    if sm_yon != "NOTR" and h["yon"] == "NOTR":
                        h["yon"] = sm_yon
                    h["stream"]  = stream_belirle(h)
                    ek           = h["semboller"][0] if h["semboller"] else "GENEL"
                    h["novelty"] = nov.skorla(ek, h["baslik"])
                    h["ml_skor"] = ml.skor(h)
                    momentum.ekle(h["baslik"])
                    if acil_mi(h["baslik"], mod):
                        h["skor"] = 9
                        acil_kuyruk.append((h, mod))
                    else:
                        kuyruk.append(h)

            isle(g_raw, state.bekl_global, "global")
            isle(t_raw, state.bekl_turkey, "turkey")

            # ACİL — hemen gonder
            if acil_kuyruk:
                log.info(f"ACIL: {len(acil_kuyruk)} haber")
                for h, mod in acil_kuyruk:
                    try:
                        haber_id = db_kaydet(h, mod, gonderildi_flag=1)
                        sent     = await bot.send_message(
                            chat_id=CHAT_ID,
                            text=mesaj_olustur(h, mod, acil=True),
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                            reply_markup=feedback_keyboard(haber_id)
                        )
                        db_msg_id_guncelle(haber_id, sent.message_id)
                        state.toplam_gonderilen += 1
                        await asyncio.sleep(1)
                    except Exception as ex:
                        log.error(f"Acil gonderme: {ex}")

            toplam = len(state.bekl_global) + len(state.bekl_turkey)
            if toplam > 0:
                log.info(f"Kuyruk: GL{len(state.bekl_global)} | TR{len(state.bekl_turkey)}")

            # AI skorlama — dinamik esik
            yeterli = len(state.bekl_global) >= 5 or len(state.bekl_turkey) >= 5
            if toplam > 0 and (yeterli or (now - state.son_ai) >= AI_INTERVAL):
                state.son_ai = now
                gonderilenler = []

                if state.bekl_global:
                    sk = claude_skore_et(state.bekl_global[:20], "global")
                    for h in sk:
                        h["fs"] = (h.get("skor",0) *
                                   h.get("novelty",1.0) *
                                   (1 + h.get("ml_skor",0.5)))
                        # Watchlist bonus: esik -1
                        if watchlist.haberde_var_mi(h.get("semboller",[])):
                            h["fs"] *= 1.5
                    gonderilenler += [
                        (h, "global") for h in
                        sorted(
                            [h for h in sk if h.get("skor",0) >= state.global_thresh
                             or (watchlist.haberde_var_mi(h.get("semboller",[])) and h.get("skor",0) >= max(5, state.global_thresh - 2))],
                            key=lambda x: x["fs"], reverse=True
                        )[:MAX_GLOBAL]
                    ]
                    state.bekl_global = []

                if state.bekl_turkey:
                    sk = claude_skore_et(state.bekl_turkey[:25], "turkey")
                    for h in sk:
                        h["fs"] = (h.get("skor",0) *
                                   h.get("novelty",1.0) *
                                   (1 + h.get("ml_skor",0.5)))
                        if watchlist.haberde_var_mi(h.get("semboller",[])):
                            h["fs"] *= 1.5
                    gonderilenler += [
                        (h, "turkey") for h in
                        sorted(
                            [h for h in sk if h.get("skor",0) >= state.turkey_thresh
                             or (watchlist.haberde_var_mi(h.get("semboller",[])) and h.get("skor",0) >= max(5, state.turkey_thresh - 2))],
                            key=lambda x: x["fs"], reverse=True
                        )[:MAX_TURKEY]
                    ]
                    state.bekl_turkey = []

                gonderilenler.sort(key=lambda x: x[0].get("fs",0), reverse=True)

                # Dinamik esik guncelle
                state.dinamik_esik_guncelle(gonderilenler)

                if gonderilenler:
                    log.info(f"Gonderiliyor: {len(gonderilenler)} haber (TR_esik={state.turkey_thresh} GL_esik={state.global_thresh})")
                    for h, mod in gonderilenler:
                        try:
                            haber_id = db_kaydet(h, mod, gonderildi_flag=1)
                            sent     = await bot.send_message(
                                chat_id=CHAT_ID,
                                text=mesaj_olustur(h, mod),
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True,
                                reply_markup=feedback_keyboard(haber_id)
                            )
                            db_msg_id_guncelle(haber_id, sent.message_id)
                            state.toplam_gonderilen += 1
                            log.info(
                                f"[{h['skor']}][{h.get('stream','?')}] "
                                f"{h['baslik'][:65]}"
                            )
                            await asyncio.sleep(1.5)
                        except Exception as ex:
                            log.error(f"Gonderme: {ex}")
                else:
                    log.info(f"Esik yok (TR={state.turkey_thresh} GL={state.global_thresh})")

            # Dinamik esik — her dongude kontrol (AI scoring disinda da dusebilsin)
            state.dinamik_esik_guncelle([])

            # Brent fiyat kontrolu (her 2 dk)
            if (now - state.son_brent_fiyat) >= PRICE_INTERVAL:
                state.son_brent_fiyat = now
                await brent_radar.fiyat_kontrol(bot)

            # Brent teknik (her 6 saat)
            if (now - state.son_brent_teknik) >= BRENT_TEKNIK_INTERVAL:
                state.son_brent_teknik = now
                await brent_radar.teknik_gonder(bot)

            # Market radar — DXY/VIX/XU030/USDTRY (her 3 dk)
            if (now - state.son_radar) >= 180:
                state.son_radar = now
                try:
                    alarms = await loop.run_in_executor(None, market_radar.kontrol)
                    for inst, cfg, fiyat, baz, degisim, seviye, yon in alarms:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=market_radar.alarm_mesaj(inst, cfg, fiyat, baz, degisim, seviye, yon),
                            parse_mode=ParseMode.HTML
                        )
                except Exception as ex:
                    log.debug(f"MarketRadar: {ex}")

            # X Sentiment (her 3 dk)
            if TWSCRAPE_AVAILABLE and (now - x_sentiment.last_scan) >= x_sentiment.SCAN_INTERVAL:
                try:
                    await x_sentiment.heat_kontrol(bot)
                except Exception as ex:
                    log.debug(f"XSentiment: {ex}")

            # Macro engine (her 60 sn)
            if (now - state.son_macro) >= MACRO_INTERVAL:
                state.son_macro = now
                await macro_engine.kontrol(bot)

            # Momentum alarmlar
            for kume_adi, n, son_h in momentum.kontrol():
                try:
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=momentum.alarm_mesaj(kume_adi, n, son_h),
                        parse_mode=ParseMode.HTML
                    )
                    log.info(f"Momentum: {kume_adi} ({n})")
                except Exception as ex:
                    log.error(f"Momentum: {ex}")

            # ML veri guncelle (her 20 dk)
            if (now - state.son_ml_guncelle) >= 1200:
                state.son_ml_guncelle = now
                await asyncio.get_running_loop().run_in_executor(
                    None, ml.fiyat_guncelle, [("move_5m",5), ("move_15m",15), ("move_60m",60)]
                )
                if (ml.son_egitim is None or
                        (simdi - ml.son_egitim).days >= 7):
                    await asyncio.get_running_loop().run_in_executor(None, ml.egit)



            # Sabah brifing (09:45)
            if simdi.hour == 9 and simdi.minute == 45:
                bugun_str = simdi.strftime("%Y%m%d")
                if state.son_sabah != bugun_str:
                    state.son_sabah = bugun_str
                    await sabah_brifing(bot)

            # Oglen brifing (13:00)
            if simdi.hour == 13 and simdi.minute == 0:
                bugun_str = simdi.strftime("%Y%m%d")
                if state.son_oglen != bugun_str:
                    state.son_oglen = bugun_str
                    await oglen_brifing(bot)

            # Aksam ozeti (17:45)
            if simdi.hour == 17 and simdi.minute == 45:
                bugun_str = simdi.strftime("%Y%m%d")
                if state.son_aksam != bugun_str:
                    state.son_aksam = bugun_str
                    await aksam_ozeti(bot)

            # 6 saatlik sistem raporu
            if (simdi - state.son_durum).total_seconds() > 21600:
                state.son_durum = simdi
                try:
                    with db_connect() as con:
                        tk   = con.execute("SELECT COUNT(*) FROM haberler").fetchone()[0]
                        ml_v = con.execute("SELECT COUNT(*) FROM haberler WHERE relative_move IS NOT NULL").fetchone()[0]
                        fb_c = con.execute("SELECT COUNT(*) FROM haberler WHERE feedback IS NOT NULL").fetchone()[0]
                except Exception:
                    tk, ml_v, fb_c = 0, 0, 0
                brent_su_an = brent_radar._brent_fiyat_cek()
                brent_str   = f"Brent: {brent_su_an:.2f}" if brent_su_an else "Brent: —"
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=(
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📊 <b>SISTEM RAPORU</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"Calisma: {int((simdi-state.baslangic).total_seconds()//3600)} saat\n"
                        f"Gonderilen: {state.toplam_gonderilen} haber\n"
                        f"DB kayit: {tk}\n"
                        f"ML verisi: {ml_v}/{ml.MIN_VERI} | {'Aktif' if ml.trained else 'Birikim'}\n"
                        f"Feedback: {fb_c} | Esik: TR={state.turkey_thresh} GL={state.global_thresh}\n"
                        f"{brent_str}\n"
                        f"Watchlist: {', '.join(watchlist.liste()[:5]) or '—'}\n"
                        f"{simdi.strftime('%d.%m.%Y %H:%M')}"
                    ),
                    parse_mode=ParseMode.HTML
                )

            # Hash temizle
            if len(gonderilen) > 10000:
                dedup_yukle()
                log.info(f"Hash seti DB'den yenilendi: {len(gonderilen)}")

            # Feedback + komut kontrol (her dongude — TCELL, /durum, /radar vs.)
            await feedback_kontrol(bot)

        except Exception as e:
            log.error(f"Dongu hatasi: {e}")
            await asyncio.sleep(30)

        await asyncio.sleep(POLL_INTERVAL)


asyncio.run(ana_dongu())
