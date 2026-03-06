╔══════════════════════════════════════════════════════╗
║        BERKAY TERMINATOR v3.1 — RAILWAY              ║
║  Macro Engine · Event Dedup · Source Tiering         ║
║  Feedback Buttons · Multi-Horizon ML · 3-Stream      ║
╚══════════════════════════════════════════════════════╝

v3.1 YENİLİKLER:
  ✅ Kalıcı dedup — restart'ta hash'ler SQLite'tan yüklenir
  ✅ Timestamp fix — isoformat, tüm DB sorgular doğru çalışır
  ✅ Source tiering — TCMB/KAP/Reuters ayrı puanlanır
  ✅ Event ID — aynı olaydan 20dk içinde gelen haberler tek event
  ✅ Macro Engine — Finnhub calendar + 30dk önce + veri anı + 5dk tepki
  ✅ Surprise score — (actual-forecast)/abs(forecast)
  ✅ Feedback butonları — ✅❌⏰ her mesaja, SQLite'a kayıt
  ✅ 3 stream — MACRO / COMPANY / GEO mesaj formatında ayrım
  ✅ Multi-horizon ML — 5m/15m/60m/close + relative_move
  ✅ TR full takvim — TCMB/TÜFE/cari/büyüme + ABD core
"""

import asyncio, feedparser, requests, hashlib, json, logging
import time, re, os, random, sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
from bs4 import BeautifulSoup
import anthropic
from telethon import TelegramClient, events as tg_events
from telethon.sessions import StringSession
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

try:
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ════════════════════════════════════════════════
# 🔑 KEY'LER
# ════════════════════════════════════════════════
TELEGRAM_TOKEN    = os.environ["TELEGRAM_TOKEN"]
CHAT_ID           = os.environ["CHAT_ID"]
ANTHROPIC_KEY     = os.environ["ANTHROPIC_KEY"]
FINNHUB_KEY       = os.environ["FINNHUB_KEY"]
MARKETAUX_KEY     = os.environ["MARKETAUX_KEY"]
TELEGRAM_API_ID   = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELETHON_SESSION  = os.environ.get("TELETHON_SESSION", "")

# ════════════════════════════════════════════════
# ⚙️ AYARLAR
# ════════════════════════════════════════════════
POLL_INTERVAL    = 12
AI_INTERVAL      = 35
MAX_GLOBAL       = 3
MAX_TURKEY       = 8
TURKEY_THRESH    = 7
GLOBAL_THRESH    = 8
NOVELTY_HOURS    = 24
PRICE_INTERVAL   = 300
MACRO_INTERVAL   = 60
FEEDBACK_INTERVAL= 60

TELEGRAM_KANALLARI = []

# ════════════════════════════════════════════════
# 📊 BIST30 + ENTİTY
# ════════════════════════════════════════════════
BIST30 = [
    "THYAO","GARAN","AKBNK","ISCTR","YKBNK","TUPRS","EREGL","ASELS",
    "KCHOL","SAHOL","SISE","TOASO","FROTO","PGSUS","KOZAL","EKGYO",
    "BIMAS","MGROS","ULKER","TCELL","TTKOM","ARCLK","VESTL","DOHOL",
    "PETKM","AYGAZ","ENKAI","TAVHL","LOGO","SOKM"
]

ENTITY_DICT = {
    "THYAO": ["türk hava yolları","thy","turkish airlines"],
    "GARAN": ["garanti","garanti bbva"],
    "AKBNK": ["akbank"],
    "ISCTR": ["iş bankası","işbank","isbank"],
    "YKBNK": ["yapı kredi","yapi kredi"],
    "TUPRS": ["tüpraş","tupras"],
    "EREGL": ["ereğli","erdemir","eregli","isdemir"],
    "ASELS": ["aselsan"],
    "KCHOL": ["koç holding","koç grubu"],
    "SAHOL": ["sabancı holding","sabancı"],
    "SISE":  ["şişecam","sisecam"],
    "TOASO": ["tofaş","tofas"],
    "FROTO": ["ford otosan"],
    "PGSUS": ["pegasus"],
    "TCELL": ["turkcell"],
    "TTKOM": ["türk telekom","turk telekom","ttnet"],
    "BIMAS": ["bim","bim mağazaları"],
    "MGROS": ["migros"],
    "ARCLK": ["arçelik","arcelik","beko"],
    "PETKM": ["petkim"],
    "ENKAI": ["enka inşaat","enka"],
    "TAVHL": ["tav havalimanları","tav airports"],
    "KOZAL": ["koza altın","koza anadolu"],
    "EKGYO": ["emlak konut","emlak gyo"],
    "ULKER": ["ülker","ulker"],
    "VESTL": ["vestel"],
    "DOHOL": ["doğuş holding"],
    "AYGAZ": ["aygaz"],
    "LOGO":  ["logo yazılım"],
    "SOKM":  ["şok market","şok mağazaları"],
}

# ════════════════════════════════════════════════
# 🏆 SOURCE TIERING
# ════════════════════════════════════════════════
TIER1_SOURCES = [
    "tcmb","spk","bddk","hazine","resmi gazete","borsa istanbul",
    "kap","btk","epdk","ssb","rekabet kurumu","borsaistanbul"
]
TIER2_SOURCES = [
    "reuters","bloomberg","ap news","fed","ecb","iaea","nato",
    "abd dışişleri","aa ekonomi","aa gündem","dha","bia","isw",
    "al monitor","finnhub","marketaux","usgs"
]

def kaynak_tier(kaynak: str) -> int:
    k = kaynak.lower()
    if any(s in k for s in TIER1_SOURCES): return 1
    if any(s in k for s in TIER2_SOURCES): return 2
    return 3

def tier_skor_ayarla(skor: int, tier: int) -> int:
    if tier == 1: return min(10, skor + 1)
    if tier == 3: return max(0, skor - 1)
    return skor

# ════════════════════════════════════════════════
# 🎯 SEKTÖR MATRİSİ
# ════════════════════════════════════════════════
SEKTOR_MATRISI = [
    (["petrol fiyat arttı","brent yükseldi","opec kesinti","hürmüz riski","iran petrol ambargo"],
     ["TUPRS","PETKM","AYGAZ"], "BEARISH", "Petrol maliyeti artar → marj baskısı"),
    (["petrol düştü","brent geriledi","opec üretim arttı"],
     ["TUPRS","PETKM","AYGAZ"], "BULLISH", "Petrol düşer → marj genişler"),
    (["savunma ihalesi","ssb sözleşme","siha","insansız hava","füze sözleşme"],
     ["ASELS"], "BULLISH", "Savunma siparişi → gelir artışı"),
    (["savaş ilan","kara harekâtı","bombardıman başladı","çatışma tırmandı"],
     ["ASELS","KOZAL"], "MIXED", "Jeopolitik risk → güvenli liman"),
    (["altın yükseldi","altın rekor","gold rally","xau yükseldi"],
     ["KOZAL"], "BULLISH", "Altın fiyatı → gelir artar"),
    (["altın düştü","gold drops","altın geriledi"],
     ["KOZAL"], "BEARISH", "Altın düşer → gelir azalır"),
    (["çelik fiyat arttı","demir fiyat arttı","hrc arttı","steel up"],
     ["EREGL"], "BULLISH", "Çelik fiyatı artar → karlılık artar"),
    (["çelik düştü","steel drops","demir düştü"],
     ["EREGL"], "BEARISH", "Çelik düşer → karlılık azalır"),
    (["faiz artırdı","faiz artış sürprizi","tcmb sıkılaştı","baz puan artış"],
     ["GARAN","AKBNK","ISCTR","YKBNK"], "BEARISH", "Faiz artışı → banka marjı sıkışır"),
    (["faiz indirdi","tcmb gevşedi","baz puan indirim","faiz indirim"],
     ["GARAN","AKBNK","ISCTR","YKBNK"], "BULLISH", "Faiz indirimi → banka karlılığı artar"),
    (["dolar sert yükseldi","kur tırmandı","tl değer kaybetti","tl çöktü"],
     ["THYAO","FROTO","EREGL","TUPRS"], "BULLISH", "TL zayıflar → ihracatçı kazanır"),
    (["dolar sert yükseldi","kur tırmandı","tl değer kaybetti"],
     ["BIMAS","MGROS","ARCLK"], "BEARISH", "TL zayıflar → ithalatçı maliyet artar"),
    (["turizm rekoru","turist sayısı arttı","yolcu rekoru"],
     ["THYAO","PGSUS","TAVHL"], "BULLISH", "Turizm güçlü → yolcu geliri artar"),
    (["büyük deprem","şiddetli deprem","yıkıcı deprem"],
     ["EKGYO","ENKAI"], "BEARISH", "Deprem → inşaat/GYO riski"),
    (["ihracat rekoru","dış ticaret fazlası","ihracat güçlü"],
     ["FROTO","TOASO","EREGL","ARCLK"], "BULLISH", "İhracat güçlü → döviz geliri artar"),
]

def sektor_analiz(baslik: str):
    b = baslik.lower()
    hisseler, yon, ozet = [], "NÖTR", ""
    for keywords, syms, _yon, _oz in SEKTOR_MATRISI:
        if any(kw in b for kw in keywords):
            for s in syms:
                if s not in hisseler: hisseler.append(s)
            if not ozet: yon, ozet = _yon, _oz
    return hisseler[:5], yon, ozet

# ════════════════════════════════════════════════
# 🚨 ACİL KEYWORD'LER (Spesifik)
# ════════════════════════════════════════════════
ACIL_TR = [
    "tcmb acil toplantı","merkez bankası acil","olağanüstü para kurulu",
    "faiz kararı açıklandı","faiz kararı bugün",
    "büyük deprem","şiddetli deprem","depremin büyüklüğü 6","depremin büyüklüğü 7",
    "iflas başvurusu","konkordato ilan","spk işlem durdurdu",
    "türkiye'ye yaptırım","yaptırım paketi türkiye",
    "sıkıyönetim ilan","olağanüstü hal ilan","sokağa çıkma yasağı",
    "tl kriz","kur 40","kur 45","lira çöktü",
    "bedelsiz sermaye artırımı","temettü dağıtım tarihi","pay geri alım programı",
    "satın alma anlaşması imzalandı","devralma tamamlandı","birleşme onayı",
]
ACIL_GLOBAL = [
    "fed emergency meeting","emergency rate cut",
    "war declared","nuclear strike","nato article 5 invoked",
    "strait of hormuz closed","oil embargo","opec emergency",
    "market circuit breaker","trading halted nyse",
    "sanctions on turkey","iran nuclear deal collapse",
    "sovereign default","imf emergency bailout",
]

def acil_mi(baslik: str, mod: str) -> bool:
    b = baslik.lower()
    return any(kw in b for kw in (ACIL_TR if mod == "turkey" else ACIL_GLOBAL))

# ════════════════════════════════════════════════
# 📡 EVENT ID — Aynı olaydan spam engeli
# ════════════════════════════════════════════════
EVENT_CLUSTERS = {
    "mideast":     ["israel","iran","tel aviv","tehran","idf","hamas","hizbullah","gaza"],
    "ukraine":     ["ukrayna","rusya","ukraine","russia","putin","zelensky","kyiv"],
    "fed_rate":    ["fomc","powell","federal reserve rate decision","fed rate"],
    "tcmb_rate":   ["tcmb faiz","ppk kararı","para politikası toplantı"],
    "oil_shock":   ["brent +","petrol fiyat arttı","opec acil","hürmüz kapandı"],
    "tl_crisis":   ["tl kriz","dolar tavan","kur +","lira çöküş"],
    "earthquake":  ["büyük deprem","şiddetli deprem","earthquake m","sismik"],
    "china":       ["çin ekonomi","china gdp","pboc","yuan devaluation"],
}

event_log: dict[str, list] = defaultdict(list)  # event_id -> [(baslik, dt)]
EVENT_WINDOW_MIN = 20

def event_id_bul(baslik: str) -> str | None:
    b = baslik.lower()
    for eid, keywords in EVENT_CLUSTERS.items():
        if any(kw in b for kw in keywords):
            return eid
    return None

def event_tekrar_mi(baslik: str) -> bool:
    eid = event_id_bul(baslik)
    if not eid: return False
    sinir = datetime.now() - timedelta(minutes=EVENT_WINDOW_MIN)
    event_log[eid] = [(b, t) for b, t in event_log[eid] if t > sinir]
    duplicate = len(event_log[eid]) >= 2
    event_log[eid].append((baslik, datetime.now()))
    return duplicate

# ════════════════════════════════════════════════
# 🗂️ STREAM BELIRLEME
# ════════════════════════════════════════════════
def stream_belirle(h: dict) -> str:
    b   = h["baslik"].lower()
    k   = h["kaynak"].lower()
    tip = h.get("kaynak_tip", "")
    t   = h.get("tier", 2)

    if t == 1 and any(s in k for s in ["tcmb","spk","bddk","hazine","resmi"]):
        return "MACRO"
    if tip == "sirket" or any(kw in b for kw in ["kap ","temettü","bedelsiz","geri alım","kontrat","sözleşme imza","bilanço"]):
        return "COMPANY"
    if any(kw in b for kw in ["petrol","brent","altın","gold","savaş","war","füze","deprem","earthquake","jeopolitik","opec","hürmüz"]):
        return "GEO"
    return "NEWS"

# ════════════════════════════════════════════════
# 🗄️ SQLite — Tam schema
# ════════════════════════════════════════════════
DB_PATH = "/app/terminator.db"

def db_init():
    con = sqlite3.connect(DB_PATH)
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
        yon             TEXT DEFAULT 'NÖTR',
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
        symbols     TEXT,
        rx_usdtry   REAL,
        rx_xu030    REAL
    );
    CREATE TABLE IF NOT EXISTS fiyat_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp   TEXT,
        sembol      TEXT,
        fiyat       REAL
    );
    """)
    con.commit()
    con.close()

# Kalıcı dedup — startup'ta yükle
gonderilen: set[str] = set()

def dedup_yukle():
    global gonderilen
    try:
        sinir = (datetime.now() - timedelta(hours=24)).isoformat()
        con   = sqlite3.connect(DB_PATH)
        rows  = con.execute(
            "SELECT hash FROM dedup_hashes WHERE timestamp > ?", (sinir,)
        ).fetchall()
        con.close()
        gonderilen = {r[0] for r in rows}
        log.info(f"📂 Dedup: {len(gonderilen)} hash yüklendi")
    except Exception as e:
        log.warning(f"dedup_yukle: {e}")

def dedup_kaydet(h: str):
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute(
            "INSERT OR REPLACE INTO dedup_hashes (hash,timestamp) VALUES (?,?)",
            (h, datetime.now().isoformat())
        )
        con.commit()
        con.close()
    except: pass

def db_kaydet(h: dict, mod: str, gonderildi: int = 0) -> int:
    """Haberi DB'ye kaydet, ID döndür (feedback için gerekli)"""
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.execute("""
            INSERT INTO haberler
            (event_id,timestamp,stream,kaynak,tier,baslik,url,
             ai_skor,ml_skor,yon,semboller,novelty,mod,gonderildi,
             scheduled_event,surprise)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            event_id_bul(h.get("baslik","")),
            datetime.now().isoformat(),
            h.get("stream","NEWS"),
            h.get("kaynak",""),
            h.get("tier", 2),
            h.get("baslik",""),
            h.get("url",""),
            h.get("skor", 0),
            h.get("ml_skor", 0.5),
            h.get("yon","NÖTR"),
            json.dumps(h.get("semboller",[])),
            h.get("novelty", 1.0),
            mod,
            gonderildi,
            1 if h.get("scheduled_event") else 0,
            h.get("surprise"),
        ))
        haber_id = cur.lastrowid
        con.commit()
        con.close()
        return haber_id
    except Exception as e:
        log.debug(f"db_kaydet: {e}")
        return 0

def db_msg_id_guncelle(haber_id: int, msg_id: int):
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("UPDATE haberler SET telegram_msg_id=? WHERE id=?", (msg_id, haber_id))
        con.commit()
        con.close()
    except: pass

# ════════════════════════════════════════════════
# 💬 FEEDBACK BUTONU
# ════════════════════════════════════════════════
def feedback_keyboard(haber_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ İşe yaradı", callback_data=f"fb_good_{haber_id}"),
        InlineKeyboardButton("❌ Gürültü",    callback_data=f"fb_noise_{haber_id}"),
        InlineKeyboardButton("⏰ Geç kaldı",  callback_data=f"fb_late_{haber_id}"),
    ]])

update_offset = 0

async def feedback_kontrol(bot: Bot):
    global update_offset
    try:
        updates = await bot.get_updates(
            offset=update_offset, timeout=0,
            allowed_updates=["callback_query"]
        )
        for upd in updates:
            update_offset = upd.update_id + 1
            cq = upd.callback_query
            if not cq: continue
            await bot.answer_callback_query(callback_query_id=cq.id, text="✅ Kaydedildi")
            parts = cq.data.split("_")
            if len(parts) == 3 and parts[0] == "fb":
                fb_type  = parts[1]  # good/noise/late
                haber_id = int(parts[2])
                con = sqlite3.connect(DB_PATH)
                con.execute("UPDATE haberler SET feedback=? WHERE id=?", (fb_type, haber_id))
                con.commit()
                con.close()
                fb_emoji = {"good":"✅","noise":"❌","late":"⏰"}.get(fb_type,"?")
                log.info(f"Feedback {fb_emoji} → haber #{haber_id}")
    except Exception as e:
        log.debug(f"feedback_kontrol: {e}")

# ════════════════════════════════════════════════
# 🤖 ML MODELİ — Multi-horizon labels
# ════════════════════════════════════════════════
class MLModel:
    def __init__(self):
        self.model      = None
        self.trained    = False
        self.son_egitim = None
        self.MIN_VERI   = 100

    def ozellik_cikar(self, h: dict) -> list:
        b = h.get("baslik","").lower()
        t = h.get("tier", 2)
        return [
            h.get("skor", 0) / 10.0,
            h.get("novelty", 1.0),
            len(h.get("semboller",[])) / 5.0,
            1.0 if h.get("yon")=="BULLISH" else (-1.0 if h.get("yon")=="BEARISH" else 0.0),
            1.0 if h.get("kaynak_tip")=="sirket" else 0.0,
            (3 - t) / 2.0,  # tier feature: Tier1=1.0, Tier2=0.5, Tier3=0.0
            1.0 if any(k in b for k in ["tcmb","merkez bankası","faiz"]) else 0.0,
            1.0 if any(k in b for k in ["deprem","afet","patlama"]) else 0.0,
            1.0 if any(k in b for k in ["temettü","bedelsiz","geri alım","kap"]) else 0.0,
            1.0 if any(k in b for k in ["savaş","operasyon","füze","nato"]) else 0.0,
            1.0 if any(k in b for k in ["petrol","brent","opec","enerji"]) else 0.0,
            1.0 if any(k in b for k in ["dolar","kur","tl","döviz"]) else 0.0,
            1.0 if any(k in b for k in ["fed","ecb","faiz kararı","baz puan"]) else 0.0,
            1.0 if any(k in b for k in ["altın","gold","xau"]) else 0.0,
            1.0 if h.get("scheduled_event") else 0.0,
            min(h.get("surprise", 0) or 0, 1.0),
            min(len(h.get("baslik","")) / 200.0, 1.0),
        ]

    def egit(self):
        if not ML_AVAILABLE: return
        try:
            con  = sqlite3.connect(DB_PATH)
            rows = con.execute("""
                SELECT ai_skor,novelty,semboller,yon,baslik,tier,scheduled_event,
                       surprise,relative_move
                FROM haberler
                WHERE gonderildi=1 AND relative_move IS NOT NULL
            """).fetchall()
            con.close()
            if len(rows) < self.MIN_VERI:
                log.info(f"🤖 ML: {len(rows)}/{self.MIN_VERI} — birikim devam")
                return
            X, y = [], []
            for r in rows:
                h = {
                    "skor":r[0],"novelty":r[1],"semboller":json.loads(r[2] or "[]"),
                    "yon":r[3],"baslik":r[4],"tier":r[5] or 2,
                    "scheduled_event":r[6],"surprise":r[7]
                }
                X.append(self.ozellik_cikar(h))
                y.append(1 if (r[8] or 0) > 0.3 else 0)
            self.model = GradientBoostingClassifier(
                n_estimators=80, max_depth=3, learning_rate=0.1, random_state=42
            )
            self.model.fit(X, y)
            self.trained    = True
            self.son_egitim = datetime.now()
            acc = sum(1 for i,xi in enumerate(X) if self.model.predict([xi])[0]==y[i]) / len(y)
            log.info(f"🤖 ML eğitildi! {len(rows)} örnek | acc={acc:.2f}")
        except Exception as e:
            log.warning(f"ML egit: {e}")

    def skor(self, h: dict) -> float:
        if not self.trained or not ML_AVAILABLE: return 0.5
        try:
            return round(self.model.predict_proba([self.ozellik_cikar(h)])[0][1], 3)
        except: return 0.5

    def fiyat_guncelle(self, horizons: list[tuple[str, int]]):
        """
        horizons: [("move_5m", 5), ("move_15m", 15), ("move_60m", 60)]
        Her horizon için gönderilmiş haberleri çek, fiyat değişimini kaydet
        """
        if not ML_AVAILABLE: return
        for field, minutes in horizons:
            try:
                sinir_ust = (datetime.now() - timedelta(minutes=minutes-2)).isoformat()
                sinir_alt = (datetime.now() - timedelta(minutes=minutes+5)).isoformat()
                con  = sqlite3.connect(DB_PATH)
                rows = con.execute(f"""
                    SELECT id,semboller FROM haberler
                    WHERE gonderildi=1 AND {field} IS NULL
                    AND timestamp BETWEEN ? AND ?
                    LIMIT 20
                """, (sinir_alt, sinir_ust)).fetchall()
                con.close()
                for haber_id, semboller_json in rows:
                    syms = json.loads(semboller_json or "[]")
                    if not syms: continue
                    try:
                        r = requests.get(
                            "https://finnhub.io/api/v1/quote",
                            params={"symbol":f"XIST:{syms[0]}","token":FINNHUB_KEY},
                            timeout=5
                        ).json()
                        dp = r.get("dp")  # % değişim
                        if dp is None: continue
                        # Relative move = hisse - xu030 (yaklaşık)
                        xu030 = requests.get(
                            "https://finnhub.io/api/v1/quote",
                            params={"symbol":"XIST:XU030","token":FINNHUB_KEY},
                            timeout=5
                        ).json().get("dp", 0) or 0
                        relative = dp - xu030
                        con2 = sqlite3.connect(DB_PATH)
                        con2.execute(
                            f"UPDATE haberler SET {field}=?, relative_move=? WHERE id=?",
                            (dp, relative, haber_id)
                        )
                        con2.commit()
                        con2.close()
                    except: pass
            except Exception as e:
                log.debug(f"fiyat_guncelle {field}: {e}")

ml = MLModel()

# ════════════════════════════════════════════════
# 💹 FİYAT RADAR
# ════════════════════════════════════════════════
class FiyatRadar:
    def __init__(self):
        self.onceki  = {}
        self.ESIKLER = {"USDTRY": 0.8, "GOLD": 1.0, "BRENT": 1.5}

    async def kontrol(self, bot: Bot):
        veriler = {}
        try:
            for sym, label in [
                ("OANDA:USDTRY", "USDTRY"),
                ("OANDA:XAUUSD", "GOLD"),
            ]:
                r = requests.get(
                    "https://finnhub.io/api/v1/quote",
                    params={"symbol": sym, "token": FINNHUB_KEY}, timeout=6
                ).json()
                if r.get("c"): veriler[label] = r["c"]
        except: pass

        ISIMLER = {"USDTRY":"💵 USD/TRY","GOLD":"🥇 Altın (XAU/USD)","BRENT":"🛢️ Brent"}
        ETKILER = {
            "USDTRY": "📈 THYAO·FROTO·EREGL  |  📉 BIMAS·MGROS·ARCLK",
            "GOLD":   "📈 KOZAL",
            "BRENT":  "📉 TUPRS·PETKM·AYGAZ",
        }
        for label, fiyat in veriler.items():
            if label in self.onceki and self.onceki[label]:
                degisim = abs((fiyat - self.onceki[label]) / self.onceki[label] * 100)
                if degisim >= self.ESIKLER.get(label, 1.0):
                    yon = "📈" if fiyat > self.onceki[label] else "📉"
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=(
                                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"💹 <b>FİYAT ANOMALİSİ</b> {yon}\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                                f"📌 <b>{ISIMLER.get(label, label)}</b>\n"
                                f"   Önce: <code>{self.onceki[label]:.3f}</code>\n"
                                f"   Şimdi: <code>{fiyat:.3f}</code>\n"
                                f"   Hareket: <b>{yon} %{degisim:.2f}</b>\n\n"
                                f"🎯 {ETKILER.get(label,'—')}\n"
                                f"⏰ {datetime.now().strftime('%H:%M:%S')}"
                            ),
                            parse_mode=ParseMode.HTML
                        )
                        log.info(f"💹 {label} %{degisim:.1f}")
                    except Exception as e:
                        log.error(f"Fiyat radar gönderme: {e}")
            self.onceki[label] = fiyat

fiyat_radar = FiyatRadar()

# ════════════════════════════════════════════════
# 📅 MACRO ENGINE — 3 Fazlı Sistem
# ════════════════════════════════════════════════
class MacroEngine:
    """
    Faz 1 — 30 dk önce: beklenti + senaryo analizi
    Faz 2 — Veri anı: actual vs forecast + surprise score
    Faz 3 — 5 dk sonra: piyasa tepkisi
    """
    def __init__(self):
        self.sent_pre      = set()    # pre-release gönderildi
        self.sent_release  = set()    # release gönderildi
        self.sent_reaction = set()    # reaction gönderildi
        self.reaction_queue: list[dict] = []  # 5dk bekleme listesi

        # Türkiye + önemli global etkinlikler için BIST etkisi
        self.EVENT_CONTEXT = {
            # Türkiye
            "Turkey CPI":              ("🇹🇷", ["GARAN","AKBNK","ISCTR","YKBNK","BIMAS"], "TL, bankalar, tüketim"),
            "Turkey PPI":              ("🇹🇷", ["TUPRS","EREGL","FROTO"], "Sanayi, üretim maliyeti"),
            "Turkey Unemployment":     ("🇹🇷", ["BIMAS","MGROS","ULKER"], "Tüketim hisseleri"),
            "Turkey Current Account":  ("🇹🇷", ["XU030"], "TL ve endeks"),
            "Turkey Industrial Production":("🇹🇷", ["EREGL","FROTO","TOASO"], "Sanayi"),
            "Turkey GDP":              ("🇹🇷", ["XU030","GARAN","AKBNK"], "Endeks geneli"),
            "Turkey Trade Balance":    ("🇹🇷", ["FROTO","TOASO","EREGL"], "İhracatçılar"),
            "Turkey Interest Rate":    ("🇹🇷", ["GARAN","AKBNK","ISCTR","YKBNK","XU030"], "TÜM PİYASA — EN KRİTİK"),
            "Turkey Budget Balance":   ("🇹🇷", ["XU030"], "Genel endeks"),
            "Turkey Capacity Utilization": ("🇹🇷", ["SISE","ARCLK","FROTO"], "Sanayi"),
            "Turkey Consumer Confidence": ("🇹🇷", ["BIMAS","MGROS","ARCLK"], "Perakende"),
            "Turkey Inflation Rate":   ("🇹🇷", ["GARAN","AKBNK","ISCTR","YKBNK"], "Bankalar, TRY"),
            # ABD
            "United States Non Farm Payrolls": ("🇺🇸", ["XU030"], "Gelişen piyasalar, risk"),
            "United States CPI":       ("🇺🇸", ["XU030","KOZAL"], "Global risk-off, altın"),
            "United States PPI":       ("🇺🇸", ["XU030"], "Enflasyon beklentisi"),
            "United States Interest Rate": ("🇺🇸", ["XU030","GARAN","AKBNK"], "Fed faiz → TÜM GELİŞEN PİYASA"),
            "United States GDP":       ("🇺🇸", ["XU030"], "Global büyüme beklentisi"),
            "United States Unemployment Rate": ("🇺🇸", ["XU030"], "ABD işgücü, risk iştahı"),
            "United States PMI":       ("🇺🇸", ["XU030"], "Global aktivite"),
            "United States ADP Employment": ("🇺🇸", ["XU030"], "NFP öncü göstergesi"),
            "United States Initial Jobless Claims": ("🇺🇸", ["XU030"], "Haftalık istihdam"),
            "United States Retail Sales": ("🇺🇸", ["XU030"], "Tüketim"),
        }

        self.SENARYO = {
            "Turkey CPI": {
                "yuksek": "TL üzerinde baskı, bankalar negatif, TCMB sıkılaşma beklentisi artar",
                "dusuk":  "TL'ye destek, bankalar pozitif, TCMB gevşeme yolu açılır"
            },
            "Turkey Interest Rate": {
                "yuksek": "Bankalar baskı, TL güçlenir, endeks düşer",
                "dusuk":  "Bankalar rallisi, TL zayıflar, endeks yükselir"
            },
            "United States Non Farm Payrolls": {
                "yuksek": "Fed sıkılık, USD güçlenir, EM satışı, BIST negatif",
                "dusuk":  "Fed gevşeme beklentisi, EM alım, BIST pozitif"
            },
            "United States CPI": {
                "yuksek": "Fed hawkish, USD güçlenir, altın düşer, BIST negatif",
                "dusuk":  "Fed dovish, USD zayıflar, altın yükselir, BIST pozitif"
            },
            "United States Interest Rate": {
                "yuksek": "EM çıkış, USD güçlenir, BIST baskı",
                "dusuk":  "EM giriş, risk iştahı artar, BIST pozitif"
            },
        }

    async def kontrol(self, bot: Bot):
        await self._finnhub_kontrol(bot)
        await self._reaction_kontrol(bot)

    async def _finnhub_kontrol(self, bot: Bot):
        bugun = datetime.now().strftime("%Y-%m-%d")
        yarin = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            r = requests.get(
                "https://finnhub.io/api/v1/calendar/economic",
                params={"from": bugun, "to": yarin, "token": FINNHUB_KEY},
                timeout=10
            ).json()
            for ev in r.get("economicCalendar", []):
                country   = ev.get("country", "")
                impact    = ev.get("impact", "")
                event_name= ev.get("event", "")

                # Sadece Türkiye + Yüksek etkili ABD verilerini işle
                if country == "TR":
                    pass  # Tüm TR olayları
                elif country == "US" and impact in ["high", "medium"]:
                    pass
                else:
                    continue

                try:
                    ev_time = datetime.strptime(ev["time"], "%Y-%m-%d %H:%M:%S")
                except:
                    continue

                await self._process_event(bot, ev, ev_time, event_name, country, impact)
        except Exception as e:
            log.debug(f"MacroEngine Finnhub: {e}")

    async def _process_event(self, bot, ev, ev_time, event_name, country, impact):
        now      = datetime.now()
        diff_min = (ev_time - now).total_seconds() / 60
        forecast = ev.get("estimate")
        previous = ev.get("prev")
        actual   = ev.get("actual")
        ev_key   = f"{country}_{event_name}_{ev_time.strftime('%Y%m%d')}"

        # Faz 1 — 30 dk önce uyarı
        if 25 <= diff_min <= 35:
            key = f"pre_{ev_key}"
            if key not in self.sent_pre:
                self.sent_pre.add(key)
                await self._send_pre(bot, ev_time, event_name, country, impact,
                                     forecast, previous)

        # Faz 2 — Veri açıklandı (actual geldi, event time geçti)
        elif diff_min < 5 and actual is not None and str(actual).strip() not in ("", "0"):
            key = f"release_{ev_key}"
            if key not in self.sent_release:
                self.sent_release.add(key)
                # SQLite'a kaydet
                try:
                    surprise = None
                    if forecast and forecast != 0:
                        surprise = (float(actual) - float(forecast)) / abs(float(forecast))
                    con = sqlite3.connect(DB_PATH)
                    con.execute("""
                        INSERT OR REPLACE INTO macro_events
                        (event_key,timestamp,country,event_name,importance,
                         forecast,previous,actual,surprise,phase,symbols)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        ev_key, datetime.now().isoformat(), country, event_name,
                        impact, forecast, previous, actual, surprise, "release",
                        json.dumps(self.EVENT_CONTEXT.get(event_name, ("","[]",""))[1])
                    ))
                    con.commit()
                    con.close()
                except: pass
                await self._send_release(bot, ev_time, event_name, country, impact,
                                         actual, forecast, previous)
                # 5 dk tepki kuyruğu
                self.reaction_queue.append({
                    "queued_at": now,
                    "event_name": event_name,
                    "country": country,
                    "actual": actual,
                    "forecast": forecast,
                    "key": f"reaction_{ev_key}",
                    "symbols": self.EVENT_CONTEXT.get(event_name, ("","[]",""))[1],
                })

    async def _reaction_kontrol(self, bot: Bot):
        now = datetime.now()
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
        ctx    = self.EVENT_CONTEXT.get(event_name, ("🌍", [], "—"))
        flag   = ctx[0]
        syms   = "  ".join([f"<code>{s}</code>" for s in ctx[1][:5]]) or "—"
        ozet   = ctx[2]
        impact_icon = "🔴" if impact == "high" else "🟡"

        senaryo = self.SENARYO.get(event_name)
        senaryo_str = ""
        if senaryo:
            senaryo_str = (
                f"\n\n<b>Senaryolar:</b>\n"
                f"📈 Yüksek gelirse: {senaryo['yuksek']}\n"
                f"📉 Düşük gelirse: {senaryo['dusuk']}"
            )

        tahmin_str = f"Beklenti: <code>{forecast}</code>  " if forecast else ""
        onceki_str = f"Önceki: <code>{previous}</code>" if previous else ""

        try:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=(
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏰ <b>30 DAKİKA SONRA</b>  {flag}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📋 <b>{event_name}</b>\n"
                    f"🕐 <code>{ev_time.strftime('%H:%M')}</code>  {impact_icon} {impact.upper()}\n\n"
                    f"{tahmin_str}{onceki_str}\n\n"
                    f"🎯 Etkilenecek: {syms}\n"
                    f"📌 {ozet}"
                    f"{senaryo_str}"
                ),
                parse_mode=ParseMode.HTML
            )
            log.info(f"⏰ Pre-release: {event_name}")
        except Exception as e:
            log.error(f"send_pre: {e}")

    async def _send_release(self, bot, ev_time, event_name, country, impact,
                             actual, forecast, previous):
        flag = self.EVENT_CONTEXT.get(event_name, ("🌍",))[0]
        ctx  = self.EVENT_CONTEXT.get(event_name, ("🌍", [], "—"))
        syms = "  ".join([f"<code>{s}</code>" for s in ctx[1][:5]]) or "—"

        surprise_str = ""
        surprise_icon = ""
        if forecast and str(forecast).strip() not in ("", "0"):
            try:
                surprise = float(actual) - float(forecast)
                pct      = surprise / abs(float(forecast))
                if pct > 0.05:
                    surprise_icon = "🔥 BEKLENTIDEN SICAK"
                    surprise_str  = f"Sürpriz: <b>+{surprise:.3f} ({pct*100:.1f}%)</b> ↑"
                elif pct < -0.05:
                    surprise_icon = "🧊 BEKLENTIDEN SOĞUK"
                    surprise_str  = f"Sürpriz: <b>{surprise:.3f} ({pct*100:.1f}%)</b> ↓"
                else:
                    surprise_icon = "➡️ BEKLENTIYE YAKIN"
                    surprise_str  = f"Sürpriz: <b>{surprise:+.3f}</b>"
            except: pass

        try:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=(
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 <b>VERİ GELDİ</b>  {flag}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📋 <b>{event_name}</b>\n\n"
                    f"Actual:   <b><code>{actual}</code></b>\n"
                    f"Forecast: <code>{forecast or '—'}</code>\n"
                    f"Previous: <code>{previous or '—'}</code>\n\n"
                    f"{surprise_str}\n"
                    f"<b>{surprise_icon}</b>\n\n"
                    f"🎯 Etkilenecek: {syms}\n"
                    f"⏰ 5 dk içinde piyasa tepkisi gelecek..."
                ),
                parse_mode=ParseMode.HTML
            )
            log.info(f"📊 Release: {event_name} actual={actual}")
        except Exception as e:
            log.error(f"send_release: {e}")

    async def _send_reaction(self, bot, item):
        symbols = item.get("symbols", [])
        reactions = []
        # USDTRY tepkisi
        try:
            r = requests.get(
                "https://finnhub.io/api/v1/quote",
                params={"symbol": "OANDA:USDTRY", "token": FINNHUB_KEY}, timeout=5
            ).json()
            if r.get("dp"):
                reactions.append(f"💵 USD/TRY: <b>{r['dp']:+.2f}%</b>")
        except: pass
        # Altın
        try:
            r = requests.get(
                "https://finnhub.io/api/v1/quote",
                params={"symbol": "OANDA:XAUUSD", "token": FINNHUB_KEY}, timeout=5
            ).json()
            if r.get("dp"):
                reactions.append(f"🥇 Altın: <b>{r['dp']:+.2f}%</b>")
        except: pass
        # İlgili hisse (ilk sembol)
        if symbols:
            try:
                r = requests.get(
                    "https://finnhub.io/api/v1/quote",
                    params={"symbol": f"XIST:{symbols[0]}", "token": FINNHUB_KEY},
                    timeout=5
                ).json()
                if r.get("dp"):
                    reactions.append(f"📈 {symbols[0]}: <b>{r['dp']:+.2f}%</b>")
            except: pass

        if not reactions:
            return

        reaction_text = "\n".join(reactions)
        try:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=(
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚡ <b>İLK PİYASA TEPKİSİ</b> (5 dk)\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📋 <b>{item['event_name']}</b>\n\n"
                    f"{reaction_text}\n\n"
                    f"⏰ {datetime.now().strftime('%H:%M:%S')}"
                ),
                parse_mode=ParseMode.HTML
            )
            log.info(f"⚡ Reaction: {item['event_name']}")
        except Exception as e:
            log.error(f"send_reaction: {e}")

macro_engine = MacroEngine()

# ════════════════════════════════════════════════
# ⚡ MOMENTUM RADAR
# ════════════════════════════════════════════════
class MomentumRadar:
    def __init__(self):
        self.kume         = defaultdict(list)
        self.PENCERE_DK   = 15
        self.ESIK_ORTA    = 3
        self.ESIK_KRITIK  = 5
        self.gonderilen   = set()
        self.KUMELER = {
            "İsrail-İran":    ["israel","iran","tel aviv","tehran","idf","hamas","hizbullah"],
            "Ukrayna-Rusya":  ["ukrayna","rusya","ukraine","russia","putin","zelensky"],
            "Fed-Faiz":       ["fomc","powell","federal reserve rate","fed rate decision"],
            "TCMB-Faiz":      ["tcmb faiz","ppk kararı","para politikası toplantı"],
            "Petrol-Enerji":  ["brent +","petrol fiyat arttı","opec acil","hürmüz kapandı"],
            "TL-Kriz":        ["tl kriz","dolar tavan","lira çöküş","kur saldırı"],
            "Altın":          ["altın rekor","gold rally","xau yükseldi","ons fiyat"],
            "Deprem":         ["büyük deprem","şiddetli deprem","earthquake m"],
            "Çin-Ekonomi":    ["çin ekonomi","china gdp","pboc","yuan"],
            "Savunma-ASELS":  ["aselsan","savunma ihalesi","siha sözleşme","ssb projesi"],
        }
        self.ETKILENENLER = {
            "İsrail-İran":   "ASELS · KOZAL · TUPRS · PETKM",
            "Ukrayna-Rusya": "ASELS · TUPRS · EREGL",
            "Fed-Faiz":      "XU030 · Bankalar · USD/TRY",
            "TCMB-Faiz":     "GARAN · AKBNK · ISCTR · YKBNK",
            "Petrol-Enerji": "TUPRS · PETKM · AYGAZ",
            "TL-Kriz":       "THYAO·FROTO·EREGL ↑  |  BIMAS·MGROS ↓",
            "Altın":         "KOZAL",
            "Deprem":        "EKGYO · ENKAI",
            "Çin-Ekonomi":   "EREGL · PETKM · ihracatçılar",
            "Savunma-ASELS": "ASELS",
        }

    def ekle(self, baslik: str):
        b = baslik.lower()
        for kume_adi, keywords in self.KUMELER.items():
            if any(kw in b for kw in keywords):
                self.kume[kume_adi].append((baslik, datetime.now()))

    def _temizle(self):
        sinir = datetime.now() - timedelta(minutes=self.PENCERE_DK)
        for k in list(self.kume.keys()):
            self.kume[k] = [(b,t) for b,t in self.kume[k] if t > sinir]

    def kontrol(self) -> list:
        self._temizle()
        alarmlar = []
        for kume_adi, haberler in self.kume.items():
            n = len(haberler)
            if n < self.ESIK_ORTA: continue
            anahtar = f"{kume_adi}_{n // self.ESIK_ORTA}"
            if anahtar in self.gonderilen: continue
            self.gonderilen.add(anahtar)
            alarmlar.append((kume_adi, n, haberler[-3:]))
        if len(self.gonderilen) > 300:
            self.gonderilen = set(list(self.gonderilen)[-150:])
        return alarmlar

    def alarm_mesaj(self, kume_adi: str, n: int, son_h: list) -> str:
        ikon    = "🔥" if n >= self.ESIK_KRITIK else "⚡"
        seviye  = "KRİTİK MOMENTUM" if n >= self.ESIK_KRITIK else "HIZLANIYOR"
        etki    = self.ETKILENENLER.get(kume_adi, "BIST geneli")
        haberler_str = "".join([f"  • {b[:80]}\n" for b,_ in son_h])
        return (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{ikon} <b>{seviye}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 Konu: <b>{kume_adi}</b>\n"
            f"📊 Son {self.PENCERE_DK} dakika: <b>{n} haber</b>\n\n"
            f"📰 Son haberler:\n{haberler_str}\n"
            f"🎯 Etkilenecek: {etki}\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )

momentum = MomentumRadar()

# ════════════════════════════════════════════════
# 🧠 NOVELTY MEMORY
# ════════════════════════════════════════════════
class NoveltyMemory:
    def __init__(self):
        self.memory = defaultdict(list)

    def _temizle(self):
        sinir = datetime.now() - timedelta(hours=NOVELTY_HOURS)
        for e in list(self.memory.keys()):
            self.memory[e] = [(kws,ts) for kws,ts in self.memory[e] if ts > sinir]

    def skorla(self, entity: str, baslik: str) -> float:
        self._temizle()
        kws = set(re.findall(r'\w{4,}', baslik.lower()))
        max_ov = 0.0
        for gecmis_kws, _ in self.memory.get(entity, []):
            if kws and gecmis_kws:
                ov = len(kws & gecmis_kws) / max(len(kws), len(gecmis_kws))
                max_ov = max(max_ov, ov)
        self.memory[entity].append((kws, datetime.now()))
        if max_ov > 0.65: return 0.15
        if max_ov > 0.35: return 0.55
        return 1.0

    def entity_bul(self, baslik: str) -> list:
        b = baslik.lower()
        out = []
        for t in BIST30:
            if t.lower() in b: out.append(t)
        for t, vs in ENTITY_DICT.items():
            if t not in out:
                for v in vs:
                    if v in b:
                        out.append(t)
                        break
        return out[:5]

nov = NoveltyMemory()

# ════════════════════════════════════════════════
# 📡 RSS KAYNAKLARI
# ════════════════════════════════════════════════
RSS_GLOBAL = [
    ("Reuters",         "https://feeds.reuters.com/reuters/topNews"),
    ("Reuters Biz",     "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters EM",      "https://feeds.reuters.com/reuters/emergingMarketsNews"),
    ("Reuters Dünya",   "https://feeds.reuters.com/reuters/worldNews"),
    ("AP News",         "https://rsshub.app/apnews/topics/ap-top-news"),
    ("BBC Dünya",       "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("BBC Ekonomi",     "https://feeds.bbci.co.uk/news/business/rss.xml"),
    ("CNBC",            "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("CNBC Markets",    "https://www.cnbc.com/id/15839135/device/rss/rss.html"),
    ("ZeroHedge",       "https://feeds.feedburner.com/zerohedge/feed"),
    ("FED",             "https://www.federalreserve.gov/feeds/press_all.xml"),
    ("ECB",             "https://www.ecb.europa.eu/rss/press.html"),
    ("EIA Petrol",      "https://www.eia.gov/rss/todayinenergy.xml"),
    ("OilPrice",        "https://oilprice.com/rss/main"),
    ("ISW",             "https://www.understandingwar.org/rss.xml"),
    ("Al Monitor TR",   "https://www.al-monitor.com/rss/turkey.xml"),
    ("Al Monitor ME",   "https://www.al-monitor.com/rss/mideast.xml"),
    ("IAEA",            "https://www.iaea.org/feeds/topstories.xml"),
    ("ABD Dışişleri",   "https://www.state.gov/rss-feed/press-releases/feed/"),
    ("NATO",            "https://www.nato.int/cps/en/natolive/news.rss"),
    ("USGS Deprem",     "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.atom"),
    ("IMF",             "https://www.imf.org/en/News/rss?language=eng"),
    ("BIS",             "https://www.bis.org/doclist/bis_fsr.rss"),
]

RSS_TURKEY = [
    ("TCMB",            "https://www.tcmb.gov.tr/wps/wcm/connect/TR/rss/"),
    ("SPK",             "https://www.spk.gov.tr/rss/HaberDetay"),
    ("BDDK",            "https://www.bddk.org.tr/Rss/RssDuyuru"),
    ("Hazine",          "https://www.hmb.gov.tr/rss"),
    ("Borsa İstanbul",  "https://www.borsaistanbul.com/tr/rss/haberler"),
    ("Resmi Gazete",    "https://www.resmigazete.gov.tr/rss/tum.xml"),
    ("BTK",             "https://www.btk.gov.tr/rss"),
    ("Rekabet Kurumu",  "https://www.rekabet.gov.tr/tr/Rss/Karar"),
    ("AA Ekonomi",      "https://www.aa.com.tr/tr/rss/default?cat=ekonomi"),
    ("AA Gündem",       "https://www.aa.com.tr/tr/rss/default?cat=gundem"),
    ("DHA Ekonomi",     "https://www.dha.com.tr/rss/ekonomi.xml"),
    ("Bloomberg HT",    "https://www.bloomberght.com/rss"),
    ("Dünya",           "https://www.dunya.com/rss/rss.xml"),
    ("Ekonomim",        "https://www.ekonomim.com/rss"),
    ("Para Analiz",     "https://www.paraanaliz.com/feed/"),
    ("Finans Gündem",   "https://www.finansgundem.com/rss"),
    ("Anka Haber",      "https://www.ankahaber.net/rss.xml"),
    ("NTV Ekonomi",     "https://www.ntv.com.tr/ekonomi.rss"),
    ("Hürriyet Ekon",   "https://www.hurriyet.com.tr/rss/ekonomi"),
    ("Sabah Ekonomi",   "https://www.sabah.com.tr/rss/ekonomi.xml"),
    ("Sözcü Ekonomi",   "https://www.sozcu.com.tr/rss/ekonomi.xml"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}

# ════════════════════════════════════════════════
# 📥 TOPLAYICILAR
# ════════════════════════════════════════════════
def yeni_h(kaynak: str, baslik: str, url: str, tip: str = "rss") -> dict:
    tier = kaynak_tier(kaynak)
    return {
        "kaynak": kaynak, "baslik": baslik.strip()[:260],
        "url": url, "zaman": datetime.now().strftime("%H:%M"),
        "skor": 0, "ml_skor": 0.5, "yon": "NÖTR",
        "semboller": [], "sektor_ozet": "", "stream": "NEWS",
        "ozet": "", "novelty": 1.0, "kaynak_tip": tip,
        "tier": tier, "scheduled_event": False, "surprise": None,
    }

def rss_cek(feeds: list) -> list:
    out = []
    for kaynak, url in feeds:
        try:
            feed = feedparser.parse(url, request_headers=HEADERS)
            for e in feed.entries[:6]:
                b = (e.get("title") or "").strip()
                if b and len(b) > 10:
                    out.append(yeni_h(kaynak, b, e.get("link", "")))
        except Exception as e:
            log.debug(f"RSS {kaynak}: {e}")
    return out

def marketaux_cek() -> list:
    out = []
    try:
        r = requests.get(
            "https://api.marketaux.com/v1/news/all",
            params={"countries":"tr","filter_entities":"true","language":"tr,en",
                    "limit":10,"api_token":MARKETAUX_KEY},
            timeout=12, headers=HEADERS
        )
        for item in r.json().get("data", []):
            b = item.get("title","")
            if not b: continue
            syms = [e.get("symbol","") for e in item.get("entities",[])
                   if e.get("exchange","") in ["BIST","XIST","IST"] and e.get("symbol")]
            h = yeni_h("Marketaux", b, item.get("url",""), "api")
            h["semboller"] = syms[:4]
            out.append(h)
    except Exception as e:
        log.warning(f"Marketaux: {e}")
    return out

def finnhub_genel_cek() -> list:
    out = []
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/news",
            params={"category":"general","token":FINNHUB_KEY},
            timeout=10, headers=HEADERS
        )
        for item in r.json()[:10]:
            b = item.get("headline","")
            if b:
                out.append(yeni_h(item.get("source","Finnhub"), b, item.get("url",""), "api"))
    except Exception as e:
        log.warning(f"Finnhub genel: {e}")
    return out

def finnhub_bist_cek() -> list:
    out   = []
    dun   = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    bugun = datetime.now().strftime("%Y-%m-%d")
    for s in random.sample(BIST30, 6):
        try:
            r = requests.get(
                "https://finnhub.io/api/v1/company-news",
                params={"symbol":f"XIST:{s}","from":dun,"to":bugun,"token":FINNHUB_KEY},
                timeout=7, headers=HEADERS
            )
            for item in r.json()[:2]:
                b = item.get("headline","")
                if b:
                    h = yeni_h(f"Finnhub [{s}]", f"[{s}] {b}", item.get("url",""), "sirket")
                    h["semboller"] = [s]
                    out.append(h)
            time.sleep(0.1)
        except: pass
    return out

def sirket_haberleri_cek() -> list:
    out = []
    for ad, url, domain in [
        ("Foreks",       "https://www.foreks.com/haberler/sirket-haberleri/",  "www.foreks.com"),
        ("BorsaGündem",  "https://www.borsagundem.com.tr/sirket-haberleri",    "www.borsagundem.com.tr"),
        ("Investing TR", "https://tr.investing.com/news/company-news",          "tr.investing.com"),
    ]:
        try:
            r = requests.get(url, timeout=12, headers=HEADERS)
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

# ════════════════════════════════════════════════
# 🧠 CLAUDE ANALİZ
# ════════════════════════════════════════════════
def claude_skore_et(haberler: list, mod: str) -> list:
    if not haberler: return []
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        liste  = "\n".join([f"{i+1}. [{h['tier']}. Tier] {h['baslik']}"
                            for i,h in enumerate(haberler)])

        if mod == "global":
            prompt = f"""Sen BIST/VİOP uzmanı Türk tradersin. Global haberleri BIST etkisi açısından skorla.
Tier bilgisi: Tier1=TCMB/KAP/Reuters, Tier2=Bloomberg/AA, Tier3=medya

PUANLAMA:
9-10 = Piyasayı ANINDA etkiler: Fed acil, büyük savaş, Hürmüz kapanır, küresel kriz
7-8  = Önemli: Fed/ECB sürpriz, petrol %3+, jeopolitik şok
5-6  = Orta: Beklenen veri, bölgesel gerilim
0-4  = Gürültü: Magazin, rutin haber

Tier1 kaynak +1 puan, Tier3 kaynak -1 puan.

SADECE JSON:
[{{"id":1,"skor":9,"yon":"BEARISH","semboller":["TUPRS"],"ozet":"Türkçe max 12 kelime"}}]

Haberler:
{liste}"""
        else:
            prompt = f"""Sen BIST/VİOP uzmanı Türk tradersin. Türkiye haberlerini BIST etkisi açısından skorla.
BIST30: {', '.join(BIST30)}
[SEMBOL] ile başlayanlar = KAP/şirket haberi!
Tier1 kaynak +1 puan, Tier3 kaynak -1 puan.

PUANLAMA:
9-10 = Acil: TCMB sürpriz faiz, TL kriz, yaptırım, deprem
8    = Çok önemli: BIST30 KAP kararı, enflasyon/büyüme verisi, TCMB/SPK/BDDK kararı
7    = Önemli: Orta şirket haberi, sektör kararı
5-6  = Takip: Küçük şirket, genel ekonomi yorum
0-4  = Gürültü: Siyaset, spor, magazin, belirsiz

SADECE JSON:
[{{"id":1,"skor":8,"yon":"BULLISH","semboller":["THYAO"],"ozet":"Türkçe max 12 kelime"}}]

Haberler:
{liste}"""

        msg  = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=2500,
            messages=[{"role":"user","content":prompt}]
        )
        text = msg.content[0].text.strip()
        if "```" in text:
            m = re.search(r'\[.*\]', text, re.DOTALL)
            text = m.group(0) if m else "[]"
        if not text.startswith("["):
            m = re.search(r'\[.*\]', text, re.DOTALL)
            text = m.group(0) if m else "[]"

        for s in json.loads(text):
            idx = s.get("id",0) - 1
            if 0 <= idx < len(haberler):
                mevcut = haberler[idx].get("semboller",[])
                raw_skor = max(0, min(10, int(s.get("skor",0))))
                # Tier ayarı
                adj_skor = tier_skor_ayarla(raw_skor, haberler[idx].get("tier", 2))
                haberler[idx].update({
                    "skor":      adj_skor,
                    "yon":       s.get("yon","NÖTR"),
                    "semboller": mevcut if mevcut else s.get("semboller",[]),
                    "ozet":      s.get("ozet","")
                })
    except Exception as e:
        log.warning(f"Claude ({mod}): {e}")
    return haberler

# ════════════════════════════════════════════════
# 💬 MESAJ FORMATI — 3 Stream
# ════════════════════════════════════════════════
KAYNAK_IKONLARI = {
    "Reuters":"🔴","AP News":"🔴","BBC":"🟣","CNBC":"🟡","ZeroHedge":"🐻",
    "FED":"🏦","ECB":"🏦","BIS":"🏦","IMF":"🏦","EIA":"⛽","OilPrice":"🛢️",
    "ISW":"⚔️","Al Monitor":"🌙","IAEA":"☢️","NATO":"🛡️","ABD Dışişleri":"🦅",
    "USGS":"🌍","TCMB":"🏛️","SPK":"🏛️","BDDK":"🏛️","Hazine":"🏛️","BTK":"🏛️",
    "Rekabet":"⚖️","Borsa İstanbul":"📊","Resmi Gazete":"📜",
    "AA":"📡","DHA":"📡","Bloomberg HT":"💹","Dünya":"💹","Para Analiz":"💹",
    "Ekonomim":"💹","Marketaux":"📊","Finnhub":"📈","Foreks":"📈",
    "BorsaGündem":"📈","Investing":"📈",
}

STREAM_HEADER = {
    "MACRO":   "🏛️ <b>MACRO</b>",
    "COMPANY": "🏢 <b>COMPANY</b>",
    "GEO":     "🌍 <b>GEO / MARKET</b>",
    "NEWS":    "📰 <b>NEWS</b>",
}

def kaynak_ikon(kaynak: str) -> str:
    for k, v in KAYNAK_IKONLARI.items():
        if k.lower() in kaynak.lower(): return v
    return "📌"

def hash_h(t: str) -> str:
    return hashlib.md5(t.encode("utf-8","ignore")).hexdigest()[:12]

def mesaj_olustur(h: dict, mod: str, acil: bool = False) -> str:
    skor   = h.get("skor", 0)
    yon    = h.get("yon","NÖTR")
    syms   = h.get("semboller",[])
    ozet   = h.get("ozet","")
    nv     = h.get("novelty",1.0)
    ml_s   = h.get("ml_skor",0.5)
    sk_oz  = h.get("sektor_ozet","")
    stream = h.get("stream","NEWS")
    tier   = h.get("tier", 2)
    flag   = "🇹🇷" if mod=="turkey" else "🌍"

    if acil or skor >= 9:
        seviye, renk = "🚨 <b>ACİL ALARM</b>", "🔴"
    elif skor >= 8:
        seviye, renk = "⚡ <b>ÖNEMLİ</b>", "🟡"
    elif skor >= 7:
        seviye, renk = "🔵 <b>TAKİP</b>", "🔵"
    else:
        seviye, renk = "⚪ <b>BİLGİ</b>", "⚪"

    yon_str    = {"BULLISH":"📈 BULLISH","BEARISH":"📉 BEARISH",
                  "NÖTR":"➡️ NÖTR","MIXED":"↔️ MIXED"}.get(yon, yon)
    nb         = "🆕" if nv>=0.9 else ("🔄" if nv>=0.5 else "♻️")
    tier_badge = {1:"🏆T1", 2:"", 3:"📰T3"}.get(tier, "")
    ki         = kaynak_ikon(h["kaynak"])
    ss         = "  ".join([f"<code>#{s}</code>" for s in syms[:5]])

    ml_line = ""
    if ml.trained and ml_s >= 0.7:
        ml_line = f"\n🤖 <b>ML: %{int(ml_s*100)} hareket ihtimali</b>"
    elif ml.trained and ml_s >= 0.5:
        ml_line = f"\n🤖 ML: %{int(ml_s*100)}"

    stream_label = STREAM_HEADER.get(stream, "📰 <b>NEWS</b>")

    msg  = f"━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"{renk}{flag} {seviye}  <b>[{skor}/10]</b>  {nb}  {stream_label}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"{ki} <b>{h['kaynak']}</b> {tier_badge}  ·  ⏰ <code>{h['zaman']}</code>\n\n"
    msg += f"📰 {h['baslik']}\n"
    if ozet:
        msg += f"\n{yon_str}\n💬 {ozet}\n"
    if sk_oz:
        msg += f"🏭 {sk_oz}\n"
    if ml_line:
        msg += ml_line + "\n"
    if ss:
        msg += f"\n🎯 {ss}\n"
    if h.get("url"):
        msg += f"\n🔗 <a href='{h['url']}'>Habere Git →</a>"
    return msg

# ════════════════════════════════════════════════
# 🌅 SABAH BRİFİNG + 📊 AKŞAM ÖZETİ
# ════════════════════════════════════════════════
son_sabah_brifing: str | None = None
son_aksam_ozeti:   str | None = None

async def sabah_brifing(bot: Bot):
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        sinir  = (datetime.now() - timedelta(hours=12)).isoformat()
        con    = sqlite3.connect(DB_PATH)
        rows   = con.execute("""
            SELECT baslik, ai_skor, yon, semboller, stream
            FROM haberler WHERE gonderildi=1 AND timestamp > ?
            ORDER BY ai_skor DESC LIMIT 15
        """, (sinir,)).fetchall()
        con.close()

        # Bugünün macro takvimi (Finnhub)
        bugun = datetime.now().strftime("%Y-%m-%d")
        takvim_str = "Bugün önemli veri yok"
        try:
            r = requests.get(
                "https://finnhub.io/api/v1/calendar/economic",
                params={"from":bugun,"to":bugun,"token":FINNHUB_KEY}, timeout=8
            ).json()
            events = [
                f"• {ev['time'][11:16]} — {ev['event']} [{ev.get('country','')}]"
                for ev in r.get("economicCalendar",[])
                if ev.get("impact") in ["high","medium"]
                and ev.get("country") in ["TR","US"]
            ]
            if events: takvim_str = "\n".join(events[:8])
        except: pass

        gece_listesi = "\n".join([f"• [{r[1]}/10] [{r[4]}] {r[0]}" for r in rows[:10]]) \
                       or "Gece sakin geçti."

        prompt = f"""Sen BIST/VİOP uzmanı Türk trader asistanısın. Sabah 09:45 brifingini yaz.
Kısa, net, aksiyon odaklı. Türkçe. Max 20 satır.

GECE HABERLERİ (önem sırasıyla):
{gece_listesi}

BUGÜN TAKVİM:
{takvim_str}

Yaz:
🌍 GECE NE OLDU — 3-4 madde, her birinde hisse ticker
📅 BUGÜN TAKVİM — saatlerle birlikte
👁 GÖZETLEME — bugün dikkat edilecek 3 hisse + neden
💡 GENEL YORUM — tek cümle piyasa tonu"""

        msg = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=800,
            messages=[{"role":"user","content":prompt}]
        )
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🌅 <b>SABAH BRİFİNG</b> — {datetime.now().strftime('%d.%m.%Y')}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{msg.content[0].text.strip()}"
            ),
            parse_mode=ParseMode.HTML
        )
        log.info("🌅 Sabah brifing gönderildi")
    except Exception as e:
        log.warning(f"sabah_brifing: {e}")

async def aksam_ozeti(bot: Bot):
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        sinir  = datetime.now().replace(hour=8, minute=0, second=0).isoformat()
        con    = sqlite3.connect(DB_PATH)
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
        # Feedback istatistikleri
        fb_rows = con.execute("""
            SELECT feedback, COUNT(*) FROM haberler
            WHERE feedback IS NOT NULL AND timestamp >= ?
            GROUP BY feedback
        """, (sinir,)).fetchall()
        con.close()

        acil_s   = sum(1 for r in rows if r[1] >= 9)
        onemli_s = sum(1 for r in rows if 8 <= r[1] < 9)
        takip_s  = sum(1 for r in rows if 7 <= r[1] < 8)
        ml_t     = ml_r[0] or 0
        ml_d     = ml_r[1] or 0
        ml_oran  = f"%{int(ml_d/ml_t*100)}" if ml_t > 0 else "—"
        fb_str   = "  ".join([f"{k}:{v}" for k,v in fb_rows]) or "—"

        hisse_c = defaultdict(int)
        for r in rows:
            for s in json.loads(r[3] or "[]"): hisse_c[s] += 1
        top_h    = sorted(hisse_c.items(), key=lambda x:x[1], reverse=True)[:5]
        hisse_str= "  ".join([f"<code>{s}</code>({n})" for s,n in top_h]) or "—"
        top5     = "\n".join([f"• [{r[1]}/10] [{r[4]}] {r[0][:70]}" for r in rows[:5]]) or "—"

        # Yarının takvimi
        yarin = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        yarin_takvim = "—"
        try:
            r2 = requests.get(
                "https://finnhub.io/api/v1/calendar/economic",
                params={"from":yarin,"to":yarin,"token":FINNHUB_KEY}, timeout=8
            ).json()
            evs = [f"• {e['time'][11:16]} {e['event']}"
                   for e in r2.get("economicCalendar",[])
                   if e.get("impact")=="high" and e.get("country") in ["TR","US"]]
            if evs: yarin_takvim = "\n".join(evs[:5])
        except: pass

        prompt = f"""Sen BIST/VİOP uzmanı. Kısa akşam özeti yaz. Türkçe. Max 12 satır.
EN ÖNEMLİ HABERLER:
{top5}
Yaz: genel değerlendirme (1 cümle), en kritik haber ve etkisi, yarın dikkat edilecek konu."""

        msg = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=500,
            messages=[{"role":"user","content":prompt}]
        )
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>GÜNÜN ÖZETİ</b> — {datetime.now().strftime('%d.%m.%Y')}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📨 Bugün: <b>{len(rows)} haber</b>  🔴{acil_s}  🟡{onemli_s}  🔵{takip_s}\n"
                f"🎯 En çok: {hisse_str}\n"
                f"🤖 ML doğruluk: {ml_oran} ({ml_t} ölçüm)\n"
                f"📋 Feedback: {fb_str}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{msg.content[0].text.strip()}\n\n"
                f"📅 <b>Yarın:</b>\n{yarin_takvim}"
            ),
            parse_mode=ParseMode.HTML
        )
        log.info("📊 Akşam özeti gönderildi")
    except Exception as e:
        log.warning(f"aksam_ozeti: {e}")

# ════════════════════════════════════════════════
# 🔄 ANA DÖNGÜ
# ════════════════════════════════════════════════
bekl_global:       list = []
bekl_turkey:       list = []
son_ai:            float = 0.0
son_fiyat:         float = 0.0
son_macro:         float = 0.0
son_ml_guncelle:   float = 0.0
son_feedback:      float = 0.0
son_durum:         datetime = datetime.now()
son_sabah_brifing: str | None = None
son_aksam_ozeti:   str | None = None
dongu_sayac:       int = 0
toplam_gonderilen: int = 0
baslangic:         datetime = datetime.now()
telegram_kuyruk:   list = []

async def ana_dongu():
    global bekl_global, bekl_turkey, son_ai, son_fiyat, son_macro
    global son_ml_guncelle, son_feedback, son_durum, dongu_sayac
    global toplam_gonderilen, telegram_kuyruk
    global son_sabah_brifing, son_aksam_ozeti

    db_init()
    dedup_yukle()  # ← Kalıcı dedup yükle
    bot = Bot(token=TELEGRAM_TOKEN)
    telethon_aktif = False

    # ── Telethon ──────────────────────────────
    if TELETHON_SESSION and TELEGRAM_KANALLARI:
        try:
            tg = TelegramClient(
                StringSession(TELETHON_SESSION), TELEGRAM_API_ID, TELEGRAM_API_HASH
            )
            await tg.start()
            telethon_aktif = True
            log.info(f"✅ Telethon: {len(TELEGRAM_KANALLARI)} kanal")

            @tg.on(tg_events.NewMessage(chats=TELEGRAM_KANALLARI))
            async def tg_handler(event):
                m = event.raw_text
                if m and len(m) > 15:
                    um = re.search(r'https?://\S+', m)
                    b  = m[:260].replace("\n"," ").strip()
                    h  = yeni_h(
                        f"📲 {getattr(event.chat,'title','Telegram')}",
                        b, um.group(0) if um else "", "telegram"
                    )
                    h["semboller"] = nov.entity_bul(b)
                    telegram_kuyruk.append(h)
        except Exception as e:
            log.warning(f"Telethon: {e}")

    # ── Başlangıç mesajı ──────────────────────
    await bot.send_message(
        chat_id=CHAT_ID,
        text=(
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🤖 <b>BERKAY TERMINATOR v3.1</b>\n"
            f"🗓 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "<b>SİSTEMLER:</b>\n"
            f"{'✅' if telethon_aktif else '⚪'} Telethon\n"
            "✅ Kalıcı Dedup (SQLite)\n"
            "✅ Source Tiering (T1/T2/T3)\n"
            "✅ Event ID Dedup (20dk)\n"
            "✅ Macro Engine (30dk+Veri+5dk)\n"
            "✅ Feedback Butonları\n"
            "✅ 3-Stream (MACRO/COMPANY/GEO)\n"
            "✅ Multi-Horizon ML (5m/15m/60m)\n"
            "✅ Fiyat Radar · Momentum Radar\n"
            "✅ Sabah/Akşam Rapor\n\n"
            f"🌍 {len(RSS_GLOBAL)} global  |  🇹🇷 {len(RSS_TURKEY)} TR RSS\n\n"
            "🔴 ACİL [9-10]  🟡 ÖNEMLİ [8]  🔵 TAKİP [7]\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📡 İzleme başladı..."
        ),
        parse_mode=ParseMode.HTML
    )
    log.info("✅ TERMINATOR v3.1 BAŞLADI!")

    # ── Ana döngü ─────────────────────────────
    while True:
        try:
            now   = time.time()
            simdi = datetime.now()
            dongu_sayac += 1

            # Haber çek
            g_raw = rss_cek(RSS_GLOBAL) + finnhub_genel_cek()
            t_raw = rss_cek(RSS_TURKEY) + marketaux_cek()

            # Her 4 döngüde şirket haberleri
            if dongu_sayac % 4 == 0:
                sirket = sirket_haberleri_cek() + finnhub_bist_cek()
                t_raw += sirket
                if sirket: log.info(f"📈 {len(sirket)} şirket haberi")

            # Telegram kuyruğu
            if telegram_kuyruk:
                t_raw += telegram_kuyruk[:30]
                telegram_kuyruk.clear()

            acil_kuyruk: list = []

            def isle(haberler: list, kuyruk: list, mod: str):
                for h in haberler:
                    hsh = hash_h(h["baslik"])
                    if hsh in gonderilen or not h["baslik"] or len(h["baslik"]) < 10:
                        continue
                    # Event ID dedup (aynı olay 20dk içinde)
                    if event_tekrar_mi(h["baslik"]):
                        continue
                    gonderilen.add(hsh)
                    dedup_kaydet(hsh)  # ← Kalıcı kaydet
                    # Sektör matrisi
                    sm_syms, sm_yon, sm_oz = sektor_analiz(h["baslik"])
                    if not h["semboller"]:
                        h["semboller"] = nov.entity_bul(h["baslik"])
                    for s in sm_syms:
                        if s not in h["semboller"]: h["semboller"].append(s)
                    h["semboller"] = h["semboller"][:5]
                    if sm_oz: h["sektor_ozet"] = sm_oz
                    if sm_yon != "NÖTR" and h["yon"] == "NÖTR": h["yon"] = sm_yon
                    # Stream
                    h["stream"] = stream_belirle(h)
                    # Novelty
                    ek = h["semboller"][0] if h["semboller"] else "GENEL"
                    h["novelty"] = nov.skorla(ek, h["baslik"])
                    # ML skor
                    h["ml_skor"] = ml.skor(h)
                    # Momentum
                    momentum.ekle(h["baslik"])
                    # ACİL?
                    if acil_mi(h["baslik"], mod):
                        h["skor"] = 9
                        acil_kuyruk.append((h, mod))
                    else:
                        kuyruk.append(h)

            isle(g_raw, bekl_global, "global")
            isle(t_raw, bekl_turkey, "turkey")

            # ACİL — hemen gönder
            if acil_kuyruk:
                log.info(f"🚨 {len(acil_kuyruk)} ACİL!")
                for h, mod in acil_kuyruk:
                    try:
                        haber_id = db_kaydet(h, mod, gonderildi=1)
                        sent_msg = await bot.send_message(
                            chat_id=CHAT_ID,
                            text=mesaj_olustur(h, mod, acil=True),
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                            reply_markup=feedback_keyboard(haber_id)
                        )
                        db_msg_id_guncelle(haber_id, sent_msg.message_id)
                        toplam_gonderilen += 1
                        await asyncio.sleep(1)
                    except Exception as ex:
                        log.error(f"Acil gönderme: {ex}")

            toplam = len(bekl_global) + len(bekl_turkey)
            if toplam > 0:
                log.info(f"📥 Kuyruk: 🌍{len(bekl_global)} | 🇹🇷{len(bekl_turkey)}")

            # AI skorlama
            yeterli = len(bekl_global) >= 5 or len(bekl_turkey) >= 5
            if toplam > 0 and (yeterli or (now - son_ai) >= AI_INTERVAL):
                son_ai = now
                gonderilenler: list = []

                if bekl_global:
                    sk = claude_skore_et(bekl_global[:20], "global")
                    for h in sk:
                        h["fs"] = h.get("skor",0) * h.get("novelty",1.0) * (1 + h.get("ml_skor",0.5))
                    gonderilenler += [(h,"global") for h in
                        sorted([h for h in sk if h.get("skor",0)>=GLOBAL_THRESH],
                               key=lambda x:x["fs"], reverse=True)[:MAX_GLOBAL]]
                    bekl_global = []

                if bekl_turkey:
                    sk = claude_skore_et(bekl_turkey[:25], "turkey")
                    for h in sk:
                        h["fs"] = h.get("skor",0) * h.get("novelty",1.0) * (1 + h.get("ml_skor",0.5))
                    gonderilenler += [(h,"turkey") for h in
                        sorted([h for h in sk if h.get("skor",0)>=TURKEY_THRESH],
                               key=lambda x:x["fs"], reverse=True)[:MAX_TURKEY]]
                    bekl_turkey = []

                gonderilenler.sort(key=lambda x: x[0].get("fs",0), reverse=True)

                if gonderilenler:
                    log.info(f"✅ {len(gonderilenler)} haber")
                    for h, mod in gonderilenler:
                        try:
                            haber_id = db_kaydet(h, mod, gonderildi=1)
                            sent_msg = await bot.send_message(
                                chat_id=CHAT_ID,
                                text=mesaj_olustur(h, mod),
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True,
                                reply_markup=feedback_keyboard(haber_id)
                            )
                            db_msg_id_guncelle(haber_id, sent_msg.message_id)
                            toplam_gonderilen += 1
                            log.info(f"[{h['skor']}][{h.get('stream','?')}] {h['baslik'][:65]}")
                            await asyncio.sleep(1.5)
                        except Exception as ex:
                            log.error(f"Gönderme: {ex}")
                else:
                    log.info("— Eşik yok")

            # Fiyat radar (her 5 dk)
            if (now - son_fiyat) >= PRICE_INTERVAL:
                son_fiyat = now
                await fiyat_radar.kontrol(bot)

            # Macro engine (her 60 sn)
            if (now - son_macro) >= MACRO_INTERVAL:
                son_macro = now
                await macro_engine.kontrol(bot)

            # Momentum alarmları
            for kume_adi, n, son_h in momentum.kontrol():
                try:
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=momentum.alarm_mesaj(kume_adi, n, son_h),
                        parse_mode=ParseMode.HTML
                    )
                    log.info(f"⚡ Momentum: {kume_adi} ({n})")
                except Exception as ex:
                    log.error(f"Momentum: {ex}")

            # Feedback kontrolü (her 60 sn)
            if (now - son_feedback) >= FEEDBACK_INTERVAL:
                son_feedback = now
                await feedback_kontrol(bot)

            # ML veri güncelle (her 20 dk)
            if (now - son_ml_guncelle) >= 1200:
                son_ml_guncelle = now
                ml.fiyat_guncelle([("move_5m",5), ("move_15m",15), ("move_60m",60)])
                if ml.son_egitim is None or (simdi-ml.son_egitim).days >= 7:
                    await asyncio.get_event_loop().run_in_executor(None, ml.egit)

            # Sabah brifing (09:45)
            if simdi.hour == 9 and simdi.minute == 45:
                bugun_str = simdi.strftime("%Y%m%d")
                if son_sabah_brifing != bugun_str:
                    son_sabah_brifing = bugun_str
                    await sabah_brifing(bot)

            # Akşam özeti (17:45)
            if simdi.hour == 17 and simdi.minute == 45:
                bugun_str = simdi.strftime("%Y%m%d")
                if son_aksam_ozeti != bugun_str:
                    son_aksam_ozeti = bugun_str
                    await aksam_ozeti(bot)

            # 6 saatlik sistem raporu
            if (simdi - son_durum).total_seconds() > 21600:
                son_durum = simdi
                try:
                    con  = sqlite3.connect(DB_PATH)
                    tk   = con.execute("SELECT COUNT(*) FROM haberler").fetchone()[0]
                    ml_v = con.execute(
                        "SELECT COUNT(*) FROM haberler WHERE relative_move IS NOT NULL"
                    ).fetchone()[0]
                    fb_c = con.execute(
                        "SELECT COUNT(*) FROM haberler WHERE feedback IS NOT NULL"
                    ).fetchone()[0]
                    con.close()
                except: tk, ml_v, fb_c = 0, 0, 0
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=(
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📊 <b>SİSTEM RAPORU</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"⏱ Çalışma: {int((simdi-baslangic).total_seconds()//3600)} saat\n"
                        f"📨 Gönderilen: {toplam_gonderilen} haber\n"
                        f"🗄 DB kayıt: {tk}\n"
                        f"🤖 ML verisi: {ml_v}/{ml.MIN_VERI}\n"
                        f"🤖 ML aktif: {'✅' if ml.trained else '⏳ Birikim devam'}\n"
                        f"📋 Feedback: {fb_c} etiket\n"
                        f"⏰ {simdi.strftime('%d.%m.%Y %H:%M')}"
                    ),
                    parse_mode=ParseMode.HTML
                )

            # Hash temizle
            if len(gonderilen) > 10000:
                gonderilen.clear()
                log.info("🧹 Hash seti temizlendi")

        except Exception as e:
            log.error(f"Döngü hatası: {e}")
            await asyncio.sleep(30)

        await asyncio.sleep(POLL_INTERVAL)

asyncio.run(ana_dongu())
