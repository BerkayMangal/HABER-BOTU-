"""
╔══════════════════════════════════════════════╗
║     BERKAY TERMINATOR v3.0 — RAILWAY         ║
║     ML + Sektör Matrisi + Fiyat Radar        ║
╚══════════════════════════════════════════════╝
"""

import asyncio, feedparser, requests, hashlib, json, logging
import time, re, os, random, sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
from bs4 import BeautifulSoup
import anthropic
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telegram import Bot
from telegram.constants import ParseMode

try:
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
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
POLL_INTERVAL  = 12
AI_INTERVAL    = 35
MAX_GLOBAL     = 3
MAX_TURKEY     = 8
TURKEY_THRESH  = 7
GLOBAL_THRESH  = 8
NOVELTY_HOURS  = 24
PRICE_INTERVAL = 300

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
# 🎯 SEKTÖR MATRİSİ
# ════════════════════════════════════════════════
SEKTOR_MATRISI = [
    (["petrol fiyat arttı","brent yükseldi","ham petrol","opec","hürmüz","iran petrol ambargo"],
     ["TUPRS","PETKM","AYGAZ"], "BEARISH", "Petrol maliyeti artar → marj baskısı"),
    (["petrol düştü","petrol geriledi","brent geriledi","opec üretim arttı"],
     ["TUPRS","PETKM","AYGAZ"], "BULLISH", "Petrol maliyeti azalır → marj genişler"),
    (["savunma ihalesi","ssb sözleşme","siha","insansız hava","füze sözleşme","savunma projesi"],
     ["ASELS"], "BULLISH", "Savunma siparişi → gelir artışı"),
    (["savaş ilan","askeri operasyon","kara harekâtı","bombardıman başladı","çatışma tırmandı","nükleer"],
     ["ASELS","KOZAL"], "MIXED", "Jeopolitik risk → güvenli liman + savunma"),
    (["altın yükseldi","altın rekor","gold rally","xau yükseldi"],
     ["KOZAL"], "BULLISH", "Altın fiyatı artar → gelir artar"),
    (["altın düştü","gold drops","altın geriledi"],
     ["KOZAL"], "BEARISH", "Altın fiyatı düşer → gelir azalır"),
    (["çelik fiyat arttı","demir fiyat arttı","steel price up","hrc arttı"],
     ["EREGL"], "BULLISH", "Çelik fiyatı artar → karlılık artar"),
    (["çelik fiyat düştü","steel drops","demir düştü"],
     ["EREGL"], "BEARISH", "Çelik fiyatı düşer → karlılık azalır"),
    (["faiz artırdı","faiz artış sürprizi","tcmb sıkılaştı","hawkish","baz puan artış"],
     ["GARAN","AKBNK","ISCTR","YKBNK"], "BEARISH", "Faiz artışı → banka marjlarını sıkar"),
    (["faiz indirdi","faiz indirim","tcmb gevşedi","dovish","baz puan indirim"],
     ["GARAN","AKBNK","ISCTR","YKBNK"], "BULLISH", "Faiz indirimi → banka karlılığı artar"),
    (["dolar sert yükseldi","kur tırmandı","tl değer kaybetti","döviz krizi","tl çöktü"],
     ["THYAO","FROTO","EREGL","TUPRS"], "BULLISH", "TL zayıflar → ihracatçı kazanır"),
    (["dolar sert yükseldi","kur tırmandı","tl değer kaybetti"],
     ["BIMAS","MGROS","ARCLK"], "BEARISH", "TL zayıflar → ithalatçı maliyet artar"),
    (["turizm rekoru","turist sayısı arttı","yolcu rekoru","koltuk doluluk arttı"],
     ["THYAO","PGSUS","TAVHL"], "BULLISH", "Turizm güçlü → yolcu geliri artar"),
    (["büyük deprem","şiddetli deprem","yıkıcı deprem","depremin büyüklüğü"],
     ["EKGYO","ENKAI"], "BEARISH", "Deprem → inşaat/GYO riski"),
    (["elektrik zammı","doğalgaz zammı","epdk kararı","enerji fiyat"],
     ["TUPRS","PETKM","TCELL","TTKOM"], "BEARISH", "Enerji maliyeti artar"),
    (["ihracat rekoru","dış ticaret fazlası","ihracat güçlü"],
     ["FROTO","TOASO","EREGL","ARCLK"], "BULLISH", "İhracat güçlü → döviz geliri artar"),
    (["konkordato","iflas başvurusu","faaliyetleri durdurdu","haciz kararı"],
     [], "BEARISH", "Şirket finansal sıkıntı"),
]

def sektor_analiz(baslik):
    b = baslik.lower()
    tum_hisseler, aciklama, yon = [], "", "NÖTR"
    for keywords, hisseler, _yon, _ac in SEKTOR_MATRISI:
        if any(kw in b for kw in keywords):
            for h in hisseler:
                if h not in tum_hisseler:
                    tum_hisseler.append(h)
            if not aciklama:
                aciklama, yon = _ac, _yon
    return tum_hisseler[:5], yon, aciklama

# ════════════════════════════════════════════════
# 🚨 ACİL KEYWORD'LER
# ════════════════════════════════════════════════
ACIL_TR = [
    "tcmb acil toplantı","merkez bankası acil","olağanüstü para kurulu",
    "faiz kararı açıklandı","faiz kararı bugün",
    "büyük deprem","şiddetli deprem","yıkıcı deprem","depremin büyüklüğü 6",
    "iflas başvurusu","konkordato ilan","spk işlem durdurdu","spk borsa durdurdu",
    "türkiye'ye yaptırım","yaptırım paketi türkiye","türkiye ambargo",
    "sıkıyönetim ilan","olağanüstü hal ilan","sokağa çıkma yasağı",
    "tl kriz","kur 40","kur 45","dolar tavan","lira çöktü",
    "bedelsiz sermaye artırımı","temettü dağıtım tarihi","pay geri alım programı",
    "satın alma anlaşması imzalandı","devralma tamamlandı","birleşme onayı",
    "halka arz fiyatı belirlendi","ipo bugün","halka arz iptal edildi",
]

ACIL_GLOBAL = [
    "fed emergency meeting","emergency rate cut","fomc emergency",
    "war declared","nuclear strike","nato article 5 invoked",
    "strait of hormuz closed","oil embargo","opec emergency cut",
    "market circuit breaker","trading halted nyse","stock market crash",
    "sanctions on turkey","iran nuclear deal collapse","israel iran ground war",
    "sovereign default","imf emergency bailout","lehman",
]

def acil_mi(baslik, mod):
    b = baslik.lower()
    return any(kw in b for kw in (ACIL_TR if mod == "turkey" else ACIL_GLOBAL))

# ════════════════════════════════════════════════
# 📡 RSS KAYNAKLARI
# ════════════════════════════════════════════════
RSS_GLOBAL = [
    ("Reuters",          "https://feeds.reuters.com/reuters/topNews"),
    ("Reuters Biz",      "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters EM",       "https://feeds.reuters.com/reuters/emergingMarketsNews"),
    ("Reuters Dünya",    "https://feeds.reuters.com/reuters/worldNews"),
    ("AP News",          "https://rsshub.app/apnews/topics/ap-top-news"),
    ("BBC Dünya",        "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("BBC Ekonomi",      "https://feeds.bbci.co.uk/news/business/rss.xml"),
    ("CNBC",             "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("CNBC Markets",     "https://www.cnbc.com/id/15839135/device/rss/rss.html"),
    ("ZeroHedge",        "https://feeds.feedburner.com/zerohedge/feed"),
    ("FED",              "https://www.federalreserve.gov/feeds/press_all.xml"),
    ("ECB",              "https://www.ecb.europa.eu/rss/press.html"),
    ("EIA Petrol",       "https://www.eia.gov/rss/todayinenergy.xml"),
    ("OilPrice",         "https://oilprice.com/rss/main"),
    ("ISW",              "https://www.understandingwar.org/rss.xml"),
    ("Al Monitor TR",    "https://www.al-monitor.com/rss/turkey.xml"),
    ("Al Monitor ME",    "https://www.al-monitor.com/rss/mideast.xml"),
    ("IAEA",             "https://www.iaea.org/feeds/topstories.xml"),
    ("ABD Dışişleri",    "https://www.state.gov/rss-feed/press-releases/feed/"),
    ("NATO",             "https://www.nato.int/cps/en/natolive/news.rss"),
    ("USGS Deprem",      "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.atom"),
    ("BIS",              "https://www.bis.org/doclist/bis_fsr.rss"),
    ("IMF",              "https://www.imf.org/en/News/rss?language=eng"),
]

RSS_TURKEY = [
    ("TCMB",             "https://www.tcmb.gov.tr/wps/wcm/connect/TR/rss/"),
    ("SPK",              "https://www.spk.gov.tr/rss/HaberDetay"),
    ("BDDK",             "https://www.bddk.org.tr/Rss/RssDuyuru"),
    ("Hazine",           "https://www.hmb.gov.tr/rss"),
    ("Borsa İstanbul",   "https://www.borsaistanbul.com/tr/rss/haberler"),
    ("Resmi Gazete",     "https://www.resmigazete.gov.tr/rss/tum.xml"),
    ("BTK",              "https://www.btk.gov.tr/rss"),
    ("Rekabet Kurumu",   "https://www.rekabet.gov.tr/tr/Rss/Karar"),
    ("AA Ekonomi",       "https://www.aa.com.tr/tr/rss/default?cat=ekonomi"),
    ("AA Gündem",        "https://www.aa.com.tr/tr/rss/default?cat=gundem"),
    ("DHA Ekonomi",      "https://www.dha.com.tr/rss/ekonomi.xml"),
    ("İHA Ekonomi",      "https://www.iha.com.tr/rss/ekonomi.xml"),
    ("Bloomberg HT",     "https://www.bloomberght.com/rss"),
    ("Dünya",            "https://www.dunya.com/rss/rss.xml"),
    ("Ekonomim",         "https://www.ekonomim.com/rss"),
    ("Para Analiz",      "https://www.paraanaliz.com/feed/"),
    ("Finans Gündem",    "https://www.finansgundem.com/rss"),
    ("Anka Haber",       "https://www.ankahaber.net/rss.xml"),
    ("NTV Ekonomi",      "https://www.ntv.com.tr/ekonomi.rss"),
    ("Hürriyet Ekon",    "https://www.hurriyet.com.tr/rss/ekonomi"),
    ("Sabah Ekonomi",    "https://www.sabah.com.tr/rss/ekonomi.xml"),
    ("Cumhuriyet Ekon",  "https://www.cumhuriyet.com.tr/rss/ekonomi.xml"),
    ("Sözcü Ekonomi",    "https://www.sozcu.com.tr/rss/ekonomi.xml"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}

# ════════════════════════════════════════════════
# 🗄️ SQLite
# ════════════════════════════════════════════════
DB_PATH = "/app/terminator.db"

def db_init():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS haberler (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        zaman TEXT, kaynak TEXT, baslik TEXT, url TEXT,
        ai_skor INTEGER, ml_skor REAL, yon TEXT,
        semboller TEXT, novelty REAL, mod TEXT,
        gonderildi INTEGER DEFAULT 0,
        gercek_hareket REAL
    )""")
    con.execute("""CREATE TABLE IF NOT EXISTS fiyat_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        zaman TEXT, sembol TEXT, fiyat REAL
    )""")
    con.commit()
    con.close()

def db_kaydet(h, mod, gonderildi=0):
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("""INSERT INTO haberler
            (zaman,kaynak,baslik,url,ai_skor,ml_skor,yon,semboller,novelty,mod,gonderildi)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (h["zaman"], h["kaynak"], h["baslik"], h.get("url",""),
             h.get("skor",0), h.get("ml_skor",0.5), h.get("yon","NÖTR"),
             json.dumps(h.get("semboller",[])), h.get("novelty",1.0), mod, gonderildi))
        con.commit()
        con.close()
    except Exception as e:
        log.debug(f"DB: {e}")

# ════════════════════════════════════════════════
# 🤖 ML MODELİ
# ════════════════════════════════════════════════
class MLModel:
    def __init__(self):
        self.model      = None
        self.trained    = False
        self.son_egitim = None
        self.MIN_VERI   = 100

    def ozellik_cikar(self, h):
        b = h["baslik"].lower()
        return [
            h.get("skor",0) / 10.0,
            h.get("novelty",1.0),
            len(h.get("semboller",[])) / 5.0,
            1.0 if h.get("yon")=="BULLISH" else (-1.0 if h.get("yon")=="BEARISH" else 0.0),
            1.0 if h.get("kaynak_tip")=="sirket" else 0.0,
            1.0 if any(k in b for k in ["tcmb","merkez bankası","faiz"]) else 0.0,
            1.0 if any(k in b for k in ["deprem","afet","patlama"]) else 0.0,
            1.0 if any(k in b for k in ["temettü","bedelsiz","geri alım","kap"]) else 0.0,
            1.0 if any(k in b for k in ["savaş","operasyon","füze","nato"]) else 0.0,
            1.0 if any(k in b for k in ["petrol","brent","opec","enerji"]) else 0.0,
            1.0 if any(k in b for k in ["dolar","kur","tl","döviz"]) else 0.0,
            1.0 if any(k in b for k in ["fed","ecb","faiz kararı","baz puan"]) else 0.0,
            1.0 if any(k in b for k in ["altın","gold","xau"]) else 0.0,
            min(len(h["baslik"]) / 200.0, 1.0),
        ]

    def egit(self):
        if not ML_AVAILABLE: return
        try:
            con = sqlite3.connect(DB_PATH)
            rows = con.execute("""
                SELECT ai_skor,novelty,semboller,yon,kaynak_tip,baslik,gercek_hareket
                FROM haberler WHERE gercek_hareket IS NOT NULL AND gonderildi=1
            """).fetchall()
            con.close()
            if len(rows) < self.MIN_VERI:
                log.info(f"🤖 ML: {len(rows)}/{self.MIN_VERI} veri — birikim devam")
                return
            X = [self.ozellik_cikar({
                "skor":row[0],"novelty":row[1],"semboller":json.loads(row[2] or "[]"),
                "yon":row[3],"kaynak_tip":row[4],"baslik":row[5]
            }) for row in rows]
            y = [1 if (row[6] or 0) > 0.5 else 0 for row in rows]
            self.model = GradientBoostingClassifier(n_estimators=50, max_depth=3, random_state=42)
            self.model.fit(X, y)
            self.trained    = True
            self.son_egitim = datetime.now()
            acc = sum(1 for i,xi in enumerate(X) if self.model.predict([xi])[0]==y[i]) / len(y)
            log.info(f"🤖 ML eğitildi! {len(rows)} örnek | accuracy={acc:.2f}")
        except Exception as e:
            log.warning(f"ML egit: {e}")

    def skor(self, h):
        if not self.trained or not ML_AVAILABLE: return 0.5
        try:
            return round(self.model.predict_proba([self.ozellik_cikar(h)])[0][1], 3)
        except: return 0.5

    def fiyat_guncelle(self):
        if not ML_AVAILABLE: return
        try:
            sinir = (datetime.now() - timedelta(minutes=25)).isoformat()
            con   = sqlite3.connect(DB_PATH)
            rows  = con.execute("""
                SELECT id,semboller FROM haberler
                WHERE gonderildi=1 AND gercek_hareket IS NULL AND zaman > ? LIMIT 15
            """, (sinir,)).fetchall()
            con.close()
            for haber_id, semboller_json in rows:
                syms = json.loads(semboller_json or "[]")
                if not syms: continue
                try:
                    r = requests.get(
                        "https://finnhub.io/api/v1/quote",
                        params={"symbol":f"XIST:{syms[0]}","token":FINNHUB_KEY}, timeout=5
                    ).json()
                    dp = r.get("dp")
                    if dp is not None:
                        con2 = sqlite3.connect(DB_PATH)
                        con2.execute("UPDATE haberler SET gercek_hareket=? WHERE id=?", (dp, haber_id))
                        con2.commit()
                        con2.close()
                except: pass
        except Exception as e:
            log.debug(f"fiyat_guncelle: {e}")

ml = MLModel()

# ════════════════════════════════════════════════
# 💹 FİYAT ANOMALİ RADAR
# ════════════════════════════════════════════════
class FiyatRadar:
    def __init__(self):
        self.onceki = {}
        self.ESIKLER = {"BRENT":1.5, "GOLD":1.0, "USDTRY":0.8}

    async def kontrol(self, bot):
        veriler = {}
        try:
            for sym, label in [("OANDA:USDTRY","USDTRY"),("OANDA:XAUUSD","GOLD")]:
                r = requests.get(
                    "https://finnhub.io/api/v1/quote",
                    params={"symbol":sym,"token":FINNHUB_KEY}, timeout=6
                ).json()
                if r.get("c"): veriler[label] = r["c"]
        except: pass

        for label, fiyat in veriler.items():
            if label in self.onceki and self.onceki[label]:
                degisim = abs((fiyat - self.onceki[label]) / self.onceki[label] * 100)
                if degisim >= self.ESIKLER.get(label, 1.5):
                    yon = "📈" if fiyat > self.onceki[label] else "📉"
                    isimler = {"BRENT":"🛢️ Brent Ham Petrol","GOLD":"🥇 Altın XAU/USD","USDTRY":"💵 USD/TRY"}
                    etkiler = {
                        "BRENT": "TUPRS · PETKM · AYGAZ",
                        "GOLD": "KOZAL",
                        "USDTRY": "📈 THYAO·FROTO·EREGL  |  📉 BIMAS·MGROS·ARCLK"
                    }
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=(
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"💹 <b>FİYAT ANOMALİSİ</b> {yon}\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                            f"📌 <b>{isimler.get(label, label)}</b>\n"
                            f"   Önce: <code>{self.onceki[label]:.3f}</code>\n"
                            f"   Şimdi: <code>{fiyat:.3f}</code>\n"
                            f"   Hareket: <b>{yon} %{degisim:.2f}</b>\n\n"
                            f"🎯 Etkilenecek: {etkiler.get(label,'—')}\n"
                            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
                        ),
                        parse_mode=ParseMode.HTML
                    )
                    log.info(f"💹 ALARM: {label} %{degisim:.1f}")
            self.onceki[label] = fiyat

fiyat_radar = FiyatRadar()

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

    def skorla(self, entity, baslik):
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

    def entity_bul(self, baslik):
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
# 📥 TOPLAYICILAR
# ════════════════════════════════════════════════
def yeni_h(kaynak, baslik, url, tip="rss"):
    return {
        "kaynak": kaynak, "baslik": baslik.strip()[:260],
        "url": url, "zaman": datetime.now().strftime("%H:%M"),
        "skor": 0, "ml_skor": 0.5, "yon": "NÖTR",
        "semboller": [], "sektor_ozet": "",
        "ozet": "", "novelty": 1.0, "kaynak_tip": tip
    }

def rss_cek(feeds):
    out = []
    for kaynak, url in feeds:
        try:
            feed = feedparser.parse(url, request_headers=HEADERS)
            for e in feed.entries[:6]:
                b = (e.get("title") or "").strip()
                if b and len(b) > 10:
                    out.append(yeni_h(kaynak, b, e.get("link","")))
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
            timeout=12, headers=HEADERS)
        for item in r.json().get("data",[]):
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

def finnhub_genel_cek():
    out = []
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/news",
            params={"category":"general","token":FINNHUB_KEY},
            timeout=10, headers=HEADERS)
        for item in r.json()[:10]:
            b = item.get("headline","")
            if b:
                out.append(yeni_h(item.get("source","Finnhub"), b, item.get("url",""), "api"))
    except Exception as e:
        log.warning(f"Finnhub: {e}")
    return out

def finnhub_bist_cek():
    out = []
    dun   = (datetime.now()-timedelta(days=3)).strftime("%Y-%m-%d")
    bugun = datetime.now().strftime("%Y-%m-%d")
    for s in random.sample(BIST30, 6):
        try:
            r = requests.get(
                "https://finnhub.io/api/v1/company-news",
                params={"symbol":f"XIST:{s}","from":dun,"to":bugun,"token":FINNHUB_KEY},
                timeout=7, headers=HEADERS)
            for item in r.json()[:2]:
                b = item.get("headline","")
                if b:
                    h = yeni_h(f"Finnhub [{s}]", f"[{s}] {b}", item.get("url",""), "sirket")
                    h["semboller"] = [s]
                    out.append(h)
            time.sleep(0.1)
        except: pass
    return out

def sirket_haberleri_cek():
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
def claude_skore_et(haberler, mod):
    if not haberler: return []
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        liste  = "\n".join([f"{i+1}. {h['baslik']}" for i,h in enumerate(haberler)])
        if mod == "global":
            prompt = f"""Sen BIST/VİOP uzmanı Türk tradersin. Global haberleri BIST etkisi açısından skorla.

PUANLAMA:
9-10 = Piyasayı ANINDA etkiler: Fed acil, büyük savaş, Hürmüz kapanır, küresel kriz
7-8  = Önemli: Fed/ECB sürpriz, petrol %3+, İsrail-İran tırmanması, jeopolitik şok
5-6  = Orta: Beklenen veri, bölgesel gerilim
0-4  = Gürültü: Şirket haberi, magazin, rutin

SADECE JSON:
[{{"id":1,"skor":9,"yon":"BEARISH","semboller":["TUPRS"],"ozet":"Türkçe max 12 kelime"}}]

Haberler:
{liste}"""
        else:
            prompt = f"""Sen BIST/VİOP uzmanı Türk tradersin. Türkiye haberlerini BIST etkisi açısından skorla.
BIST30: {', '.join(BIST30)}
[SEMBOL] ile başlayanlar = KAP/şirket haberi!

PUANLAMA:
9-10 = Acil: TCMB sürpriz faiz, TL kriz, yaptırım, deprem, olağanüstü hal
8    = Çok önemli: BIST30 KAP kararı, enflasyon/büyüme verisi, TCMB/SPK/BDDK kararı
7    = Önemli: Orta şirket haberi, sektör kararı, regülasyon
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
                haberler[idx].update({
                    "skor":      max(0, min(10, int(s.get("skor",0)))),
                    "yon":       s.get("yon","NÖTR"),
                    "semboller": mevcut if mevcut else s.get("semboller",[]),
                    "ozet":      s.get("ozet","")
                })
    except Exception as e:
        log.warning(f"Claude ({mod}): {e}")
    return haberler

# ════════════════════════════════════════════════
# 💬 MESAJ FORMATI
# ════════════════════════════════════════════════
KAYNAK_IKONLARI = {
    "Reuters":"🔴","AP News":"🔴","BBC":"🟣","CNBC":"🟡","ZeroHedge":"🐻",
    "FED":"🏦","ECB":"🏦","BIS":"🏦","IMF":"🏦",
    "EIA":"⛽","OilPrice":"🛢️","ISW":"⚔️","Al Monitor":"🌙",
    "IAEA":"☢️","NATO":"🛡️","ABD Dışişleri":"🦅","USGS":"🌍",
    "TCMB":"🏛️","SPK":"🏛️","BDDK":"🏛️","Hazine":"🏛️","BTK":"🏛️",
    "Rekabet":"⚖️","Borsa İstanbul":"📊","Resmi Gazete":"📜",
    "AA":"📡","DHA":"📡","İHA":"📡",
    "Bloomberg HT":"💹","Dünya":"💹","Para Analiz":"💹","Ekonomim":"💹",
    "Finans Gündem":"💹","Anka":"💹","NTV":"📺","Hürriyet":"📰",
    "Sabah":"📰","Cumhuriyet":"📰","Sözcü":"📰",
    "Marketaux":"📊","Finnhub":"📈","Foreks":"📈","BorsaGündem":"📈",
    "Investing":"📈","KAP":"📋",
}

def kaynak_ikon(kaynak):
    for k, v in KAYNAK_IKONLARI.items():
        if k.lower() in kaynak.lower(): return v
    return "📌"

def hash_h(t):
    return hashlib.md5(t.encode("utf-8","ignore")).hexdigest()[:12]

def mesaj_olustur(h, mod, acil=False):
    skor   = h.get("skor", 0)
    yon    = h.get("yon", "NÖTR")
    syms   = h.get("semboller", [])
    ozet   = h.get("ozet", "")
    nv     = h.get("novelty", 1.0)
    ml_s   = h.get("ml_skor", 0.5)
    sk_oz  = h.get("sektor_ozet", "")
    flag   = "🇹🇷" if mod=="turkey" else "🌍"

    if acil or skor >= 9:
        seviye, renk = "🚨 <b>ACİL ALARM</b>", "🔴"
    elif skor >= 8:
        seviye, renk = "⚡ <b>ÖNEMLİ</b>", "🟡"
    elif skor >= 7:
        seviye, renk = "🔵 <b>TAKİP</b>", "🔵"
    else:
        seviye, renk = "⚪ <b>BİLGİ</b>", "⚪"

    yon_str = {"BULLISH":"📈 BULLISH","BEARISH":"📉 BEARISH",
               "NÖTR":"➡️ NÖTR","MIXED":"↔️ MIXED"}.get(yon, yon)
    nb  = "🆕" if nv>=0.9 else ("🔄" if nv>=0.5 else "♻️")
    ki  = kaynak_ikon(h["kaynak"])
    ss  = "  ".join([f"<code>#{s}</code>" for s in syms[:5]])

    if ml.trained and ml_s >= 0.7:
        ml_line = f"\n🤖 <b>ML: %{int(ml_s*100)} hareket ihtimali</b>"
    elif ml.trained and ml_s >= 0.5:
        ml_line = f"\n🤖 ML: %{int(ml_s*100)}"
    else:
        ml_line = ""

    msg  = f"━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"{renk}{flag} {seviye}  <b>[{skor}/10]</b>  {nb}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"{ki} <b>{h['kaynak']}</b>  ·  ⏰ <code>{h['zaman']}</code>\n\n"
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
# 🔄 ANA DÖNGÜ
# ════════════════════════════════════════════════
gonderilen        = set()
bekl_global       = []
bekl_turkey       = []
son_ai            = 0.0
son_fiyat         = 0.0
son_ml_guncelle   = 0.0
son_durum         = datetime.now()
dongu_sayac       = 0
toplam_gonderilen = 0
baslangic         = datetime.now()
telegram_kuyruk   = []

async def ana_dongu():
    global bekl_global, bekl_turkey, son_ai, son_fiyat, son_ml_guncelle
    global dongu_sayac, toplam_gonderilen, son_durum, telegram_kuyruk

    db_init()
    bot = Bot(token=TELEGRAM_TOKEN)
    telethon_aktif = False

    if TELETHON_SESSION and TELEGRAM_KANALLARI:
        try:
            tg = TelegramClient(StringSession(TELETHON_SESSION), TELEGRAM_API_ID, TELEGRAM_API_HASH)
            await tg.start()
            telethon_aktif = True

            @tg.on(events.NewMessage(chats=TELEGRAM_KANALLARI))
            async def tg_handler(event):
                m = event.raw_text
                if m and len(m) > 15:
                    um = re.search(r'https?://\S+', m)
                    b  = m[:260].replace("\n"," ").strip()
                    h  = yeni_h(f"📲 {getattr(event.chat,'title','Telegram')}",
                               b, um.group(0) if um else "", "telegram")
                    h["semboller"] = nov.entity_bul(b)
                    telegram_kuyruk.append(h)
        except Exception as e:
            log.warning(f"Telethon: {e}")

    await bot.send_message(
        chat_id=CHAT_ID,
        text=(
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🤖 <b>BERKAY TERMINATOR v3.0</b>\n"
            f"🗓 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "<b>SİSTEMLER:</b>\n"
            f"{'✅' if telethon_aktif else '⚪'} Telethon\n"
            "✅ Sektör Matrisi (BIST30)\n"
            "✅ Fiyat Radar (Brent/Altın/TRY)\n"
            "✅ ML Model (veri birikince)\n"
            "✅ Novelty Memory 24h\n"
            "✅ SQLite Veri Kaydı\n\n"
            "<b>KAYNAKLAR:</b>\n"
            f"🌍 {len(RSS_GLOBAL)} global  |  🇹🇷 {len(RSS_TURKEY)} Türkiye\n"
            "⚔️ ISW · Al Monitor · IAEA · NATO\n"
            "🌍 USGS Deprem · EIA · IMF · BIS\n"
            "🏛️ TCMB · SPK · BDDK · BTK\n\n"
            "🔴 ACİL [9-10]  🟡 ÖNEMLİ [8]  🔵 TAKİP [7]\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📡 İzleme başladı..."
        ),
        parse_mode=ParseMode.HTML
    )
    log.info("✅ TERMINATOR v3.0 BAŞLADI!")

    while True:
        try:
            now = time.time()
            dongu_sayac += 1

            g_raw = rss_cek(RSS_GLOBAL) + finnhub_genel_cek()
            t_raw = rss_cek(RSS_TURKEY) + marketaux_cek()

            if dongu_sayac % 4 == 0:
                sirket = sirket_haberleri_cek() + finnhub_bist_cek()
                t_raw += sirket
                if sirket: log.info(f"📈 {len(sirket)} şirket haberi")

            if telegram_kuyruk:
                t_raw += telegram_kuyruk[:30]
                telegram_kuyruk.clear()

            acil_kuyruk = []

            def isle(haberler, kuyruk, mod):
                for h in haberler:
                    hsh = hash_h(h["baslik"])
                    if hsh not in gonderilen and h["baslik"] and len(h["baslik"]) > 10:
                        gonderilen.add(hsh)
                        sm_syms, sm_yon, sm_oz = sektor_analiz(h["baslik"])
                        if not h["semboller"]:
                            h["semboller"] = nov.entity_bul(h["baslik"])
                        for s in sm_syms:
                            if s not in h["semboller"]: h["semboller"].append(s)
                        h["semboller"] = h["semboller"][:5]
                        if sm_oz: h["sektor_ozet"] = sm_oz
                        if sm_yon != "NÖTR" and h["yon"] == "NÖTR": h["yon"] = sm_yon
                        ek = h["semboller"][0] if h["semboller"] else "GENEL"
                        h["novelty"]  = nov.skorla(ek, h["baslik"])
                        h["ml_skor"]  = ml.skor(h)
                        if acil_mi(h["baslik"], mod):
                            h["skor"] = 9
                            acil_kuyruk.append((h, mod))
                        else:
                            kuyruk.append(h)

            isle(g_raw, bekl_global, "global")
            isle(t_raw, bekl_turkey, "turkey")

            if acil_kuyruk:
                log.info(f"🚨 {len(acil_kuyruk)} ACİL!")
                for h, mod in acil_kuyruk:
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=mesaj_olustur(h, mod, acil=True),
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True
                        )
                        toplam_gonderilen += 1
                        db_kaydet(h, mod, gonderildi=1)
                        await asyncio.sleep(1)
                    except Exception as ex:
                        log.error(f"Acil gönderme: {ex}")

            toplam = len(bekl_global) + len(bekl_turkey)
            if toplam > 0:
                log.info(f"📥 Kuyruk: 🌍{len(bekl_global)} | 🇹🇷{len(bekl_turkey)}")

            yeterli = len(bekl_global) >= 5 or len(bekl_turkey) >= 5
            if toplam > 0 and (yeterli or (now-son_ai) >= AI_INTERVAL):
                son_ai = now
                gonderilenler = []

                if bekl_global:
                    sk = claude_skore_et(bekl_global[:20], "global")
                    for h in sk:
                        h["fs"] = h.get("skor",0) * h.get("novelty",1.0) * (1+h.get("ml_skor",0.5))
                    gonderilenler += [(h,"global") for h in
                        sorted([h for h in sk if h.get("skor",0)>=GLOBAL_THRESH],
                               key=lambda x:x["fs"],reverse=True)[:MAX_GLOBAL]]
                    bekl_global = []

                if bekl_turkey:
                    sk = claude_skore_et(bekl_turkey[:25], "turkey")
                    for h in sk:
                        h["fs"] = h.get("skor",0) * h.get("novelty",1.0) * (1+h.get("ml_skor",0.5))
                    gonderilenler += [(h,"turkey") for h in
                        sorted([h for h in sk if h.get("skor",0)>=TURKEY_THRESH],
                               key=lambda x:x["fs"],reverse=True)[:MAX_TURKEY]]
                    bekl_turkey = []

                gonderilenler.sort(key=lambda x:x[0].get("fs",0), reverse=True)

                if gonderilenler:
                    log.info(f"✅ {len(gonderilenler)} haber")
                    for h, mod in gonderilenler:
                        try:
                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=mesaj_olustur(h, mod),
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True
                            )
                            toplam_gonderilen += 1
                            db_kaydet(h, mod, gonderildi=1)
                            log.info(f"[{h['skor']}] {h['baslik'][:70]}")
                            await asyncio.sleep(1.5)
                        except Exception as ex:
                            log.error(f"Gönderme: {ex}")
                else:
                    log.info("— Eşik yok")

            # Fiyat radar
            if (now - son_fiyat) >= PRICE_INTERVAL:
                son_fiyat = now
                await fiyat_radar.kontrol(bot)

            # ML veri güncelle
            if (now - son_ml_guncelle) >= 1200:
                son_ml_guncelle = now
                ml.fiyat_guncelle()
                if ml.son_egitim is None or (datetime.now()-ml.son_egitim).days >= 7:
                    await asyncio.get_event_loop().run_in_executor(None, ml.egit)

            # 6 saatlik rapor
            if (datetime.now()-son_durum).total_seconds() > 21600:
                son_durum = datetime.now()
                try:
                    con = sqlite3.connect(DB_PATH)
                    tk = con.execute("SELECT COUNT(*) FROM haberler").fetchone()[0]
                    ml_v = con.execute("SELECT COUNT(*) FROM haberler WHERE gercek_hareket IS NOT NULL").fetchone()[0]
                    con.close()
                except: tk, ml_v = 0, 0
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=(
                        "━━━━━━━━━━━━━━━━━━━━━━\n"
                        "📊 <b>DURUM RAPORU</b>\n"
                        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"⏱ Çalışma: {int((datetime.now()-baslangic).total_seconds()//3600)} saat\n"
                        f"📨 Gönderilen: {toplam_gonderilen} haber\n"
                        f"🗄 DB kayıt: {tk}\n"
                        f"🤖 ML verisi: {ml_v}/{ml.MIN_VERI}\n"
                        f"🤖 ML aktif: {'✅' if ml.trained else '⏳ Birikim devam'}\n"
                        f"⏰ {datetime.now().strftime('%d.%m.%Y %H:%M')}"
                    ),
                    parse_mode=ParseMode.HTML
                )

            if len(gonderilen) > 8000:
                gonderilen.clear()

        except Exception as e:
            log.error(f"Döngü hatası: {e}")
            await asyncio.sleep(30)

        await asyncio.sleep(POLL_INTERVAL)

asyncio.run(ana_dongu())
