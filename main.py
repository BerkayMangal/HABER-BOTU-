"""
BERKAY TERMINATOR — FINAL
Railway 7/24 deployment
"""

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
from datetime import datetime, timedelta
from collections import defaultdict
from bs4 import BeautifulSoup
import anthropic
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telegram import Bot
from telegram.constants import ParseMode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# KEY'LER — Railway > Variables'dan gelir
# ─────────────────────────────────────────────
TELEGRAM_TOKEN    = os.environ["TELEGRAM_TOKEN"]
CHAT_ID           = os.environ["CHAT_ID"]
ANTHROPIC_KEY     = os.environ["ANTHROPIC_KEY"]
FINNHUB_KEY       = os.environ["FINNHUB_KEY"]
MARKETAUX_KEY     = os.environ["MARKETAUX_KEY"]
TELEGRAM_API_ID   = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELETHON_SESSION  = os.environ.get("TELETHON_SESSION", "")

# ─────────────────────────────────────────────
# AYARLAR
# ─────────────────────────────────────────────
POLL_INTERVAL  = 12    # saniye — her döngü
AI_INTERVAL    = 40    # saniye — min AI çağrı aralığı
MAX_GLOBAL     = 2     # döngü başına max global haber
MAX_TURKEY     = 6     # döngü başına max türkiye haberi
TURKEY_THRESH  = 8     # türkiye eşiği
GLOBAL_THRESH  = 9     # global eşiği
NOVELTY_HOURS  = 24    # novelty hafıza penceresi

# ─────────────────────────────────────────────
# DİNLENECEK TELEGRAM KANALLARI
# Telegram'da arayıp üye olduğun kanalları buraya ekle
# ─────────────────────────────────────────────
TELEGRAM_KANALLARI = [
    # Örnek: "paraanaliz"
    # Örnek: "bloomberght"
]

# ─────────────────────────────────────────────
# BIST30 + ENTİTY SÖZLÜĞÜ
# ─────────────────────────────────────────────
BIST30 = [
    "THYAO","GARAN","AKBNK","ISCTR","YKBNK","TUPRS","EREGL","ASELS",
    "KCHOL","SAHOL","SISE","TOASO","FROTO","PGSUS","KOZAL","EKGYO",
    "BIMAS","MGROS","ULKER","TCELL","TTKOM","ARCLK","VESTL","DOHOL",
    "PETKM","AYGAZ","ENKAI","TAVHL","LOGO","SOKM"
]

ENTITY_DICT = {
    "THYAO": ["türk hava yolları","thy","türk havayolları","turkish airlines","thy.com"],
    "GARAN": ["garanti","garanti bbva","garanti bankası"],
    "AKBNK": ["akbank"],
    "ISCTR": ["iş bankası","işbank","isbank"],
    "YKBNK": ["yapı kredi","yapı ve kredi","yapi kredi"],
    "TUPRS": ["tüpraş","tupras","tüpraş türkiye"],
    "EREGL": ["ereğli","erdemir","eregli","isdemir"],
    "ASELS": ["aselsan"],
    "KCHOL": ["koç holding","koç grubu"],
    "SAHOL": ["sabancı holding","sabancı","sabanci"],
    "SISE":  ["şişecam","sisecam","şişe ve cam"],
    "TOASO": ["tofaş","tofas"],
    "FROTO": ["ford otosan","ford türkiye"],
    "PGSUS": ["pegasus","pegasus hava"],
    "TCELL": ["turkcell"],
    "TTKOM": ["türk telekom","turk telekom","ttnet"],
    "BIMAS": ["bim","bim mağazaları","bim a.ş"],
    "MGROS": ["migros"],
    "ARCLK": ["arçelik","arcelik","beko"],
    "PETKM": ["petkim"],
    "ENKAI": ["enka inşaat","enka"],
    "TAVHL": ["tav havalimanları","tav airports"],
    "KOZAL": ["koza altın","koza anadolu"],
    "EKGYO": ["emlak konut","emlak gyo"],
    "ULKER": ["ülker","ulker"],
    "VESTL": ["vestel"],
    "DOHOL": ["doğuş holding","dogus"],
    "AYGAZ": ["aygaz"],
    "LOGO":  ["logo yazılım","logo siber"],
    "SOKM":  ["şok market","sok market","şok mağazaları"],
}

# ─────────────────────────────────────────────
# NOVELTY MEMORY
# ─────────────────────────────────────────────
class NoveltyMemory:
    def __init__(self):
        self.memory = defaultdict(list)

    def _temizle(self):
        sinir = datetime.now() - timedelta(hours=NOVELTY_HOURS)
        for e in list(self.memory.keys()):
            self.memory[e] = [(kws, ts) for kws, ts in self.memory[e] if ts > sinir]

    def skorla(self, entity, baslik):
        self._temizle()
        kws = set(re.findall(r'\w{4,}', baslik.lower()))
        max_ov = 0.0
        for gecmis_kws, _ in self.memory.get(entity, []):
            if kws and gecmis_kws:
                ov = len(kws & gecmis_kws) / max(len(kws), len(gecmis_kws))
                max_ov = max(max_ov, ov)
        self.memory[entity].append((kws, datetime.now()))
        if max_ov > 0.65: return 0.2
        if max_ov > 0.35: return 0.6
        return 1.0

    def entity_bul(self, baslik):
        b = baslik.lower()
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

# ─────────────────────────────────────────────
# RSS KAYNAKLARI
# ─────────────────────────────────────────────
RSS_GLOBAL = [
    ("🔴 Reuters",       "https://feeds.reuters.com/reuters/topNews"),
    ("🔴 Reuters Biz",   "https://feeds.reuters.com/reuters/businessNews"),
    ("🔴 Reuters EM",    "https://feeds.reuters.com/reuters/emergingMarketsNews"),
    ("⚡ CNBC",          "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("🐻 ZeroHedge",     "https://feeds.feedburner.com/zerohedge/feed"),
    ("🏦 FED",           "https://www.federalreserve.gov/feeds/press_all.xml"),
    ("🏦 ECB",           "https://www.ecb.europa.eu/rss/press.html"),
    ("🌍 BBC Business",  "https://feeds.bbci.co.uk/news/business/rss.xml"),
    ("📊 FT Markets",    "https://www.ft.com/markets?format=rss"),
]

RSS_TURKEY = [
    ("🏛️ TCMB",          "https://www.tcmb.gov.tr/wps/wcm/connect/TR/rss/"),
    ("🏛️ SPK",           "https://www.spk.gov.tr/rss/HaberDetay"),
    ("🏛️ BDDK",          "https://www.bddk.org.tr/Rss/RssDuyuru"),
    ("🏛️ Hazine",        "https://www.hmb.gov.tr/rss"),
    ("🏛️ Borsa İst.",    "https://www.borsaistanbul.com/tr/rss/haberler"),
    ("🏛️ Resmi Gazete",  "https://www.resmigazete.gov.tr/rss/tum.xml"),
    ("📡 AA Ekonomi",    "https://www.aa.com.tr/tr/rss/default?cat=ekonomi"),
    ("📡 DHA Ekonomi",   "https://www.dha.com.tr/rss/ekonomi.xml"),
    ("💹 Bloomberg HT",  "https://www.bloomberght.com/rss"),
    ("💹 Dünya",         "https://www.dunya.com/rss/rss.xml"),
    ("💹 Ekonomim",      "https://www.ekonomim.com/rss"),
    ("💹 Para Analiz",   "https://www.paraanaliz.com/feed/"),
    ("💹 Anka Haber",    "https://www.ankahaber.net/rss.xml"),
    ("💹 Finans Gündem", "https://www.finansgundem.com/rss"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

def yeni_h(kaynak, baslik, url, tip="rss"):
    return {
        "kaynak": kaynak, "baslik": baslik.strip()[:250],
        "url": url, "zaman": datetime.now().strftime("%H:%M"),
        "skor": 0, "yon": "NÖTR", "semboller": [],
        "ozet": "", "novelty": 1.0, "kaynak_tip": tip
    }

# ─────────────────────────────────────────────
# TOPLAYICILAR
# ─────────────────────────────────────────────
def rss_cek(feeds):
    out = []
    for kaynak, url in feeds:
        try:
            feed = feedparser.parse(url, request_headers=HEADERS)
            for e in feed.entries[:6]:
                b = (e.get("title") or "").strip()
                if b and len(b) > 10:
                    out.append(yeni_h(kaynak, b, e.get("link", "")))
        except Exception as e:
            log.debug(f"RSS hata {kaynak}: {e}")
    return out

def marketaux_cek():
    out = []
    try:
        r = requests.get(
            "https://api.marketaux.com/v1/news/all",
            params={
                "countries": "tr",
                "filter_entities": "true",
                "language": "tr,en",
                "limit": 10,
                "api_token": MARKETAUX_KEY
            }, timeout=12, headers=HEADERS)
        for item in r.json().get("data", []):
            b = item.get("title", "")
            if not b: continue
            syms = [
                e.get("symbol", "") for e in item.get("entities", [])
                if e.get("exchange", "") in ["BIST", "XIST", "IST"] and e.get("symbol")
            ]
            h = yeni_h("📊 Marketaux", b, item.get("url", ""), "api")
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
            params={"category": "general", "token": FINNHUB_KEY},
            timeout=10, headers=HEADERS)
        for item in r.json()[:10]:
            b = item.get("headline", "")
            if b:
                out.append(yeni_h(
                    f"📡 {item.get('source', 'Finnhub')}",
                    b, item.get("url", ""), "api"
                ))
    except Exception as e:
        log.warning(f"Finnhub genel: {e}")
    return out

def finnhub_bist_cek():
    out = []
    dun   = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    bugun = datetime.now().strftime("%Y-%m-%d")
    for sembol in random.sample(BIST30, 6):
        try:
            r = requests.get(
                "https://finnhub.io/api/v1/company-news",
                params={
                    "symbol": f"XIST:{sembol}",
                    "from": dun, "to": bugun,
                    "token": FINNHUB_KEY
                }, timeout=8, headers=HEADERS)
            for item in r.json()[:3]:
                b = item.get("headline", "")
                if b:
                    h = yeni_h(
                        f"📈 Finnhub [{sembol}]",
                        f"[{sembol}] {b}",
                        item.get("url", ""), "sirket"
                    )
                    h["semboller"] = [sembol]
                    out.append(h)
            time.sleep(0.1)
        except Exception as e:
            log.debug(f"Finnhub {sembol}: {e}")
    return out

def sirket_haberleri_cek():
    out = []
    kaynaklar = [
        ("📈 Foreks",      "https://www.foreks.com/haberler/sirket-haberleri/", "www.foreks.com"),
        ("📈 BorsaGündem", "https://www.borsagundem.com.tr/sirket-haberleri",  "www.borsagundem.com.tr"),
        ("📈 Investing TR","https://tr.investing.com/news/company-news",        "tr.investing.com"),
    ]
    for ad, url, domain in kaynaklar:
        try:
            r = requests.get(url, timeout=12, headers=HEADERS)
            soup = BeautifulSoup(r.text, "html.parser")
            selectors = ["h3 a", "h2 a", "h4 a", ".news-title a",
                        "a[href*='/haberler/']", "article a", ".articleItem a"]
            seen = set()
            for sel in selectors:
                for item in soup.select(sel)[:15]:
                    b = item.get_text(strip=True)
                    href = item.get("href", "")
                    if b and len(b) > 20 and b not in seen:
                        seen.add(b)
                        full = href if href.startswith("http") else f"https://{domain}{href}"
                        out.append(yeni_h(ad, b[:250], full, "sirket"))
        except Exception as e:
            log.warning(f"{ad}: {e}")
    return out

# ─────────────────────────────────────────────
# CLAUDE ANALİZ
# ─────────────────────────────────────────────
def claude_skore_et(haberler, mod):
    if not haberler:
        return []
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        liste = "\n".join([f"{i+1}. {h['baslik']}" for i, h in enumerate(haberler)])

        if mod == "global":
            prompt = f"""Sen BIST/VİOP uzmanı bir Türk tradersin. Global haberleri BIST etkisi açısından skorla.

SKORLAMA:
9-10 = Piyasayı ANINDA etkiler: Fed acil toplantı, büyük savaş başlangıcı, Lehman tipi kriz, petrol %10+
7-8  = Önemli: Fed/ECB sürpriz karar, petrol %5+, büyük jeopolitik kriz, küresel resesyon sinyali  
4-6  = Takip et: Normal veri açıklamaları, beklenen kararlar
0-3  = Gürültü: Şirket haberi, Elon tweet, iç siyaset, spor, magazin

SADECE JSON döndür, başka hiçbir şey yazma:
[{{"id":1,"skor":9,"yon":"BEARISH","semboller":["XU030"],"ozet":"Türkçe max 15 kelime"}}]

Haberler:
{liste}"""

        else:
            prompt = f"""Sen BIST/VİOP uzmanı bir Türk tradersin. Türkiye haberlerini BIST etkisi açısından skorla.

BIST30 hisseleri: {', '.join(BIST30)}
[SEMBOL] ile başlayanlar = şirket haberi, bunlara dikkat et!

SKORLAMA:
9-10 = Piyasayı ANINDA etkiler: TCMB faiz sürprizi, TRY krizi (1 günde %5+), yaptırım, büyük deprem, sıkıyönetim
8    = Çok önemli: BIST30 şirket/KAP haberi (büyük sözleşme, M&A, temettü, bilanço), enflasyon/büyüme verisi, TCMB/SPK kararı
7    = Önemli: Orta büyüklük şirket haberi, sektör kararı, önemli regülasyon
5-6  = Takip et: Küçük şirket haberi, genel ekonomi yorumu
0-4  = Gürültü: Siyaset, spor, magazin, belirsiz haber

SADECE JSON döndür, başka hiçbir şey yazma:
[{{"id":1,"skor":8,"yon":"BULLISH","semboller":["THYAO"],"ozet":"Türkçe max 15 kelime"}}]

Haberler:
{liste}"""

        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2500,
            messages=[{"role": "user", "content": prompt}]
        )
        text = msg.content[0].text.strip()

        # JSON temizle
        if "```" in text:
            text = re.search(r'\[.*\]', text, re.DOTALL)
            text = text.group(0) if text else "[]"
        if not text.startswith("["):
            m = re.search(r'\[.*\]', text, re.DOTALL)
            text = m.group(0) if m else "[]"

        for s in json.loads(text):
            idx = s.get("id", 0) - 1
            if 0 <= idx < len(haberler):
                mevcut = haberler[idx].get("semboller", [])
                haberler[idx].update({
                    "skor":      max(0, min(10, int(s.get("skor", 0)))),
                    "yon":       s.get("yon", "NÖTR"),
                    "semboller": mevcut if mevcut else s.get("semboller", []),
                    "ozet":      s.get("ozet", "")
                })
    except Exception as e:
        log.warning(f"Claude ({mod}): {e}")
    return haberler

# ─────────────────────────────────────────────
# MESAJ FORMATI
# ─────────────────────────────────────────────
def hash_h(t):
    return hashlib.md5(t.encode("utf-8", errors="ignore")).hexdigest()[:12]

def mesaj_olustur(h, mod):
    skor = h.get("skor", 0)
    yon  = h.get("yon", "NÖTR")
    syms = h.get("semboller", [])
    ozet = h.get("ozet", "")
    nv   = h.get("novelty", 1.0)
    flag = "🇹🇷" if mod == "turkey" else "🌍"

    if skor >= 9:
        ikon, etiket = "🔴🚨", "ACİL"
    elif skor >= 8:
        ikon, etiket = "🟡⚡", "ÖNEMLİ"
    else:
        ikon, etiket = "🔵", "TAKİP"

    nb  = "🆕" if nv >= 0.9 else ("🔄" if nv >= 0.5 else "♻️")
    yi  = {"BULLISH": "📈", "BEARISH": "📉", "NÖTR": "➡️"}.get(yon, "➡️")
    ss  = "  ".join([f"<b>#{s}</b>" for s in syms[:5]])

    msg  = f"{ikon}{flag}{nb} <b>{etiket}</b> [{skor}/10]\n"
    msg += f"📌 {h['kaynak']}  |  ⏰ {h['zaman']}\n\n"
    msg += f"📰 {h['baslik']}\n"
    if ozet:
        msg += f"\n{yi} <b>{yon}</b>: {ozet}\n"
    if ss:
        msg += f"\n🎯 {ss}\n"
    if h.get("url"):
        msg += f"\n🔗 <a href='{h['url']}'>Habere Git</a>"
    return msg

# ─────────────────────────────────────────────
# DURUM MESAJI (her 6 saatte bir)
# ─────────────────────────────────────────────
async def durum_gonder(bot, telethon_aktif, toplam_gonderilen, baslangic):
    sure = datetime.now() - baslangic
    saat = int(sure.total_seconds() // 3600)
    await bot.send_message(
        chat_id=CHAT_ID,
        text=(
            f"📊 <b>BOT DURUM RAPORU</b>\n\n"
            f"⏱️ Çalışma süresi: {saat} saat\n"
            f"📨 Gönderilen haber: {toplam_gonderilen}\n"
            f"{'✅' if telethon_aktif else '⚪'} Telegram kanalları\n"
            f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        ),
        parse_mode=ParseMode.HTML
    )

# ─────────────────────────────────────────────
# ANA DÖNGÜ
# ─────────────────────────────────────────────
gonderilen      = set()
bekl_global     = []
bekl_turkey     = []
son_ai          = 0.0
dongu_sayac     = 0
telegram_kuyruk = []
toplam_gonderilen = 0
baslangic       = datetime.now()
son_durum       = datetime.now()

async def ana_dongu():
    global bekl_global, bekl_turkey, son_ai, dongu_sayac
    global telegram_kuyruk, toplam_gonderilen, son_durum

    bot = Bot(token=TELEGRAM_TOKEN)
    telethon_aktif = False

    # ── Telethon ──────────────────────────────
    if TELETHON_SESSION and TELEGRAM_KANALLARI:
        try:
            tg = TelegramClient(
                StringSession(TELETHON_SESSION),
                TELEGRAM_API_ID, TELEGRAM_API_HASH
            )
            await tg.start()
            telethon_aktif = True
            log.info(f"✅ Telethon: {len(TELEGRAM_KANALLARI)} kanal")

            @tg.on(events.NewMessage(chats=TELEGRAM_KANALLARI))
            async def tg_handler(event):
                m = event.raw_text
                if m and len(m) > 15:
                    um = re.search(r'https?://\S+', m)
                    b  = m[:250].replace("\n", " ").strip()
                    h  = yeni_h(
                        f"📲 {getattr(event.chat, 'title', 'Telegram')}",
                        b, um.group(0) if um else "", "telegram"
                    )
                    h["semboller"] = nov.entity_bul(b)
                    telegram_kuyruk.append(h)
                    log.info(f"📲 {b[:50]}")
        except Exception as e:
            log.warning(f"Telethon başlatma hatası: {e}")

    # ── Başlangıç mesajı ──────────────────────
    await bot.send_message(
        chat_id=CHAT_ID,
        text=(
            "🤖 <b>BERKAY TERMINATOR — RAILWAY 🚀</b>\n\n"
            f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
            f"{'✅' if telethon_aktif else '⚪'} Telethon ({len(TELEGRAM_KANALLARI)} kanal)\n"
            "✅ Novelty Memory (24 saat)\n"
            "✅ Entity Resolution (BIST30)\n"
            "✅ Foreks + BorsaGündem + Investing TR\n"
            "✅ Finnhub BIST30 şirket haberleri\n"
            "✅ Marketaux TR\n\n"
            f"🇹🇷 {len(RSS_TURKEY)} RSS kaynağı\n"
            f"🌍 {len(RSS_GLOBAL)} RSS kaynağı\n\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "🆕 Yeni  🔄 Benzeri var  ♻️ Tekrar\n"
            "🔴🚨 ACİL  🟡⚡ ÖNEMLİ  🔵 TAKİP\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Hazırım! Haberler geliyor... 📡"
        ),
        parse_mode=ParseMode.HTML
    )
    log.info("✅ BOT BAŞLADI!")

    # ── Ana döngü ─────────────────────────────
    while True:
        try:
            now = time.time()
            dongu_sayac += 1

            # Her döngü: RSS + API
            g_raw = rss_cek(RSS_GLOBAL) + finnhub_genel_cek()
            t_raw = rss_cek(RSS_TURKEY) + marketaux_cek()

            # Her 4 döngüde (~48sn): şirket haberleri
            if dongu_sayac % 4 == 0:
                sirket = sirket_haberleri_cek() + finnhub_bist_cek()
                t_raw += sirket
                if sirket:
                    log.info(f"📈 {len(sirket)} şirket haberi")

            # Telegram kuyruğu
            if telegram_kuyruk:
                t_raw += telegram_kuyruk[:30]
                telegram_kuyruk.clear()

            # Haberleri işle ve kuyruğa ekle
            def isle(haberler, kuyruk):
                for h in haberler:
                    hsh = hash_h(h["baslik"])
                    if hsh not in gonderilen and h["baslik"] and len(h["baslik"]) > 10:
                        gonderilen.add(hsh)
                        if not h["semboller"]:
                            h["semboller"] = nov.entity_bul(h["baslik"])
                        ek = h["semboller"][0] if h["semboller"] else "GENEL"
                        h["novelty"] = nov.skorla(ek, h["baslik"])
                        kuyruk.append(h)

            isle(g_raw, bekl_global)
            isle(t_raw, bekl_turkey)

            toplam = len(bekl_global) + len(bekl_turkey)
            if toplam > 0:
                log.info(f"📥 Kuyruk: 🌍{len(bekl_global)} | 🇹🇷{len(bekl_turkey)}")

            # AI çağrısı
            yeterli = len(bekl_global) >= 5 or len(bekl_turkey) >= 5
            sure_ok = (now - son_ai) >= AI_INTERVAL

            if toplam > 0 and (yeterli or sure_ok):
                son_ai = now
                gonderilenler = []

                if bekl_global:
                    sk = claude_skore_et(bekl_global[:20], "global")
                    for h in sk:
                        h["fs"] = h.get("skor", 0) * h.get("novelty", 1.0)
                    filtre = sorted(
                        [h for h in sk if h.get("skor", 0) >= GLOBAL_THRESH],
                        key=lambda x: x["fs"], reverse=True
                    )[:MAX_GLOBAL]
                    gonderilenler += [(h, "global") for h in filtre]
                    bekl_global = []

                if bekl_turkey:
                    sk = claude_skore_et(bekl_turkey[:25], "turkey")
                    for h in sk:
                        h["fs"] = h.get("skor", 0) * h.get("novelty", 1.0)
                    filtre = sorted(
                        [h for h in sk if h.get("skor", 0) >= TURKEY_THRESH],
                        key=lambda x: x["fs"], reverse=True
                    )[:MAX_TURKEY]
                    gonderilenler += [(h, "turkey") for h in filtre]
                    bekl_turkey = []

                gonderilenler.sort(key=lambda x: x[0].get("fs", 0), reverse=True)

                if gonderilenler:
                    log.info(f"✅ {len(gonderilenler)} haber gönderiliyor")
                    for h, mod in gonderilenler:
                        try:
                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=mesaj_olustur(h, mod),
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True
                            )
                            toplam_gonderilen += 1
                            e2 = "🔴" if h["skor"] >= 9 else "🟡"
                            f2 = "🇹🇷" if mod == "turkey" else "🌍"
                            log.info(f"{e2}{f2}[{h['skor']}] {h['baslik'][:70]}")
                            await asyncio.sleep(1.5)
                        except Exception as ex:
                            log.error(f"Gönderme hatası: {ex}")
                else:
                    log.info("— Eşik yok")

            # Her 6 saatte durum raporu
            if (datetime.now() - son_durum).total_seconds() > 21600:
                await durum_gonder(bot, telethon_aktif, toplam_gonderilen, baslangic)
                son_durum = datetime.now()

            # Hash seti büyürse temizle (bellek)
            if len(gonderilen) > 5000:
                log.info("🧹 Hash seti temizleniyor")
                gonderilen.clear()

        except Exception as e:
            log.error(f"Döngü hatası: {e}")
            await asyncio.sleep(30)

        await asyncio.sleep(POLL_INTERVAL)

asyncio.run(ana_dongu())
