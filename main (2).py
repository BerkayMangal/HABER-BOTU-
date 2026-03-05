import asyncio
import feedparser
import requests
import hashlib
import json
import logging
import time
import re
import os
from datetime import datetime, timedelta
from collections import defaultdict
from bs4 import BeautifulSoup
import anthropic
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telegram import Bot
from telegram.constants import ParseMode

# ============================================================
# ⚙️  AYARLAR — Railway'de Environment Variables'dan gelir
# ============================================================
TELEGRAM_TOKEN    = os.environ["TELEGRAM_TOKEN"]
CHAT_ID           = os.environ["CHAT_ID"]
ANTHROPIC_KEY     = os.environ["ANTHROPIC_KEY"]
FINNHUB_KEY       = os.environ["FINNHUB_KEY"]
MARKETAUX_KEY     = os.environ["MARKETAUX_KEY"]
TELEGRAM_API_ID   = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELETHON_SESSION  = os.environ.get("TELETHON_SESSION", "")

POLL_INTERVAL  = 15
AI_INTERVAL    = 45
MAX_GLOBAL     = 2
MAX_TURKEY     = 8
TURKEY_THRESH  = 8
GLOBAL_THRESH  = 9
NOVELTY_WINDOW = 1440

# ============================================================
# 📡 TELEGRAM KANALLARI — Kendin doldur
# ============================================================
TELEGRAM_KANALLARI = [
    # Buraya Telegram'da bulduğun gerçek kanal username'lerini yaz
    # Örnek: "paraanaliz", "bloomberght" vb.
]

# ============================================================
# 🇹🇷 BIST30 + ENTITY DICT
# ============================================================
BIST30 = [
    "THYAO","GARAN","AKBNK","ISCTR","YKBNK","TUPRS","EREGL","ASELS",
    "KCHOL","SAHOL","SISE","TOASO","FROTO","PGSUS","KOZAL","EKGYO",
    "BIMAS","MGROS","ULKER","TCELL","TTKOM","ARCLK","VESTL","DOHOL",
    "PETKM","AYGAZ","ENKAI","TAVHL","LOGO","SOKM"
]

ENTITY_DICT = {
    "THYAO": ["türk hava yolları","thy","türk havayolları","turkish airlines"],
    "GARAN": ["garanti","garanti bbva","garanti bankası"],
    "AKBNK": ["akbank"],
    "ISCTR": ["iş bankası","işbank","isbank"],
    "YKBNK": ["yapı kredi","yapı ve kredi"],
    "TUPRS": ["tüpraş","tupras"],
    "EREGL": ["ereğli","erdemir","eregli"],
    "ASELS": ["aselsan"],
    "KCHOL": ["koç holding","koç"],
    "SAHOL": ["sabancı holding","sabancı"],
    "SISE":  ["şişecam","sisecam"],
    "TOASO": ["tofaş","tofas"],
    "FROTO": ["ford otosan"],
    "PGSUS": ["pegasus"],
    "TCELL": ["turkcell"],
    "TTKOM": ["türk telekom","turk telekom"],
    "BIMAS": ["bim","bim mağazaları"],
    "MGROS": ["migros"],
    "ARCLK": ["arçelik","arcelik"],
    "PETKM": ["petkim"],
    "ENKAI": ["enka"],
    "TAVHL": ["tav havalimanları","tav havalimanlari"],
}

# ============================================================
# 🧠 NOVELTY MEMORY
# ============================================================
class NoveltyMemory:
    def __init__(self, window_minutes=NOVELTY_WINDOW):
        self.memory = defaultdict(list)
        self.window = window_minutes

    def _temizle(self):
        now = datetime.now()
        for entity in list(self.memory.keys()):
            self.memory[entity] = [
                (kws, ts) for kws, ts in self.memory[entity]
                if (now - ts).total_seconds() / 60 < self.window
            ]

    def skorla(self, entity, baslik):
        self._temizle()
        keywords = set(re.findall(r'\w{4,}', baslik.lower()))
        gecmis = self.memory.get(entity, [])
        max_overlap = 0.0
        for gecmis_kws, _ in gecmis:
            if keywords and gecmis_kws:
                overlap = len(keywords & gecmis_kws) / max(len(keywords), len(gecmis_kws))
                max_overlap = max(max_overlap, overlap)
        self.memory[entity].append((keywords, datetime.now()))
        if max_overlap > 0.6: return 0.2
        elif max_overlap > 0.3: return 0.6
        return 1.0

    def entity_bul(self, baslik):
        baslik_lower = baslik.lower()
        bulunanlar = []
        for ticker in BIST30:
            if ticker.lower() in baslik_lower:
                bulunanlar.append(ticker)
        for ticker, varyasyonlar in ENTITY_DICT.items():
            for v in varyasyonlar:
                if v in baslik_lower and ticker not in bulunanlar:
                    bulunanlar.append(ticker)
        return bulunanlar[:4]

novelty = NoveltyMemory()

# ============================================================
# 📡 RSS KAYNAKLARI
# ============================================================
RSS_GLOBAL = [
    ("🔴 Reuters",      "https://feeds.reuters.com/reuters/topNews"),
    ("🔴 Reuters Biz",  "https://feeds.reuters.com/reuters/businessNews"),
    ("🔴 Reuters EM",   "https://feeds.reuters.com/reuters/emergingMarketsNews"),
    ("⚡ CNBC",         "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("🐻 ZeroHedge",    "https://feeds.feedburner.com/zerohedge/feed"),
    ("🏦 FED",          "https://www.federalreserve.gov/feeds/press_all.xml"),
    ("🏦 ECB",          "https://www.ecb.europa.eu/rss/press.html"),
]

RSS_TURKEY = [
    ("🏛️ TCMB",         "https://www.tcmb.gov.tr/wps/wcm/connect/TR/rss/"),
    ("🏛️ SPK",          "https://www.spk.gov.tr/rss/HaberDetay"),
    ("🏛️ BDDK",         "https://www.bddk.org.tr/Rss/RssDuyuru"),
    ("🏛️ Hazine",       "https://www.hmb.gov.tr/rss"),
    ("🏛️ Borsa İst.",   "https://www.borsaistanbul.com/tr/rss/haberler"),
    ("🏛️ Resmi Gazete", "https://www.resmigazete.gov.tr/rss/tum.xml"),
    ("📡 AA Ekonomi",   "https://www.aa.com.tr/tr/rss/default?cat=ekonomi"),
    ("📡 DHA Ekonomi",  "https://www.dha.com.tr/rss/ekonomi.xml"),
    ("💹 Bloomberg HT", "https://www.bloomberght.com/rss"),
    ("💹 Dünya",        "https://www.dunya.com/rss/rss.xml"),
    ("💹 Ekonomim",     "https://www.ekonomim.com/rss"),
    ("💹 Para Analiz",  "https://www.paraanaliz.com/feed/"),
]

# ============================================================
# 📡 TOPLAYICILAR
# ============================================================
def yeni_haber(kaynak, baslik, url, tip="rss"):
    return {
        "kaynak": kaynak, "baslik": baslik, "url": url,
        "zaman": datetime.now().strftime("%H:%M"),
        "skor": 0, "yon": "NÖTR", "semboller": [], "ozet": "",
        "novelty": 1.0, "kaynak_tip": tip
    }

def rss_cek(feeds):
    out = []
    for kaynak, url in feeds:
        try:
            for e in feedparser.parse(url).entries[:6]:
                b = e.get("title","").strip()
                if b: out.append(yeni_haber(kaynak, b, e.get("link","")))
        except: pass
    return out

def sirket_haberleri_cek():
    out = []
    kaynaklar = [
        ("📈 Foreks", "https://www.foreks.com/haberler/sirket-haberleri/", "foreks.com"),
        ("📈 BorsaGündem", "https://www.borsagundem.com.tr/sirket-haberleri", "borsagundem.com.tr"),
    ]
    for ad, url, domain in kaynaklar:
        try:
            r = requests.get(url, timeout=12,
                           headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
            soup = BeautifulSoup(r.text, "html.parser")
            for item in soup.select("h3 a, h2 a, .news-title a, a[href*='/haberler/']")[:12]:
                b = item.get_text(strip=True)
                href = item.get("href","")
                if b and len(b) > 20:
                    full_url = href if href.startswith("http") else f"https://{domain}{href}"
                    out.append(yeni_haber(ad, b[:200], full_url, "sirket"))
        except Exception as e:
            print(f"⚠️ {ad}: {e}")
    return out

def marketaux_cek():
    out = []
    try:
        r = requests.get(
            f"https://api.marketaux.com/v1/news/all"
            f"?countries=tr&filter_entities=true&language=tr,en"
            f"&api_token={MARKETAUX_KEY}", timeout=10)
        for item in r.json().get("data",[])[:10]:
            b = item.get("title","")
            entities = item.get("entities",[])
            syms = [e.get("symbol","") for e in entities
                   if e.get("exchange","") in ["BIST","XIST","IST"] and e.get("symbol")]
            if b:
                h = yeni_haber("📊 Marketaux", b, item.get("url",""), "api")
                h["semboller"] = syms[:3]
                out.append(h)
    except Exception as e:
        print(f"⚠️ Marketaux: {e}")
    return out

def finnhub_cek():
    out = []
    try:
        r = requests.get(
            f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_KEY}",
            timeout=10)
        for item in r.json()[:8]:
            b = item.get("headline","")
            if b:
                out.append(yeni_haber(f"📡 {item.get('source','Finnhub')}", b,
                                     item.get("url",""), "api"))
    except: pass
    return out

def finnhub_bist30_cek():
    """Her döngüde 5 rastgele BIST30 hissesi için haber çek"""
    out = []
    import random
    bugun = datetime.now().strftime("%Y-%m-%d")
    dun   = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    for sembol in random.sample(BIST30, 5):
        try:
            r = requests.get(
                f"https://finnhub.io/api/v1/company-news"
                f"?symbol=XIST:{sembol}&from={dun}&to={bugun}&token={FINNHUB_KEY}",
                timeout=8)
            for item in r.json()[:2]:
                b = item.get("headline","")
                if b:
                    h = yeni_haber(f"📈 [{sembol}]", f"[{sembol}] {b}",
                                  item.get("url",""), "sirket")
                    h["semboller"] = [sembol]
                    out.append(h)
            time.sleep(0.15)
        except: pass
    return out

# ============================================================
# 🤖 CLAUDE ANALİZ
# ============================================================
def claude_skore_et(haberler, mod):
    if not haberler: return []
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        liste = "\n".join([f"{i+1}. {h['baslik']}" for i,h in enumerate(haberler)])

        if mod == "global":
            prompt = f"""BIST/VİOP uzmanı Türk tradersin. GLOBAL haberleri skorla.

9-10 = Fed acil, büyük savaş, petrol %10+, küresel kriz
7-8  = Fed/ECB faiz sürprizi, petrol/altın %5+, büyük jeopolitik
0-6  = Diğer her şey. Elon/sosyal medya/ABD iç siyaset = 0-2.

SADECE JSON: [{{"id":1,"skor":9,"yon":"BEARISH","semboller":["XU030"],"ozet":"Türkçe 1 cümle"}}]
Haberler:\n{liste}"""
        else:
            prompt = f"""BIST/VİOP uzmanı Türk tradersin. TÜRKİYE haberlerini skorla.
BIST30: {', '.join(BIST30)}
[SEMBOL] ile başlayanlar = şirket haberi, dikkat et!

9-10 = TCMB faiz sürprizi, SPK/BDDK acil, TRY krizi, yaptırım, deprem
8    = BIST30 şirket haberi, KAP bildirimi, enflasyon/büyüme verisi, TCMB açıklama
6-7  = Sektör haberi, küçük şirket, ekonomi yorumu
0-5  = Siyaset, spor, magazin, gürültü

SADECE JSON: [{{"id":1,"skor":8,"yon":"BULLISH","semboller":["THYAO"],"ozet":"Türkçe 1 cümle"}}]
Haberler:\n{liste}"""

        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[{"role":"user","content":prompt}]
        )
        text = msg.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1].replace("json","").strip()
        if not text.endswith("]"):
            last = text.rfind("},")
            if last > 0: text = text[:last+1] + "]"

        for s in json.loads(text):
            idx = s["id"]-1
            if 0 <= idx < len(haberler):
                mevcut = haberler[idx].get("semboller",[])
                haberler[idx].update({
                    "skor":      s.get("skor",0),
                    "yon":       s.get("yon","NÖTR"),
                    "semboller": mevcut if mevcut else s.get("semboller",[]),
                    "ozet":      s.get("ozet","")
                })
    except Exception as e:
        print(f"⚠️ Claude ({mod}): {e}")
    return haberler

# ============================================================
# 🎯 MESAJ
# ============================================================
def hash_h(t): return hashlib.md5(t.encode()).hexdigest()[:12]

def mesaj_olustur(h, mod):
    skor    = h.get("skor",0)
    yon     = h.get("yon","NÖTR")
    syms    = h.get("semboller",[])
    ozet    = h.get("ozet","")
    nov     = h.get("novelty",1.0)
    flag    = "🇹🇷" if mod=="turkey" else "🌍"
    kaynak  = h["kaynak"]

    if skor >= 9:   ikon, etiket = "🔴🚨", "ACİL"
    elif skor >= 8: ikon, etiket = "🟡", "ÖNEMLİ"
    else:           ikon, etiket = "🔵", "TAKİP"

    nbadge = "🆕" if nov >= 0.9 else ("🔄" if nov >= 0.5 else "♻️")
    yikon  = {"BULLISH":"📈","BEARISH":"📉","NÖTR":"➡️"}.get(yon,"➡️")
    sym_s  = "  ".join([f"<b>#{s}</b>" for s in syms[:5]])

    msg  = f"{ikon}{flag}{nbadge} <b>{etiket}</b> [{skor}/10]  |  {kaynak}  |  ⏰ {h['zaman']}\n\n"
    msg += f"📰 {h['baslik']}\n"
    if ozet: msg += f"\n{yikon} <b>{yon}</b>: {ozet}\n"
    if sym_s: msg += f"🎯 {sym_s}\n"
    if h.get("url"): msg += f"\n🔗 <a href='{h['url']}'>Habere Git</a>"
    return msg

# ============================================================
# 🚀 ANA BOT
# ============================================================
gonderilen      = set()
bekl_global     = []
bekl_turkey     = []
son_ai          = 0
dongu_sayac     = 0
telegram_kuyruk = []

async def ana_dongu():
    global bekl_global, bekl_turkey, son_ai, dongu_sayac, telegram_kuyruk

    bot = Bot(token=TELEGRAM_TOKEN)

    # Telethon
    telethon_aktif = False
    if TELETHON_SESSION and TELEGRAM_KANALLARI:
        try:
            tg_client = TelegramClient(
                StringSession(TELETHON_SESSION),
                TELEGRAM_API_ID, TELEGRAM_API_HASH
            )
            await tg_client.start()
            print(f"✅ Telethon: {len(TELEGRAM_KANALLARI)} kanal")
            telethon_aktif = True

            @tg_client.on(events.NewMessage(chats=TELEGRAM_KANALLARI))
            async def tg_handler(event):
                mesaj = event.raw_text
                if mesaj and len(mesaj) > 10:
                    url_m = re.search(r'https?://\S+', mesaj)
                    baslik = mesaj[:200].replace("\n"," ").strip()
                    h = yeni_haber(
                        f"📲 {event.chat.title if event.chat else 'Telegram'}",
                        baslik, url_m.group(0) if url_m else "", "telegram"
                    )
                    h["semboller"] = novelty.entity_bul(baslik)
                    telegram_kuyruk.append(h)
                    print(f"📲 {baslik[:50]}")
        except Exception as e:
            print(f"⚠️ Telethon: {e}")

    await bot.send_message(
        chat_id=CHAT_ID,
        text=(
            "🤖 <b>BERKAY TERMINATOR — Railway 🚀</b>\n\n"
            f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
            f"{'✅' if telethon_aktif else '⚪'} Telethon Telegram kanalları\n"
            "✅ Novelty Memory (24 saat)\n"
            "✅ Entity Resolution (BIST30)\n"
            "✅ Foreks + BorsaGündem scraper\n"
            "✅ Finnhub BIST30 şirket haberleri\n"
            "✅ Marketaux TR\n\n"
            f"🇹🇷 {len(RSS_TURKEY)} RSS kaynağı\n"
            f"🌍 {len(RSS_GLOBAL)} RSS kaynağı\n\n"
            "🆕=Yeni  🔄=Benzeri var  ♻️=Tekrar\n"
            "🔴🚨 ACİL | 🟡 ÖNEMLİ | 🔵 TAKİP"
        ),
        parse_mode=ParseMode.HTML
    )
    print("✅ BOT BAŞLADI!")

    while True:
        try:
            now = time.time()
            dongu_sayac += 1

            g_raw = rss_cek(RSS_GLOBAL) + finnhub_cek()
            t_raw = rss_cek(RSS_TURKEY) + marketaux_cek()

            if dongu_sayac % 4 == 0:
                sirket = sirket_haberleri_cek() + finnhub_bist30_cek()
                t_raw += sirket
                if sirket: print(f"📈 {len(sirket)} şirket haberi")

            if telegram_kuyruk:
                t_raw += telegram_kuyruk[:20]
                telegram_kuyruk.clear()

            def isle(haberler, kuyruk):
                for h in haberler:
                    hsh = hash_h(h["baslik"])
                    if hsh not in gonderilen and h["baslik"]:
                        gonderilen.add(hsh)
                        if not h["semboller"]:
                            h["semboller"] = novelty.entity_bul(h["baslik"])
                        ek = h["semboller"][0] if h["semboller"] else "GENEL"
                        h["novelty"] = novelty.skorla(ek, h["baslik"])
                        kuyruk.append(h)

            isle(g_raw, bekl_global)
            isle(t_raw, bekl_turkey)

            toplam = len(bekl_global) + len(bekl_turkey)
            if toplam > 0:
                print(f"📥 🌍{len(bekl_global)} | 🇹🇷{len(bekl_turkey)}")

            yeterli = len(bekl_global) >= 5 or len(bekl_turkey) >= 5
            sure_ok  = (now - son_ai) >= AI_INTERVAL

            if toplam > 0 and (yeterli or sure_ok):
                son_ai = now
                gonderilenler = []

                if bekl_global:
                    skorlu = claude_skore_et(bekl_global[:25], "global")
                    for h in skorlu:
                        h["fs"] = h.get("skor",0) * h.get("novelty",1.0)
                    for h in sorted([h for h in skorlu if h.get("skor",0)>=GLOBAL_THRESH],
                                    key=lambda x:x["fs"], reverse=True)[:MAX_GLOBAL]:
                        gonderilenler.append((h,"global"))
                    bekl_global = []

                if bekl_turkey:
                    skorlu = claude_skore_et(bekl_turkey[:30], "turkey")
                    for h in skorlu:
                        h["fs"] = h.get("skor",0) * h.get("novelty",1.0)
                    for h in sorted([h for h in skorlu if h.get("skor",0)>=TURKEY_THRESH],
                                    key=lambda x:x["fs"], reverse=True)[:MAX_TURKEY]:
                        gonderilenler.append((h,"turkey"))
                    bekl_turkey = []

                gonderilenler.sort(key=lambda x:x[0].get("fs",0), reverse=True)

                if gonderilenler:
                    print(f"✅ {len(gonderilenler)} haber")
                    for h, mod in gonderilenler:
                        try:
                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=mesaj_olustur(h, mod),
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True
                            )
                            flag = "🇹🇷" if mod=="turkey" else "🌍"
                            e = "🔴" if h["skor"]>=9 else "🟡"
                            print(f"{e}{flag}[{h['skor']}] {h['baslik'][:60]}")
                            await asyncio.sleep(1)
                        except Exception as e:
                            print(f"Gönderme hatası: {e}")
                else:
                    print("— Eşik yok")

        except Exception as e:
            print(f"Döngü hatası: {e}")

        await asyncio.sleep(POLL_INTERVAL)

logging.basicConfig(level=logging.WARNING)
asyncio.run(ana_dongu())
