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
# ⚙️  KEY'LER — Railway Variables'dan otomatik gelir
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

TELEGRAM_KANALLARI = [
    # Buraya bulduğun kanal username'lerini yaz
    # Örnek: "paraanaliz"
]

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

class NoveltyMemory:
    def __init__(self):
        self.memory = defaultdict(list)

    def _temizle(self):
        now = datetime.now()
        for entity in list(self.memory.keys()):
            self.memory[entity] = [
                (kws, ts) for kws, ts in self.memory[entity]
                if (now - ts).total_seconds() / 60 < NOVELTY_WINDOW
            ]

    def skorla(self, entity, baslik):
        self._temizle()
        keywords = set(re.findall(r'\w{4,}', baslik.lower()))
        max_overlap = 0.0
        for gecmis_kws, _ in self.memory.get(entity, []):
            if keywords and gecmis_kws:
                overlap = len(keywords & gecmis_kws) / max(len(keywords), len(gecmis_kws))
                max_overlap = max(max_overlap, overlap)
        self.memory[entity].append((keywords, datetime.now()))
        if max_overlap > 0.6: return 0.2
        elif max_overlap > 0.3: return 0.6
        return 1.0

    def entity_bul(self, baslik):
        b = baslik.lower()
        out = []
        for t in BIST30:
            if t.lower() in b: out.append(t)
        for t, vs in ENTITY_DICT.items():
            for v in vs:
                if v in b and t not in out: out.append(t)
        return out[:4]

novelty = NoveltyMemory()

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

def yeni_h(kaynak, baslik, url, tip="rss"):
    return {
        "kaynak": kaynak, "baslik": baslik, "url": url,
        "zaman": datetime.now().strftime("%H:%M"),
        "skor": 0, "yon": "NÖTR", "semboller": [],
        "ozet": "", "novelty": 1.0, "kaynak_tip": tip
    }

def rss_cek(feeds):
    out = []
    for kaynak, url in feeds:
        try:
            for e in feedparser.parse(url).entries[:6]:
                b = e.get("title","").strip()
                if b: out.append(yeni_h(kaynak, b, e.get("link","")))
        except: pass
    return out

def sirket_haberleri_cek():
    out = []
    for ad, url, domain in [
        ("📈 Foreks", "https://www.foreks.com/haberler/sirket-haberleri/", "www.foreks.com"),
        ("📈 BorsaGündem", "https://www.borsagundem.com.tr/sirket-haberleri", "www.borsagundem.com.tr"),
    ]:
        try:
            r = requests.get(url, timeout=12,
                headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
            soup = BeautifulSoup(r.text, "html.parser")
            for item in soup.select("h3 a, h2 a, .news-title a, a[href*='/haberler/']")[:12]:
                b = item.get_text(strip=True)
                href = item.get("href","")
                if b and len(b) > 20:
                    full = href if href.startswith("http") else f"https://{domain}{href}"
                    out.append(yeni_h(ad, b[:200], full, "sirket"))
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
            if not b: continue
            syms = [e.get("symbol","") for e in item.get("entities",[])
                   if e.get("exchange","") in ["BIST","XIST","IST"] and e.get("symbol")]
            h = yeni_h("📊 Marketaux", b, item.get("url",""), "api")
            h["semboller"] = syms[:3]
            out.append(h)
    except Exception as e:
        print(f"⚠️ Marketaux: {e}")
    return out

def finnhub_cek():
    out = []
    try:
        for item in requests.get(
            f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_KEY}",
            timeout=10).json()[:8]:
            b = item.get("headline","")
            if b: out.append(yeni_h(f"📡 {item.get('source','Finnhub')}", b,
                                    item.get("url",""), "api"))
    except: pass
    return out

def finnhub_bist_cek():
    import random
    out = []
    dun = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    bugun = datetime.now().strftime("%Y-%m-%d")
    for s in random.sample(BIST30, 5):
        try:
            r = requests.get(
                f"https://finnhub.io/api/v1/company-news"
                f"?symbol=XIST:{s}&from={dun}&to={bugun}&token={FINNHUB_KEY}",
                timeout=8)
            for item in r.json()[:2]:
                b = item.get("headline","")
                if b:
                    h = yeni_h(f"📈 [{s}]", f"[{s}] {b}", item.get("url",""), "sirket")
                    h["semboller"] = [s]
                    out.append(h)
            time.sleep(0.15)
        except: pass
    return out

def claude_skore_et(haberler, mod):
    if not haberler: return []
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        liste = "\n".join([f"{i+1}. {h['baslik']}" for i,h in enumerate(haberler)])

        if mod == "global":
            prompt = f"""BIST/VİOP uzmanı Türk tradersin. GLOBAL haberleri skorla.
9-10 = Fed acil, büyük savaş, petrol %10+, küresel kriz
7-8  = Fed/ECB faiz sürprizi, büyük jeopolitik
0-6  = Diğer. Elon/sosyal medya = 0-2.
SADECE JSON: [{{"id":1,"skor":9,"yon":"BEARISH","semboller":["XU030"],"ozet":"Türkçe 1 cümle"}}]
Haberler:\n{liste}"""
        else:
            prompt = f"""BIST/VİOP uzmanı Türk tradersin. TÜRKİYE haberlerini skorla.
BIST30: {', '.join(BIST30)}
[SEMBOL] ile başlayanlar = şirket haberi!

9-10 = TCMB faiz sürprizi, TRY krizi, yaptırım, deprem
8    = BIST30 şirket/KAP haberi, enflasyon verisi, TCMB açıklama
6-7  = Sektör haberi, küçük şirket
0-5  = Siyaset, spor, magazin, gürültü

SADECE JSON: [{{"id":1,"skor":8,"yon":"BULLISH","semboller":["THYAO"],"ozet":"Türkçe 1 cümle"}}]
Haberler:\n{liste}"""

        msg = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=2000,
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
                    "skor": s.get("skor",0),
                    "yon": s.get("yon","NÖTR"),
                    "semboller": mevcut if mevcut else s.get("semboller",[]),
                    "ozet": s.get("ozet","")
                })
    except Exception as e:
        print(f"⚠️ Claude ({mod}): {e}")
    return haberler

def hash_h(t): return hashlib.md5(t.encode()).hexdigest()[:12]

def mesaj_olustur(h, mod):
    skor = h.get("skor",0)
    yon  = h.get("yon","NÖTR")
    syms = h.get("semboller",[])
    ozet = h.get("ozet","")
    nov  = h.get("novelty",1.0)
    flag = "🇹🇷" if mod=="turkey" else "🌍"

    if skor >= 9:   ikon, etiket = "🔴🚨", "ACİL"
    elif skor >= 8: ikon, etiket = "🟡", "ÖNEMLİ"
    else:           ikon, etiket = "🔵", "TAKİP"

    nb   = "🆕" if nov>=0.9 else ("🔄" if nov>=0.5 else "♻️")
    yi   = {"BULLISH":"📈","BEARISH":"📉","NÖTR":"➡️"}.get(yon,"➡️")
    ss   = "  ".join([f"<b>#{s}</b>" for s in syms[:5]])

    msg  = f"{ikon}{flag}{nb} <b>{etiket}</b> [{skor}/10]  |  {h['kaynak']}  |  ⏰ {h['zaman']}\n\n"
    msg += f"📰 {h['baslik']}\n"
    if ozet: msg += f"\n{yi} <b>{yon}</b>: {ozet}\n"
    if ss:   msg += f"🎯 {ss}\n"
    if h.get("url"): msg += f"\n🔗 <a href='{h['url']}'>Habere Git</a>"
    return msg

gonderilen      = set()
bekl_global     = []
bekl_turkey     = []
son_ai          = 0
dongu_sayac     = 0
telegram_kuyruk = []

async def ana_dongu():
    global bekl_global, bekl_turkey, son_ai, dongu_sayac, telegram_kuyruk

    bot = Bot(token=TELEGRAM_TOKEN)
    telethon_aktif = False

    if TELETHON_SESSION and TELEGRAM_KANALLARI:
        try:
            tg = TelegramClient(StringSession(TELETHON_SESSION),
                               TELEGRAM_API_ID, TELEGRAM_API_HASH)
            await tg.start()
            telethon_aktif = True
            print(f"✅ Telethon: {len(TELEGRAM_KANALLARI)} kanal")

            @tg.on(events.NewMessage(chats=TELEGRAM_KANALLARI))
            async def tg_handler(event):
                m = event.raw_text
                if m and len(m) > 10:
                    um = re.search(r'https?://\S+', m)
                    b  = m[:200].replace("\n"," ").strip()
                    h  = yeni_h(f"📲 {event.chat.title if event.chat else 'TG'}",
                               b, um.group(0) if um else "", "telegram")
                    h["semboller"] = novelty.entity_bul(b)
                    telegram_kuyruk.append(h)
        except Exception as e:
            print(f"⚠️ Telethon: {e}")

    await bot.send_message(
        chat_id=CHAT_ID,
        text=(
            "🤖 <b>BERKAY TERMINATOR — Railway 🚀</b>\n\n"
            f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
            f"{'✅' if telethon_aktif else '⚪'} Telethon\n"
            "✅ Novelty Memory\n✅ Entity Resolution\n"
            "✅ Foreks + BorsaGündem\n"
            "✅ Finnhub BIST30\n✅ Marketaux TR\n\n"
            f"🇹🇷 {len(RSS_TURKEY)} RSS  |  🌍 {len(RSS_GLOBAL)} RSS\n\n"
            "🆕=Yeni  🔄=Benzeri  ♻️=Tekrar\n"
            "🔴🚨ACİL | 🟡ÖNEMLİ | 🔵TAKİP"
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
                s = sirket_haberleri_cek() + finnhub_bist_cek()
                t_raw += s
                if s: print(f"📈 {len(s)} şirket haberi")

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

            if toplam > 0 and ((len(bekl_global)>=5 or len(bekl_turkey)>=5) or
                               (now-son_ai)>=AI_INTERVAL):
                son_ai = now
                gonderilenler = []

                if bekl_global:
                    sk = claude_skore_et(bekl_global[:25], "global")
                    for h in sk: h["fs"] = h.get("skor",0)*h.get("novelty",1.0)
                    for h in sorted([h for h in sk if h.get("skor",0)>=GLOBAL_THRESH],
                                    key=lambda x:x["fs"],reverse=True)[:MAX_GLOBAL]:
                        gonderilenler.append((h,"global"))
                    bekl_global = []

                if bekl_turkey:
                    sk = claude_skore_et(bekl_turkey[:30], "turkey")
                    for h in sk: h["fs"] = h.get("skor",0)*h.get("novelty",1.0)
                    for h in sorted([h for h in sk if h.get("skor",0)>=TURKEY_THRESH],
                                    key=lambda x:x["fs"],reverse=True)[:MAX_TURKEY]:
                        gonderilenler.append((h,"turkey"))
                    bekl_turkey = []

                gonderilenler.sort(key=lambda x:x[0].get("fs",0),reverse=True)

                if gonderilenler:
                    print(f"✅ {len(gonderilenler)} haber gönderiliyor")
                    for h, mod in gonderilenler:
                        try:
                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=mesaj_olustur(h, mod),
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True
                            )
                            e = "🔴" if h["skor"]>=9 else "🟡"
                            f = "🇹🇷" if mod=="turkey" else "🌍"
                            print(f"{e}{f}[{h['skor']}] {h['baslik'][:60]}")
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
