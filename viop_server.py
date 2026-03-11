"""
VİOP v26 — Standalone Telegram Signal Server
Railway'de çalışır, TradingView'dan webhook alır,
v26 modeli çalıştırır, Telegram'a sinyal atar.

ENV variables (Railway):
  TELEGRAM_TOKEN   = bot token
  CHAT_ID          = hedef chat id
  PORT             = 8080 (Railway otomatik set eder)
"""

import os, json, logging, pickle, asyncio
from datetime import datetime
from aiohttp import web
import pandas as pd
import numpy as np

# ─── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("viop")

# ─── Config ────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("CHAT_ID", "")
MODEL_PATH     = os.environ.get("MODEL_PATH", "viop_v26_model.pkl")

# v26 parametreleri (backtest ile birebir)
PROB_THR       = 0.65
KANGAL_BL      = [4, 5, 11]      # blacklist
BLOCKED_HOURS  = [9, 10, 15]     # kapalı saatler
TP_RATIO       = 1.8
ATR_MULT_SL    = 1.5

# ─── Model yükle ───────────────────────────────────────────
MODEL = None
def model_yukle():
    global MODEL
    try:
        with open(MODEL_PATH, "rb") as f:
            MODEL = pickle.load(f)
        log.info(f"✅ Model yüklendi: {MODEL_PATH}")
    except Exception as e:
        log.warning(f"⚠️  Model yüklenemedi: {e} — prob hesaplanmaz, sinyal kalite filtresi devreye girer")

# Feature sütunları (v26 ile birebir)
FCOLS = [
    'htf_rsi','htf_rsi_slope_2','htf_rsi_slope_5','htf_rsi_slope_3','htf_st_bull',
    'rsi14','macd_hist','macd_line','rvol','flow20','adx14','dip14','dim14',
    'kangal','atr14','cvd','bo1'
]

# ─── Duplicate cache ───────────────────────────────────────
son_sinyal_zaman = {}   # {sembol: son_zaman_str}

# ─── Telegram gönder ───────────────────────────────────────
async def telegram_gonder(msg: str):
    """Telegram'a HTML mesaj gönder"""
    import aiohttp
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    log.info("✅ Telegram gönderildi")
                else:
                    body = await r.text()
                    log.error(f"Telegram hata {r.status}: {body[:200]}")
    except Exception as e:
        log.error(f"telegram_gonder: {e}")

# ─── Prob hesapla ──────────────────────────────────────────
def prob_hesapla(s: dict) -> float:
    """Webhook payload'undan prob hesapla (model varsa)"""
    if MODEL is None:
        # Model yoksa payload'daki prob'u kullan (Pine Script hesapladıysa)
        return float(s.get("prob", 0.0))
    try:
        row = {col: float(s.get(col, 0)) for col in FCOLS}
        X = pd.DataFrame([row])[FCOLS]
        prob = MODEL.predict_proba(X)[0][1]
        return float(prob)
    except Exception as e:
        log.warning(f"prob_hesapla: {e}")
        return float(s.get("prob", 0.0))

# ─── Filtre kontrol ────────────────────────────────────────
def filtre_gec(s: dict, prob: float) -> tuple[bool, str]:
    """
    v26 filtrelerini uygula.
    Returns: (gecer_mi, red_nedeni)
    """
    saat   = int(s.get("saat", 0))
    kangal = int(s.get("kangal", 0))

    if saat in BLOCKED_HOURS:
        return False, f"saat {saat} kapalı"
    if kangal in KANGAL_BL:
        return False, f"kangal {kangal} blacklist"
    if prob < PROB_THR:
        return False, f"prob {prob:.2f} < {PROB_THR}"

    return True, ""

# ─── Sinyal gücü ───────────────────────────────────────────
def guc_hesapla(prob: float) -> str:
    if prob >= 0.80:
        return "EFSANE"
    elif prob >= 0.75:
        return "GUCLU"
    elif prob >= 0.70:
        return "ORTA"
    else:
        return "NORM"

# ─── Mesaj oluştur ─────────────────────────────────────────
def mesaj_olustur(s: dict, prob: float) -> str:
    guc   = guc_hesapla(prob)
    entry = float(s.get("entry", 0))
    sl    = float(s.get("sl", 0))
    tp    = float(s.get("tp", 0))
    atr   = float(s.get("atr", 1))
    zaman = s.get("zaman", "")

    rr = round((tp - entry) / (entry - sl), 2) if (entry - sl) > 0 else 0
    risk_atr = round((entry - sl) / atr, 1) if atr > 0 else 0

    if guc == "EFSANE":
        header = "🎯💎 <b>VİOP EFSANE SİNYAL</b>"
        ikon   = "🚨🚨🚨🚨"
    elif guc == "GUCLU":
        header = "🎯💰 <b>VİOP GÜÇLÜ SİNYAL</b>"
        ikon   = "🚨🚨🚨"
    elif guc == "ORTA":
        header = "🔔 <b>VİOP SİNYAL</b>"
        ikon   = "🔔🔔"
    else:
        header = "🔵 <b>VİOP BİLDİRİM</b>"
        ikon   = "🔵"

    htf_rsi  = float(s.get("htf_rsi", 0))
    htf_rs3  = float(s.get("htf_rsi_slope_3", 0))
    htf_st   = int(s.get("htf_st_bull", 0))
    rvol     = float(s.get("rvol", 0))
    flow     = float(s.get("flow20", 0))
    kangal   = int(s.get("kangal", 0))
    rsi14    = float(s.get("rsi14", 0))
    adx14    = float(s.get("adx14", 0))

    htf_st_str = "BULL 🟢" if htf_st == 1 else "BEAR 🔴"

    msg = (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{ikon} {header}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 <b>XU030 VİOP</b>  ·  <code>{zaman}</code>\n\n"
        f"<b>GİRİŞ :</b>  <code>{entry:,.0f}</code>\n"
        f"<b>STOP  :</b>  <code>{sl:,.0f}</code>  ({risk_atr}×ATR)\n"
        f"<b>HEDEF :</b>  <code>{tp:,.0f}</code>  (1:{rr} R/R)\n\n"
        f"<b>SİNYAL GÜCÜ:</b>  <b>{guc}</b>  ({prob*100:.0f}%)\n\n"
        f"<code>HTF RSI  : {htf_rsi:.1f}   RS3: {htf_rs3:+.1f}</code>\n"
        f"<code>HTF ST   : {htf_st_str}</code>\n"
        f"<code>RSI14   : {rsi14:.1f}   ADX: {adx14:.1f}</code>\n"
        f"<code>RVOL    : {rvol:.1f}x   FLOW: {flow*100:.0f}%</code>\n"
        f"<code>KANGAL  : {kangal}/13</code>\n\n"
        f"⚠️ <i>Model sinyali. Kendi analizini ekle.</i>\n"
        f"⏰ <i>{datetime.now().strftime('%H:%M:%S')}</i>"
    )
    return msg

# ─── Ana işleyici ──────────────────────────────────────────
async def sinyal_isle(s: dict):
    """Gelen sinyali işle, filtrele, Telegram'a gönder"""
    log.info(f"Sinyal alındı: saat={s.get('saat')} kangal={s.get('kangal')} entry={s.get('entry')}")

    # Prob hesapla
    prob = prob_hesapla(s)

    # Filtre
    gec, neden = filtre_gec(s, prob)
    if not gec:
        log.info(f"❌ Reddedildi: {neden}")
        return

    # Duplicate kontrolü
    sembol = s.get("sembol", "XU030")
    zaman  = s.get("zaman", "")
    if son_sinyal_zaman.get(sembol) == zaman:
        log.info(f"⚠️  Duplicate, atlandı: {zaman}")
        return
    son_sinyal_zaman[sembol] = zaman

    # Telegram'a gönder
    msg = mesaj_olustur(s, prob)
    await telegram_gonder(msg)
    log.info(f"✅ Sinyal gönderildi: prob={prob:.2f} guc={guc_hesapla(prob)}")

# ─── Webhook handler ───────────────────────────────────────
async def webhook_handler(request):
    try:
        body = await request.text()
        log.info(f"Webhook POST: {body[:150]}")
        payload = json.loads(body)

        # Tip kontrolü — sadece viop_signal
        if payload.get("type") == "viop_signal":
            asyncio.create_task(sinyal_isle(payload))
            return web.Response(text="OK", status=200)

        return web.Response(text="IGNORED", status=200)

    except json.JSONDecodeError:
        log.error("JSON parse hatası")
        return web.Response(text="BAD JSON", status=400)
    except Exception as e:
        log.error(f"webhook_handler: {e}")
        return web.Response(text="ERROR", status=500)

async def health_handler(request):
    model_durum = "✅ yüklü" if MODEL else "⚠️ yok"
    return web.Response(
        text=f"VİOP v26 ALIVE | Model: {model_durum} | {datetime.now().strftime('%H:%M:%S')}",
        status=200
    )

async def test_handler(request):
    """
    GET /test — test sinyali gönder (deploy doğrulaması için)
    """
    test_payload = {
        "type":             "viop_signal",
        "sembol":           "XU030",
        "zaman":            datetime.now().strftime("%Y-%m-%d %H:%M"),
        "saat":             datetime.now().hour,
        "entry":            11500.0,
        "sl":               11450.0,
        "tp":               11590.0,
        "atr":              35.0,
        "prob":             0.72,
        "kangal":           7,
        "rvol":             1.3,
        "flow20":           0.62,
        "htf_rsi":          58.5,
        "htf_rsi_slope_3":  3.2,
        "htf_rsi_slope_2":  2.1,
        "htf_rsi_slope_5":  5.4,
        "htf_st_bull":      1,
        "rsi14":            56.0,
        "adx14":            24.0,
        "macd_hist":        8.5,
        "macd_line":        12.0,
        "dip14":            28.0,
        "dim14":            18.0,
        "cvd":              50000.0,
        "bo1":              1,
    }
    # Saati blocked_hours dışında tut
    if test_payload["saat"] in BLOCKED_HOURS:
        test_payload["saat"] = 11
    # Zaman'ı benzersiz yap (duplicate kaçınsın)
    test_payload["zaman"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    asyncio.create_task(sinyal_isle(test_payload))
    return web.Response(text="TEST SİNYAL GÖNDERİLDİ", status=200)

# ─── Uygulama ──────────────────────────────────────────────
async def main():
    model_yukle()

    if not TELEGRAM_TOKEN:
        log.error("❌ TELEGRAM_TOKEN env var eksik!")
    if not CHAT_ID:
        log.error("❌ CHAT_ID env var eksik!")

    app = web.Application()
    app.router.add_post("/webhook/viop", webhook_handler)
    app.router.add_get("/health",        health_handler)
    app.router.add_get("/",              health_handler)
    app.router.add_get("/test",          test_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    log.info(f"🚀 VİOP Server başladı — port {port}")
    log.info(f"   POST /webhook/viop  ← TradingView alert buraya")
    log.info(f"   GET  /test          ← deploy test")
    log.info(f"   GET  /health        ← Railway health check")

    # Sonsuza çalış
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
