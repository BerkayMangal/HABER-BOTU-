BERKAY TERMINATOR v3.3

BIST & Global Markets — AI-Powered Trading Intelligence Terminal

3 bağımsız motor, tek container. Haber akışını Claude AI ile skorlar, temel analizi saniyede çıkarır, VİOP sinyallerini XGBoost ile filtreler.
⸻
Ne Yapar?

Terminator (main.py) — 40+ kaynaktan haber toplar, Claude AI ile BIST etkisini skorlar (0-10), sadece trade edilebilir haberleri Telegram'a gönderir. Brent/DXY/VIX/USDTRY fiyat radarı, makro ekonomik takvim (30dk önceden uyarı + veri anı + 5dk tepki), momentum algılama, X (Twitter) sentiment analizi.

Fundamentals (fa_bot.py) — Telegram'a THYAO yaz, 2 saniyede gelir. Piotroski F-Score, Altman Z-Score, Beneish M-Score, Graham Güvenlik Marjı. 7 kategoride 100 üzerinden puanlama. 40 hisse pre-cache, butonlar anlık.

VİOP Server (viop_server.py) — TradingView webhook'larını dinler, XGBoost v26 modeli ile 17 teknik özellikten kazanma olasılığı hesaplar, %65+ sinyalleri Telegram'a atar.
⸻
Mimari

┌─────────────────────────────────────────┐
│            Docker Container             │
│                                         │
│  main.py ──── Haber + Radar + Macro     │
│  fa_bot.py ── Temel Analiz (ayrı token) │
│  viop_server.py ── ML Webhook (:8080)   │
│                                         │
│  start.sh → 3 process paralel           │
└─────────────────────────────────────────┘


Her bot kendi Telegram token'ı ile çalışır. Birbirini bloklamaz. Container içinde paralel, birbirinden bağımsız.
⸻
Haber Motoru Detay
Katman	Ne Yapar
RSS (40+ kaynak)	Reuters, AP, FT, WSJ, TCMB, SPK, AA, Bloomberg HT, Al Monitor, ISW...
API	Finnhub, Marketaux, Finnhub BIST company news
Scraping	Foreks, BorsaGundem, Investing TR şirket haberleri
Telethon	Telegram kanal dinleme (opsiyonel)
Claude AI	Her haberi 0-10 skorlar, yön belirler, sembol eşler
Dedup	MD5 hash + SQLite kalıcı + Novelty memory + Event clustering
Source Tiering	T1 (TCMB/Reuters) → +1, T3 (bilinmeyen) → -1
Dinamik Eşik	20dk sessizse eşik düşer, haber gelince reset
⸻
Piyasa Radarı
Enstrüman	Sarı Alarm	Kırmızı Alarm
Brent	%1.5	%3.0
DXY	%0.5	%1.0
VIX	%5.0	%10.0
XU030	%1.0	%2.0
USDTRY	%0.5	%1.0

Brent teknik analizi 6 saatte bir: Pivot seviyeleri, MA20, gün içi range, teknik yorum.
⸻
Makro Motor

TCMB PPK tarihleri ve TÜFE açıklama günleri hardcoded (2025-2026). Finnhub economic calendar ile ABD verileri (CPI, NFP, Fed faiz) otomatik takip.

3 fazlı sistem:
1. Veri gelmeden 15-45dk önce → uyarı + beklenti + senaryo
2. Veri geldiği an → actual vs forecast, surprise hesabı
3. 5dk sonra → ilk piyasa tepkisi (USDTRY, Altın, BIST)
⸻
Temel Analiz (Fundamentals V4)

Telegram'a ticker yaz, analiz gelir. /top10 ile 40 hisselik evreni tara.

Skorlama: Value, Quality, Growth, Balance, Earnings, Moat, Capital Allocation → ağırlıklı ortalama = genel skor.

Efsane metrikler: Piotroski F (9 test), Altman Z (iflas riski), Beneish M (manipülasyon), Graham intrinsic value, Buffett/Graham filtresi.

Stil: Quality Compounder, Deep Value, GARP, Premium Compounder, High-Risk Turnaround.
⸻
VİOP ML Server

TradingView'dan Pine Script alertleri → webhook → XGBoost prediction → Telegram.

Model: viop_v26_model.pkl — walk-forward validated, 6 pencere, HTF RSI slope #1 SHAP feature.

POST /webhook/viop   ← TradingView alert
GET  /health         ← Railway health check
GET  /test           ← Deploy test

⸻
Telegram Komutları

Terminator (Ana Bot)
Komut	Açıklama
/radar	Canlı Brent + XU030 + USDTRY
/durum	Sistem durumu, DB, eşikler
/heat THYAO	X (Twitter) sentiment skoru
/watch THYAO GARAN	Watchlist'e ekle
/unwatch THYAO	Watchlist'ten çıkar
/watchlist	Listeyi gör
/help	Komut listesi

Fundamentals Bot
Komut	Açıklama
TCELL	Ticker yaz, analiz gelir
/top10	En iyi 10 hisse
/ping	Bot canlı mı
⸻
Zamanlanmış Raporlar
Saat	Rapor
09:45	Sabah brifing — gece özeti + günün takvimi + gözetleme
13:00	Öğlen rapor — sabahın en sıcak 5 hissesi
17:45	Akşam özet — günün istatistikleri + yarının takvimi
6 saatte bir	Brent teknik + sistem raporu
⸻
Kurulum (Railway)

Gerekli Environment Variables

TELEGRAM_TOKEN=terminator_bot_token
FA_BOT_TOKEN=fundamentals_bot_token
CHAT_ID=telegram_chat_id
ANTHROPIC_KEY=claude_api_key
FINNHUB_KEY=finnhub_api_key
MARKETAUX_KEY=marketaux_api_key
TELEGRAM_API_ID=telethon_api_id
TELEGRAM_API_HASH=telethon_api_hash


Opsiyonel

CLAUDE_MODEL=claude-sonnet-4-20250514
TELETHON_SESSION=string_session
TELEGRAM_KANALLARI=kanal1,kanal2
PORT=8080


Deploy

git push  # Railway otomatik build + deploy


Veya lokal:

docker build -t terminator .
docker run --env-file .env -p 8080:8080 terminator

⸻
Dosya Yapısı

├── main.py              # Haber motoru + radar + macro (2900 satır)
├── fa_bot.py            # Temel analiz botu (720 satır)
├── viop_server.py       # VİOP ML webhook server
├── viop_v26_model.pkl   # Eğitilmiş XGBoost model
├── start.sh             # 3 process'i paralel başlatır
├── Dockerfile           # Python 3.11 + Istanbul TZ
├── requirements.txt     # Tüm bağımlılıklar
└── README.md

⸻
Tech Stack

Python 3.11, Claude AI (Anthropic), python-telegram-bot, Telethon, yfinance, scikit-learn, XGBoost, feedparser, BeautifulSoup, aiohttp, SQLite, joblib, twscrape, Railway
⸻
Yasal Uyarı

Bu proje eğitim ve araştırma amaçlıdır. Üretilen skorlar, analizler ve sinyaller yatırım tavsiyesi değildir. Geçmiş performans gelecek sonuçları garanti etmez.
⸻
Built by Berkay — CFO, 20 yıl kurumsal finans, Deutsche Bank → Crypto → AI Trading
