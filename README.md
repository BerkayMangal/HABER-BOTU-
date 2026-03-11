# 🤖 Berkay Terminator v3.3 & Fundamentals v4

[![Python 3.11](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Supported-2496ED.svg)](https://www.docker.com/)
[![Claude AI](https://img.shields.io/badge/AI-Claude_3.5-7C3AED.svg)](https://www.anthropic.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

Berkay Terminator, **Borsa İstanbul (BİST)** ve küresel piyasalar için geliştirilmiş; haber akışını yapay zeka ile filtreleyen, algoritmik temel analiz yapan ve makine öğrenimi (XGBoost) ile VİOP sinyalleri üreten tam teşekküllü bir Telegram piyasa terminalidir.

---

## ✨ Öne Çıkan Özellikler

* **🧠 AI Destekli Haber Filtresi:** Onlarca RSS ve API kaynağından akan haberleri Claude 3.5 ile "Trader" gözüyle skorlar. Sadece piyasayı etkileyecek (7+ puan) haberleri Telegram'dan gönderir.
* **📊 Berkay Fundamentals v4:** Herhangi bir hisse kodunu (Örn: `TCELL`) bota yazdığınızda saniyeler içinde Piotroski F, Altman Z, Beneish M ve Graham metriklerini hesaplar; değer, kalite ve büyüme skoru üretir.
* **🎯 VİOP ML Sinyal Motoru:** TradingView'dan gelen ham sinyalleri yakalar. Eğitilmiş XGBoost modeli ile sinyalin başarı ihtimalini (Prob) hesaplar ve sadece kârlı olma ihtimali yüksek (>%65) işlemleri iletir.
* **🌍 Makro & Radar Sistemi:** Brent petrol, DXY, VIX ve USDTRY'deki sert hareketleri anlık yakalar. ABD/TR makroekonomik verilerinden 30 dk önce uyarır, açıklandığı an sapmayı hesaplar.
* **🔥 Twitter (X) Sentiment:** `twscrape` kullanarak hisselerin anlık sosyal medya sıcaklık (heat) haritasını çıkarır.

---

## 🏗️ Sistem Mimarisi

Proje, tek bir Docker container'ı içinde paralel çalışan 3 farklı mikro-servisten oluşur:

1. `main.py` : Ana haber motoru, AI skorlama, piyasa radarı ve periyodik raporlar (Sabah/Öğle/Akşam brifingleri).
2. `fa_bot.py` : Sadece finansal verilere (yfinance) odaklanmış bağımsız Temel Analiz botu.
3. `viop_server.py` : `aiohttp` tabanlı asenkron webhook sunucusu ve ML Predictor.

---

## 🚀 Kurulum & Çalıştırma

Sistem **Railway** veya herhangi bir Docker ortamında çalışmaya hazırdır. 

**1. Çevresel Değişkenler (`.env`)**
Aşağıdaki değişkenleri sunucunuza ekleyin:
```ini
TELEGRAM_TOKEN=ana_bot_token
FA_BOT_TOKEN=temel_analiz_bot_token
CHAT_ID=telegram_chat_id
ANTHROPIC_KEY=claude_api_key
FINNHUB_KEY=finnhub_api_key
MARKETAUX_KEY=marketaux_api_key
TELEGRAM_API_ID=telethon_id
TELEGRAM_API_HASH=telethon_hash
TELETHON_SESSION=telethon_session_string
PORT=8080
