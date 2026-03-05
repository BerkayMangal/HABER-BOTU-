# Bu dosyayı sadece Telethon session string almak için
# KENDİ BİLGİSAYARINDA çalıştır (Colab değil!)
# pip install telethon

from telethon.sync import TelegramClient
from telethon.sessions import StringSession

API_ID   = 0          # my.telegram.org'dan al
API_HASH = ""         # my.telegram.org'dan al

with TelegramClient(StringSession(), API_ID, API_HASH) as client:
    print("\n✅ SESSION STRING (Railway'e ekle):")
    print("=" * 60)
    print(client.session.save())
    print("=" * 60)
