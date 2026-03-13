import os
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
import psycopg2
import psycopg2.extras
import re

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TelegramService")

# Load Config
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "telegram_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

def detect_language(text: str):
    text = text.lower()

    if re.search(r"[әіңғүұқөһ]", text):
        return "kz"

    if re.search(r"[a-z]", text) and not re.search(r"[а-я]", text):
        return "en"

    return "ru"
def get_peer():
    if TARGET_CHANNEL.lstrip('-').isdigit():
        return int(TARGET_CHANNEL)
    return TARGET_CHANNEL

# Database Manager
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            message_id BIGINT UNIQUE,
            date TIMESTAMP,
            sender_id BIGINT,
            text TEXT,
            media_type TEXT
        )
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date DESC)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id)
    """)
    conn.commit()
    conn.close()

def save_message(msg):
    try:
        conn = get_db_connection()
        c = conn.cursor()

        media_type = None
        if msg.media:
            media_type = type(msg.media).__name__

        text_content = msg.message if msg.message else ""

        c.execute("""
            INSERT INTO messages (message_id, date, sender_id, text, media_type)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (message_id)
            DO UPDATE SET
                date = EXCLUDED.date,
                sender_id = EXCLUDED.sender_id,
                text = EXCLUDED.text,
                media_type = EXCLUDED.media_type
        """, (
            msg.id,
            msg.date.replace(tzinfo=None) if msg.date else None,
            msg.sender_id,
            text_content,
            media_type
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"DB Error: {e}")

# Background Worker
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

async def sync_history():
    logger.info(f"Syncing history for {TARGET_CHANNEL}...")
    try:
        channel = await client.get_entity(get_peer())
        async for msg in client.iter_messages(channel, limit=None):
            save_message(msg)
        logger.info("History sync complete.")
    except Exception as e:
        logger.error(f"Sync failed: {e}")

@client.on(events.NewMessage(chats=get_peer()))
async def handler(event):
    logger.info(f"New message received: {event.message.id}")
    save_message(event.message)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await client.start()
    asyncio.create_task(sync_history())
    yield
    await client.disconnect()

app = FastAPI(lifespan=lifespan)

from fastapi.middleware.cors import CORSMiddleware


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/messages")
def get_messages(limit: int = 100, offset: int = 0, search: Optional[str] = None):
    conn = get_db_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    query = "SELECT * FROM messages"
    params = []

    if search:
        query += " WHERE text ILIKE %s"
        params.append(f"%{search}%")

    query += " ORDER BY date DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    data = []
    for row in rows:
        item = dict(row)
        if item.get("date") is not None:
            item["date"] = item["date"].isoformat()
        data.append(item)

    return JSONResponse(content=data, media_type="application/json; charset=utf-8")
@app.get("/news")
def get_news(limit: int = 10):
    conn = get_db_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    c.execute("""
        SELECT * FROM messages
        WHERE text IS NOT NULL AND text <> ''
        ORDER BY date DESC
        LIMIT %s
    """, (limit * 9,))

    rows = c.fetchall()
    conn.close()

    # Группируем сообщения по "окну времени" — 30 секунд
    # Сообщения на 3 языках отправляются почти одновременно
    from datetime import datetime, timedelta

    groups = []  # список: {"date": ..., "ru": ..., "en": ..., "kz": ...}

    for row in rows:
        text = row["text"]
        if not text:
            continue

        lang = detect_language(text)
        msg_date = row["date"]  # уже datetime объект из psycopg2

        # Ищем существующую группу в пределах 30 секунд
        matched_group = None
        for group in groups:
            if abs((group["_date"] - msg_date).total_seconds()) <= 30:
                matched_group = group
                break

        if matched_group is None:
            matched_group = {
                "_date": msg_date,
                "date": msg_date.isoformat() if msg_date else None,
                "ru": None,
                "en": None,
                "kz": None
            }
            groups.append(matched_group)

        # Только если язык ещё не заполнен
        if matched_group[lang] is None:
            matched_group[lang] = text

    # Убираем служебное поле и берём только полные/частичные группы
    result = []
    for g in groups:
        del g["_date"]
        result.append(g)

    return JSONResponse(content=result[:limit], media_type="application/json; charset=utf-8")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)