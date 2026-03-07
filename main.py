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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)