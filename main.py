import os
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional
from datetime import datetime

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
import psycopg2
import psycopg2.extras

from gemini_enricher import GeminiEnricher
from google.api_core import exceptions

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

# Gemini enricher
enricher = GeminiEnricher()


def get_peer():
    if TARGET_CHANNEL.lstrip("-").isdigit():
        return int(TARGET_CHANNEL)
    return TARGET_CHANNEL


# Database Manager
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def init_db():
    conn = get_db_connection()
    c = conn.cursor()

    # Base table
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

    # Add new columns safely for existing DB
    c.execute("""
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS language TEXT
    """)
    c.execute("""
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS category TEXT
    """)
    c.execute("""
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS summary TEXT
    """)
    c.execute("""
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS tags JSONB
    """)
    c.execute("""
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS event_date TIMESTAMP NULL
    """)
    c.execute("""
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS location TEXT
    """)
    c.execute("""
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS confidence DOUBLE PRECISION
    """)

    # Indexes
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date DESC)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_category ON messages(category)
    """)

    conn.commit()
    conn.close()
    conn = get_db_connection()
    c = conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            message_id BIGINT UNIQUE,
            date TIMESTAMP,
            sender_id BIGINT,
            text TEXT,
            media_type TEXT,

            language TEXT,
            category TEXT,
            summary TEXT,
            tags JSONB,
            event_date TIMESTAMP NULL,
            location TEXT,
            confidence DOUBLE PRECISION
        )
        """
    )

    c.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date DESC)
        """
    )
    c.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id)
        """
    )
    c.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_messages_category ON messages(category)
        """
    )

    conn.commit()
    conn.close()


async def save_message(msg):
    conn = None
    try:
        text_content = msg.message if msg.message else ""
        if not text_content.strip():
            return

        conn = get_db_connection()
        c = conn.cursor()

        # 1. Проверка — уже обработано?
        c.execute("SELECT category FROM messages WHERE message_id = %s", (msg.id,))
        existing = c.fetchone()

        if existing and existing[0] is not None:
            return

        enriched = None

        # 2. Gemini с retry
        for attempt in range(2):  # максимум 2 попытки
            try:
                await asyncio.sleep(6)  # троттлинг
                enriched = await enricher.enrich(text_content)
                break
            except exceptions.ResourceExhausted as e:
                logger.warning(f"Quota hit (attempt {attempt+1}). Sleeping 30s...")
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Gemini error: {e}")
                break

        # 3. Парсим дату
        event_date = None
        if enriched and enriched.event_date:
            try:
                event_date = datetime.fromisoformat(
                    enriched.event_date.replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except Exception:
                pass

        # 4. Сохраняем даже если enriched=None
        c.execute(
            """
            INSERT INTO messages (
                message_id, date, sender_id, text, media_type,
                language, category, summary, tags, event_date, location, confidence
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (message_id)
            DO UPDATE SET
                date = EXCLUDED.date,
                sender_id = EXCLUDED.sender_id,
                text = EXCLUDED.text,
                media_type = EXCLUDED.media_type,
                language = EXCLUDED.language,
                category = EXCLUDED.category,
                summary = EXCLUDED.summary,
                tags = EXCLUDED.tags,
                event_date = EXCLUDED.event_date,
                location = EXCLUDED.location,
                confidence = EXCLUDED.confidence
            """,
            (
                msg.id,
                msg.date.replace(tzinfo=None) if msg.date else None,
                msg.sender_id,
                text_content,
                type(msg.media).__name__ if msg.media else None,
                enriched.language if enriched else None,
                enriched.category if enriched else None,
                enriched.summary if enriched else None,
                psycopg2.extras.Json(enriched.tags) if enriched and enriched.tags else None,
                event_date,
                enriched.location if enriched else None,
                enriched.confidence if enriched else None,
            ),
        )

        conn.commit()
        logger.info(f"✅ Saved message {msg.id}")

    except Exception as e:
        logger.error("Error saving message %s: %s", getattr(msg, "id", "unknown"), e)

    finally:
        if conn:
            conn.close()

# Telegram Client
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)


async def sync_history():
    LIMIT_RECENT = 30 
    
    logger.info("Syncing the last %s messages for %s...", LIMIT_RECENT, TARGET_CHANNEL)
    try:
        channel = await client.get_entity(get_peer())
        
        # limit=30 ensures we only fetch the most recent news
        async for msg in client.iter_messages(channel, limit=LIMIT_RECENT):
            await save_message(msg)
            
        logger.info("Initial sync of last 30 messages complete.")
    except Exception as e:
        logger.error("Sync failed: %s", e)


@client.on(events.NewMessage(chats=get_peer()))
async def handler(event):
    logger.info("New message received: %s", event.message.id)
    await save_message(event.message)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await client.start()
    asyncio.create_task(sync_history())
    yield
    await client.disconnect()


app = FastAPI(lifespan=lifespan)

from fastapi.middleware.cors import CORSMiddleware

origins = [
    "http://localhost:5173",
    "https://frontend-diploma-8zym.vercel.app",
    "https://kbtucare.site",
    "https://www.kbtucare.site",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/messages")
def get_messages(
    limit: int = 10,
    offset: int = 0,
    category: Optional[str] = None,
):
    conn = get_db_connection()
    # Get Total Count for the frontend progress bar/pagination
    c_count = conn.cursor()
    count_query = "SELECT COUNT(*) FROM messages WHERE 1=1"
    if category: count_query += f" AND category = '{category}'"
    c_count.execute(count_query)
    total = c_count.fetchone()[0]

    # Get Data
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query = "SELECT * FROM messages WHERE 1=1"
    params = []
    if category:
        query += " AND category = %s"
        params.append(category)
    
    query += " ORDER BY date DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": rows
    }

import asyncio
from datetime import datetime
import logging
import psycopg2.extras

logger = logging.getLogger("telegram_worker")

BATCH_SIZE = 2       # количество сообщений за один раз
BATCH_DELAY = 30     # секунд между пачками
CHECK_INTERVAL = 600 # секунд между циклами проверки базы

async def gradual_enrich_worker():
    while True:
        try:
            conn = get_db_connection()
            c = conn.cursor()
            # Берём небольшую пачку необработанных сообщений
            c.execute(
                "SELECT message_id, text FROM messages WHERE category IS NULL OR language IS NULL ORDER BY date ASC LIMIT %s",
                (BATCH_SIZE,)
            )
            rows = c.fetchall()
            conn.close()

            if not rows:
                logger.info("No unenriched messages found.")
            else:
                for msg_id, text_content in rows:
                    try:
                        await asyncio.sleep(6)  # throttle между запросами
                        enriched = await enricher.enrich(text_content)
                    except exceptions.ResourceExhausted:
                        logger.warning("Gemini quota hit. Stopping batch processing for now.")
                        break  # прекращаем обработку, лимит исчерпан

                    if enriched:
                        conn = get_db_connection()
                        c = conn.cursor()
                        event_date = None
                        if enriched.event_date:
                            try:
                                event_date = datetime.fromisoformat(
                                    enriched.event_date.replace("Z", "+00:00")
                                ).replace(tzinfo=None)
                            except Exception:
                                pass
                        c.execute(
                            """
                            UPDATE messages
                            SET language=%s, category=%s, summary=%s, tags=%s, event_date=%s, location=%s, confidence=%s
                            WHERE message_id=%s
                            """,
                            (
                                enriched.language,
                                enriched.category,
                                enriched.summary,
                                psycopg2.extras.Json(enriched.tags) if enriched.tags else None,
                                event_date,
                                enriched.location,
                                enriched.confidence,
                                msg_id
                            ),
                        )
                        conn.commit()
                        conn.close()
                        logger.info(f"✅ Enriched message {msg_id}")
                    await asyncio.sleep(BATCH_DELAY)  # пауза между сообщениями
        except Exception as e:
            logger.error(f"Error in gradual worker: {e}")

        await asyncio.sleep(CHECK_INTERVAL)  # ждём следующую проверку базы


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(gradual_enrich_worker())

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)