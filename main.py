import os
import asyncio
import sqlite3
import logging
from contextlib import asynccontextmanager
from typing import Optional, List

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TelegramService")

# Load Config
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL")
DB_NAME = "telegram_data.db"

def get_peer():
    if TARGET_CHANNEL.lstrip('-').isdigit():
        return int(TARGET_CHANNEL)
    return TARGET_CHANNEL

# Database Manager
def get_db_connection():
    """Creates a database connection with UTF-8 enforcement"""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    # Force the database to return Python Unicode strings, not bytes
    conn.text_factory = lambda x: str(x, 'utf-8', 'ignore') if isinstance(x, bytes) else x
    return conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            message_id INTEGER UNIQUE,
            date TEXT,
            sender_id INTEGER,
            text TEXT,
            media_type TEXT
        )
    ''')
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

        c.execute('''
            INSERT OR REPLACE INTO messages 
            (message_id, date, sender_id, text, media_type)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            msg.id, 
            msg.date.strftime('%Y-%m-%d %H:%M:%S'),
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
    """Downloads past messages from the channel"""
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
    """Listens for new messages in real-time"""
    logger.info(f"New message received: {event.message.id}")
    save_message(event.message)

# FastAPI 
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
    c = conn.cursor()
    
    query = "SELECT * FROM messages"
    params = []
    
    if search:
        query += " WHERE text LIKE ?"
        params.append(f"%{search}%")
        
    query += " ORDER BY date DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    
    data = [dict(row) for row in rows]
    
    return JSONResponse(content=data, media_type="application/json; charset=utf-8")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)