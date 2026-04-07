import os
import asyncio
import logging
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field
from google import genai
from google.api_core import exceptions

load_dotenv()
logger = logging.getLogger("TelegramService")

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3-flash")


class LLMResult(BaseModel):
    language: str = Field(description="en, ru, kk, mixed, unknown")
    category: str = Field(description="event or announcement")
    summary: str = Field(description="Short one-sentence summary")
    tags: list[str] = Field(default_factory=list, description="3-6 short keywords")
    event_date: Optional[str] = Field(default=None, description="ISO datetime string or null")
    location: Optional[str] = Field(default=None, description="Location or null")
    confidence: float = Field(description="0 to 1 confidence score")


SYSTEM_PROMPT = """
You are an information extraction assistant.

Return ONLY valid JSON matching the schema.

Allowed values:
- language: "en", "ru", "kz", "mixed", "unknown"
- category: "event", "announcement"

Classification rules:
- event: a scheduled activity that people can attend or participate in
- announcement: informational update, notice, reminder, policy/service/schedule change

Extraction rules:
- summary: one short sentence
- tags: 3 to 6 useful short keywords
- event_date: only if clearly present in the post, otherwise null
- location: only if clearly present in the post, otherwise null
- confidence: number from 0 to 1
- do not invent facts
- if multiple languages are used, return "mixed"
""".strip()


class GeminiEnricher:
    def __init__(self):
        self.client = genai.Client()
        self.model = GEMINI_MODEL

    async def enrich(self, text: str) -> Optional[LLMResult]:
        try:
            response = await asyncio.to_thread(
                self.client.models.generate_content,
                model=self.model,
                contents=text,
                config={
                    "system_instruction": SYSTEM_PROMPT,
                    "response_mime_type": "application/json",
                    "response_schema": LLMResult,
                    "temperature": 0,
                },
            )
            return response.parsed
        except exceptions.ResourceExhausted as e:
            logger.warning("Gemini Quota Exceeded (429). Throttling required.")
            raise e # Raise it so main.py knows to wait
        except Exception as e:
            logger.error("Gemini enrich failed: %s", e)
            return None