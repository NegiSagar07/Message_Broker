from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@postgres:5432/webhook_db",
)

RETRY_QUEUE_1M_TTL_MS = int(os.getenv("RETRY_QUEUE_1M_TTL_MS", "60000"))
RETRY_QUEUE_5M_TTL_MS = int(os.getenv("RETRY_QUEUE_5M_TTL_MS", "300000"))

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://guest:guest@rabbitmq:5672/",
)
