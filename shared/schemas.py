# shared/schemas.py
import uuid
from datetime import datetime, timezone
from pydantic import BaseModel, Field
from typing import Dict, Any

def generate_evt_id():
    return f"evt_{uuid.uuid4().hex[:10]}"

def generate_disp_id():
    return f"disp_{uuid.uuid4().hex[:10]}"

# ---------------------------------------------------------
# 1. The Ingestion Schema (What FastAPI accepts)
# ---------------------------------------------------------
class EventCreate(BaseModel):
    """The raw data sent to our API by an internal microservice."""
    tenant_id: str = Field(..., description="The ID of the customer (e.g., ten_123)")
    event_type: str = Field(..., description="The routing key (e.g., payment.failed)")
    payload: Dict[str, Any] = Field(..., description="The actual JSON data to forward")

# ---------------------------------------------------------
# 2. The Internal Schema (What FastAPI puts in RabbitMQ)
# ---------------------------------------------------------
class EventMessage(EventCreate):
    """The 'Thin' JSON moving from FastAPI -> raw_events_bus."""
    event_id: str = Field(default_factory=generate_evt_id, description="Idempotency key")
    retry_count: int = Field(default=0, description="Tracks DLX loops")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# ---------------------------------------------------------
# 3. The Dispatch Schema (What the Router Worker creates)
# ---------------------------------------------------------
class DispatchMessage(EventMessage):
    """The 'Fat' JSON moving from Router Worker -> dispatch_bus."""
    dispatch_id: str = Field(default_factory=generate_disp_id)
    target_url: str = Field(..., description="The physical URL to hit")