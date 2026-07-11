import uuid
from datetime import datetime, timezone
from sqlalchemy import String, Boolean, DateTime, JSON, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase

class Base(DeclarativeBase):
    pass


def generate_id(prefix: str):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(String(50), primary_key=True, default=lambda: generate_id("tenant"))
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)

    # We will use this later to digitally sign the webhook payloads!
    hmac_secret_key: Mapped[str] = mapped_column(String, nullable=False, default=lambda: uuid.uuid4().hex)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # Links to the endpoints table
    endpoints: Mapped[list["Endpoint"]] = relationship(back_populates="tenant")


class Endpoint(Base):
    __tablename__ = "endpoints"

    id: Mapped[str] = mapped_column(String(50), primary_key=True, default=lambda: generate_id("endpoint"))
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    target_url: Mapped[str] = mapped_column(String(200), nullable=False)
    
    # Example: ["payment.success", "user.created"]. 
    # If a customer only wants payment alerts, we filter using this column.
    subscribed_events: Mapped[list[str]] = mapped_column(JSON, nullable=False)

    tenant: Mapped["Tenant"] = relationship(back_populates="endpoints")
    

class WebhookLog(Base):
    __tablename__ = "webhook_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String, nullable=False, index=True) # Ensures Idempotency
    endpoint_id: Mapped[str] = mapped_column(ForeignKey("endpoints.id"), nullable=False)
    
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="PENDING") # PENDING, SUCCESS, FAILED
    
    http_status_code: Mapped[int] = mapped_column(Integer, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))