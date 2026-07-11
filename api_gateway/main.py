# api_gateway/main.py
from fastapi import FastAPI, Request, status
from contextlib import asynccontextmanager
import aio_pika
from sqlalchemy import select

# Import our data contracts and database models
from shared.schemas import EventCreate, EventMessage
from shared.database import engine, AsyncSessionLocal
from shared.models import Base, Tenant, Endpoint

RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---------------------------------------------------------
    # 1. DATABASE INITIALIZATION
    # ---------------------------------------------------------
    print("Initializing Database...")
    # Instruct SQLAlchemy to physically create the tables in Postgres safely
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Silently inject the dummy data if it doesn't exist
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Tenant).where(Tenant.id == "ten_123"))
        if not result.scalars().first():
            print("Injecting dummy tenant 'ten_123'...")
            db.add(Tenant(id="ten_123", name="Test Customer"))
            db.add(Endpoint(
                tenant_id="ten_123",
                target_url="https://httpbin.org/post", 
                subscribed_events=["payment.success"]
            ))
            await db.commit()

        # Inject the failing customer for retry testing
        result_fail = await db.execute(select(Tenant).where(Tenant.id == "ten_fail"))
        if not result_fail.scalars().first():
            print("Injecting dummy tenant 'ten_fail'...")
            db.add(Tenant(id="ten_fail", name="Failing Customer"))
            db.add(Endpoint(
                tenant_id="ten_fail",
                target_url="https://httpbin.org/status/500",  # <--- GUARANTEES A 500 ERROR
                subscribed_events=["payment.failed"]
            ))
            await db.commit()
    print("Database ready.")

    # ---------------------------------------------------------
    # 2. RABBITMQ INITIALIZATION
    # ---------------------------------------------------------
    print("Connecting to RabbitMQ...")
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    app.state.amqp_connection = connection
    
    # Let the server run
    yield 
    
    # ---------------------------------------------------------
    # 3. CLEAN SHUTDOWN
    # ---------------------------------------------------------
    print("Closing RabbitMQ connection...")
    await connection.close()


app = FastAPI(lifespan=lifespan)

@app.post("/events", status_code=status.HTTP_202_ACCEPTED)
async def create_event(event: EventCreate, request: Request):
    """
    Receives an EventCreate payload, upgrades it to an EventMessage, 
    and publishes it to the raw_events_bus.
    """
    internal_message = EventMessage(**event.model_dump())
    
    connection = request.app.state.amqp_connection
    channel = await connection.channel()
    exchange = await channel.get_exchange("raw_event_bus")
    
    amqp_message = aio_pika.Message(
        body=internal_message.model_dump_json().encode(),
        content_type="application/json"
    )
    
    await exchange.publish(
        message=amqp_message,
        routing_key=internal_message.event_type
    )
    
    return {
        "status": "accepted",
        "event_id": internal_message.event_id,
        "message": "Webhook queued for processing."
    }