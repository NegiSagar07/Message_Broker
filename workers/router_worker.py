# workers/router_worker.py
import asyncio
import aio_pika
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from shared.database import AsyncSessionLocal
from shared.models import Endpoint
from shared.schemas import EventMessage, DispatchMessage
from shared.settings import RABBITMQ_URL
from shared.rabbitmq import connect_rabbitmq

async def main():
    print("Starting Router Worker...")
    
    connection = await connect_rabbitmq(RABBITMQ_URL)
    
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=10)
        
        queue = await channel.get_queue("router_queue")
        dispatch_exchange = await channel.get_exchange("dispatch_bus")

        async def process_message(message: aio_pika.IncomingMessage):
            async with message.process(ignore_processed=True):
                try:
                    event = EventMessage.model_validate_json(message.body.decode())
                    print(f"[*] Processing Event: {event.event_id} | Tenant: {event.tenant_id}")

                    async with AsyncSessionLocal() as db:
                        # [FIXED] The syntax error in the parentheses is resolved
                        stmt = (
                            select(Endpoint)
                            .options(joinedload(Endpoint.tenant))
                            .where(Endpoint.tenant_id == event.tenant_id)
                        )
                        result = await db.execute(stmt)
                        endpoint_record = result.scalars().first()

                        if not endpoint_record:
                            print(f"[!] No URL found for {event.tenant_id}. Dropping message.")
                            await message.reject(requeue=False) 
                            return

                        # [FIXED] Mapped dispatch_id to the incoming event_id for traceability
                        dispatch_data = DispatchMessage(
                            dispatch_id=event.event_id, 
                            tenant_id=event.tenant_id,
                            event_type=event.event_type,
                            target_url=endpoint_record.target_url,
                            payload=event.payload,
                            retry_count=event.retry_count,
                            hmac_secret_key=endpoint_record.tenant.hmac_secret_key
                        )

                    # [FIXED] Changed dispatch_msg to dispatch_data
                    amqp_msg = aio_pika.Message(
                        body=dispatch_data.model_dump_json().encode(),
                        content_type="application/json"
                    )
                    
                    await dispatch_exchange.publish(
                        message=amqp_msg,
                        routing_key="dispatch.execute"
                    )

                    await message.ack()
                    print(f"[v] Successfully routed {event.event_id} to {endpoint_record.target_url}")

                except Exception as e:
                    print(f"[X] Worker Error or Poison Message: {e}")
                    await message.reject(requeue=False)

        print("Waiting for messages. To exit press CTRL+C")
        await queue.consume(process_message)
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())