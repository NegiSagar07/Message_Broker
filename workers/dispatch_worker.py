# workers/dispatch_worker.py
import asyncio
import aio_pika
import httpx
from shared.security import generate_hmac_signature

from shared.schemas import DispatchMessage
from shared.settings import RABBITMQ_URL
from shared.rabbitmq import connect_rabbitmq
MAX_RETRIES = 3
BASE_DELAY_SEC = 5

async def main():
    print("Starting Advanced Dispatch Worker (Exponential Backoff enabled)...")
    
    connection = await connect_rabbitmq(RABBITMQ_URL)
    
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=10)
        
        # We need BOTH the primary queue (to pull from) and the delay exchange (to send failures to)
        queue = await channel.get_queue("primary_dispatch_queue")
        delay_exchange = await channel.get_exchange("delay_bus")
        dlx_exchange = await channel.get_exchange("dlx_bus")

        async def process_message(message: aio_pika.IncomingMessage):
            async with message.process(ignore_processed=True):
                try:
                    dispatch_data = DispatchMessage.model_validate_json(message.body.decode())
                    print(f"\n[*] Dispatching: {dispatch_data.dispatch_id} | Attempt: {dispatch_data.retry_count + 1}")
                    
                    signature = generate_hmac_signature(
                        secret_key=dispatch_data.hmac_secret_key,
                        payload=dispatch_data.payload
                    )

                    print(f"[*] Generated HMAC Signature: {signature[:15]}...")

                    # 2. Inject it into the HTTP headers
                    async with httpx.AsyncClient() as client:
                        response = await client.post(
                            dispatch_data.target_url,
                            json=dispatch_data.payload,
                            headers={"X-Webhook-Signature": signature}, # <--- [NEW] Stamp the envelope
                            timeout=10.0
                        )
                        # This automatically triggers the exception block if the status is 4xx or 5xx
                        response.raise_for_status() 
                        
                        print(f"[v] Success! Customer server returned {response.status_code}.")
                        await message.ack()

                # Catch HTTP errors (502, 404, etc.) or connection timeouts
                except (httpx.HTTPStatusError, httpx.RequestError) as e:
                    print(f"[!] Delivery Failed: {e}")
                    
                    # 1. Check if we have retries left
                    if dispatch_data.retry_count < (MAX_RETRIES - 1):
                        dispatch_data.retry_count += 1
                        if dispatch_data.retry_count == 1:
                            target_routing_key = "delay.1m"
                        elif dispatch_data.retry_count == 2:
                            target_routing_key = "delay.5m"
                    
                        # 2. Package the message for retry
                        retry_msg = aio_pika.Message(
                            body=dispatch_data.model_dump_json().encode(),
                            content_type="application/json"
                        )

                        # 3. Publish to the Delay Exchange
                        await delay_exchange.publish(
                            message=retry_msg,
                            routing_key=target_routing_key
                        )
                        print(f"[>] Scheduled retry #{dispatch_data.retry_count} to {target_routing_key}.")
                        await message.ack()

                    else:
                        print(f"[X] Max retries ({MAX_RETRIES}) reached. Routing to DLQ.")
                        
                        # 1. Package the fatal message
                        # We inject a custom header so we know WHY it died when we inspect it later
                        fatal_msg = aio_pika.Message(
                            body=dispatch_data.model_dump_json().encode(),
                            content_type="application/json",
                            headers={"x-fatal-error": str(e)} 
                        )
                        
                        # 2. Publish to the Dead Letter Exchange
                        await dlx_exchange.publish(
                            message=fatal_msg,
                            routing_key="dlq.fatal"
                        )
                        
                        # 3. ACK the original message so it leaves the primary queue
                        await message.ack()

                        
                except Exception as e:
                    print(f"[X] Critical System Error: {e}")
                    await message.reject(requeue=False)

        print("Waiting for dispatch tasks. To exit press CTRL+C")
        await queue.consume(process_message)
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())

