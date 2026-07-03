# workers/dispatch_worker.py
import asyncio
import aio_pika
import httpx

from shared.schemas import DispatchMessage

RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
MAX_RETRIES = 3
BASE_DELAY_SEC = 5

async def main():
    print("Starting Advanced Dispatch Worker (Exponential Backoff enabled)...")
    
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=10)
        
        # We need BOTH the primary queue (to pull from) and the delay exchange (to send failures to)
        queue = await channel.get_queue("primary_dispatch_queue")
        delay_exchange = await channel.get_exchange("delay_bus")

        async def process_message(message: aio_pika.IncomingMessage):
            async with message.process(ignore_processed=True):
                try:
                    dispatch_data = DispatchMessage.model_validate_json(message.body.decode())
                    print(f"\n[*] Dispatching: {dispatch_data.dispatch_id} | Attempt: {dispatch_data.retry_count + 1}")
                    
                    async with httpx.AsyncClient() as client:
                        response = await client.post(
                            dispatch_data.target_url,
                            json=dispatch_data.payload,
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
                    if dispatch_data.retry_count < MAX_RETRIES:
                        dispatch_data.retry_count += 1
                        
                        # 2. Calculate Exponential Backoff (5s -> 10s -> 20s)
                        delay_ms = int((BASE_DELAY_SEC * (2 ** (dispatch_data.retry_count - 1))) * 1000)
                        print(f" -> Requeueing to Delay Bus. RabbitMQ will hold for {delay_ms/1000} seconds...")
                        
                        # 3. Clone the message and attach the Time Machine header
                        retry_msg = aio_pika.Message(
                            body=dispatch_data.model_dump_json().encode(),
                            content_type="application/json",
                            headers={"x-delay": delay_ms} # The magic RabbitMQ plugin header
                        )
                        
                        # 4. Publish to the delay exchange, routing it back to this exact worker pipeline
                        await delay_exchange.publish(
                            message=retry_msg,
                            routing_key="dispatch.execute"
                        )
                        
                        # 5. We successfully scheduled the retry, so we ACK the *original* message to clear it
                        await message.ack()
                    else:
                        print(f"[X] Max retries ({MAX_RETRIES}) reached. Webhook permanently dropped.")
                        await message.ack() # In a massive enterprise app, we'd send this to a database table to alert the user.
                        
                except Exception as e:
                    print(f"[X] Critical System Error: {e}")
                    await message.reject(requeue=False)

        print("Waiting for dispatch tasks. To exit press CTRL+C")
        await queue.consume(process_message)
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())