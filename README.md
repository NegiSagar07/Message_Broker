Asynchronous Webhook Dispatcher

A distributed, production-grade event-driven webhook architecture designed to guarantee high-reliability data delivery to third-party servers.

Overview

Modern systems need to push data to external clients (webhooks) reliably. When a client's server goes down or becomes unresponsive, a standard synchronous HTTP request blocks the main application and drops the data.

This project solves that problem by decoupling the reception of events from their delivery. It utilizes an asynchronous API Gateway to instantly accept events, and a resilient background worker system powered by RabbitMQ. If a target server fails, the system automatically routes the payload through a Time-To-Live (TTL) exponential backoff loop (1 minute, 5 minutes) before gracefully falling back to a Dead Letter Queue (DLQ).

It is intended for backend engineers, system architects, and developers looking to implement Stripe-like or Swiggy-like reliable webhook infrastructure.

Features

Core Processing

Asynchronous Event Ingestion: Sub-millisecond API response times by offloading processing to a message broker.

Decoupled Architecture: Separate Router and Dispatch workers prevent processing bottlenecks.

Reliability & Fault Tolerance

Exponential Backoff: Automated retry mechanisms via RabbitMQ TTL exchanges (1m and 5m delays).

Dead Letter Queue (DLQ): Terminal failures are safely stored for manual audit after maximum retries are reached.

Connection Resilience: Workers utilize robust connection protocols to survive broker restarts.

Security

Cryptographic Payload Signing: Outbound webhooks are stamped with an HMAC-SHA256 signature using a tenant-specific secret key.

Strict Data Contracts: Pydantic models enforce payload structure across the entire pipeline.

Tech Stack

Backend: Python 3.11, FastAPI

Database: PostgreSQL (Asyncio / SQLAlchemy Core)

Message Broker / Task Queue: RabbitMQ (aio-pika)

HTTP Client: httpx (Asynchronous requests)

Containerization: Docker, Docker Compose

Data Validation: Pydantic v2

Architecture

The system operates on a zero-trust, decoupled flow inside a private container network:

API Gateway: Receives the incoming event via HTTP POST, validates the structure, and instantly drops it into the router_queue.

Message Broker (RabbitMQ): Orchestrates queues, delay exchanges, and dead letter routing.

Router Worker: Consumes from router_queue. Queries the PostgreSQL database for the target tenant's endpoint URL and HMAC secret key. Upgrades the message and publishes it to the dispatch_bus.

Dispatch Worker: The heavy lifter. Calculates the HMAC-SHA256 signature, attaches it to the headers, and fires the HTTP request. If the request fails (e.g., 500 Server Error), it calculates the retry count and routes the message to the appropriate TTL delay exchange.

Database (PostgreSQL): Stores relational configurations (Tenants, Endpoints, Secret Keys).

Project Structure

webhook_dispatcher_project/
├── api_gateway/
│   └── main.py              # FastAPI application and ingress routes
├── shared/
│   ├── database.py          # SQLAlchemy async engine and session configuration
│   ├── models.py            # PostgreSQL ORM models (Tenant, Endpoint)
│   ├── schemas.py           # Pydantic data contracts (EventMessage, DispatchMessage)
│   └── security.py          # Cryptography engine for HMAC-SHA256 signatures
├── workers/
│   ├── router_worker.py     # Matches events to DB endpoints and upgrades payload
│   └── dispatch_worker.py   # Executes HTTP requests, signing, and retry/DLQ logic
├── docker-compose.yml       # Multi-container orchestration
├── Dockerfile               # Single unified Docker image blueprint
├── requirements.txt         # Python dependencies
├── .env                     # Local environment variables (Not committed)
└── .env.example             # Template for environment variables


Installation

Follow these steps to set up the project locally using Docker.

1. Clone the repository

git clone https://github.com/yourusername/webhook-dispatcher.git
cd webhook-dispatcher


2. Configure Environment Variables
Copy the template file to create your local .env.

cp .env.example .env


3. Build and Start the Cluster
Ensure Docker is installed and running, then boot the multi-container system in detached mode:

docker compose up --build -d


Note: The containers are configured with depends_on health checks. The Python workers will automatically wait for PostgreSQL and RabbitMQ to be fully booted before starting.

Environment Variables

Variable

Description

Required

POSTGRES_USER

Database username

Yes

POSTGRES_PASSWORD

Database password

Yes

POSTGRES_DB

Target database name

Yes

DATABASE_URL

Asyncio connection string for SQLAlchemy

Yes

RABBITMQ_DEFAULT_USER

RabbitMQ username

Yes

RABBITMQ_DEFAULT_PASS

RabbitMQ password

Yes

RABBITMQ_URL

AMQP connection string for workers

Yes

Running the Project

Once the cluster is running, you can monitor the flow of messages in real-time.

1. View Worker Logs
Open a terminal and tail the logs of both decoupled workers:

docker compose logs -f router_worker dispatch_worker


2. Trigger a Test Event
Open a separate terminal and send a POST request to the API Gateway to trigger the webhook flow.

To simulate a Success (200 OK):

curl -X POST http://localhost:8000/events \
     -H "Content-Type: application/json" \
     -d '{
           "tenant_id": "ten_123",
           "event_type": "payment.success",
           "payload": {"order_id": "888", "status": "completed"}
         }'


To simulate a Failure and Retry Loop (Target server offline):

curl -X POST http://localhost:8000/events \
     -H "Content-Type: application/json" \
     -d '{
           "tenant_id": "ten_fail",
           "event_type": "payment.failed",
           "payload": {"order_id": "999", "status": "corrupted"}
         }'


3. Monitor the RabbitMQ Dashboard
Open your browser and navigate to http://localhost:15672 (using the credentials defined in your .env file). Check the "Queues" tab to watch messages shift between the active dispatch queue, the delay queues, and the dead-letter queue.

API Documentation

Method

Endpoint

Description

POST

/events

Ingests a new event payload. Validates and pushes to the routing broker.

Example Response for POST /events:

{
  "status": "accepted",
  "event_id": "evt_7f1c84b652",
  "message": "Webhook queued for processing."
}


Database Schema

Tenant: Represents a customer or system entity. Contains the tenant_id and the hmac_secret_key used for cryptographically signing their webhooks.

Endpoint: Represents a webhook destination. Belongs to a Tenant. Contains the target_url and an array of subscribed_events.

Authentication Flow

Egress (Dispatch): Implements a Zero-Trust approach. Every outgoing webhook includes an X-Webhook-Signature header. The signature is an HMAC-SHA256 hash of the raw JSON payload and the Tenant's secret key, proving the data was not tampered with in transit.

Error Handling

API Level: Pydantic enforces strict schema validation. Invalid payloads are rejected with HTTP 422 immediately.

Network Level: The Dispatch worker traps httpx.HTTPStatusError and httpx.RequestError (timeouts, dropped connections).

Retry Logic:

Attempt 1 fails -> Routed to delay.1m (60s hold).

Attempt 2 fails -> Routed to delay.5m (300s hold).

Attempt 3 fails -> Routed to dlq.fatal (Dead Letter Queue).

Security Features

Cryptographic Outbound Payload Signing (HMAC-SHA256)

Input Validation and Sanitization via Pydantic

Environment Variable isolation (no hardcoded secrets)

Isolated Docker bridge networking (Internal ports not exposed to host unnecessarily)

Future Improvements

Database Migrations: Integrate Alembic to manage database schema changes programmatically instead of relying on startup hooks.

Admin Dashboard: Build a frontend interface to manually replay messages sitting in the Dead Letter Queue.

Dynamic TTL: Allow tenants to configure their own custom retry limits and backoff intervals.


Contributing

Fork the Project

Create your Feature Branch (git checkout -b feature/AmazingFeature)

Commit your Changes (git commit -m 'Add some AmazingFeature')

Push to the Branch (git push origin feature/AmazingFeature)

Open a Pull Request

Please ensure your code passes standard linting and doesn't break existing broker typologies.

