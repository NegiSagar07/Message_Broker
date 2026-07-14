<div align="center">
  <img src="docs/logo.png" alt="Logo" width="120" />

  <h1>Asynchronous Webhook Dispatcher</h1>

  <p>
    A distributed, production-grade event-driven webhook architecture designed to guarantee high-reliability data delivery to third-party servers.
  </p>

  <p>
    <img src="https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white" alt="Python 3.11" />
    <img src="https://img.shields.io/badge/FastAPI-Framework-009688?logo=fastapi&logoColor=white" alt="FastAPI" />
    <img src="https://img.shields.io/badge/RabbitMQ-Message%20Broker-FF6600?logo=rabbitmq&logoColor=white" alt="RabbitMQ" />
    <img src="https://img.shields.io/badge/PostgreSQL-Database-336791?logo=postgresql&logoColor=white" alt="PostgreSQL" />
    <img src="https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker&logoColor=white" alt="Docker" />
    <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="License" />
  </p>
</div>

---

## 📌 Overview

> Modern systems need to push data to external clients (webhooks) reliably. When a client's server goes down or becomes unresponsive, a standard synchronous HTTP request blocks the main application and risks data loss.
>
> This project solves that problem by decoupling the reception of events from their delivery. It utilizes an asynchronous API Gateway to instantly accept events, and a resilient background worker system to process and dispatch those events independently.
>
> It is intended for backend engineers, system architects, and developers looking to implement Stripe-like or Swiggy-like reliable webhook infrastructure.

## 📚 Table of Contents

- [✨ Features](#-features)
  - [Core Processing](#core-processing)
  - [Reliability & Fault Tolerance](#reliability--fault-tolerance)
  - [Security](#security)
- [🧰 Tech Stack](#-tech-stack)
- [🏗️ Architecture](#️-architecture)
- [📁 Project Structure](#-project-structure)
- [⚙️ Installation](#️-installation)
- [🔐 Environment Variables](#-environment-variables)
- [🚀 Running the Project](#-running-the-project)
- [📘 API Documentation](#-api-documentation)
- [🗄️ Database Schema](#️-database-schema)
- [🔏 Authentication Flow](#-authentication-flow)
- [🛡️ Error Handling](#️-error-handling)
- [🔒 Security Features](#-security-features)
- [🛣️ Future Improvements](#️-future-improvements)
- [🤝 Contributing](#-contributing)

## ✨ Features

### Core Processing

- **Asynchronous Event Ingestion:** Sub-millisecond API response times by offloading processing to a message broker.
- **Decoupled Architecture:** Separate Router and Dispatch workers prevent processing bottlenecks.

### Reliability & Fault Tolerance

- **Exponential Backoff:** Automated retry mechanisms via RabbitMQ TTL exchanges (1m and 5m delays).
- **Dead Letter Queue (DLQ):** Terminal failures are safely stored for manual audit after maximum retries are reached.
- **Connection Resilience:** Workers utilize robust connection protocols to survive broker restarts.

### Security

- **Cryptographic Payload Signing:** Outbound webhooks are stamped with an HMAC-SHA256 signature using a tenant-specific secret key.
- **Strict Data Contracts:** Pydantic models enforce payload structure across the entire pipeline.

## 🧰 Tech Stack

- 🐍 **Backend:** `Python 3.11`, `FastAPI`
- 🗄️ **Database:** `PostgreSQL`, `Asyncio`, `SQLAlchemy Core`
- 📨 **Message Broker / Task Queue:** `RabbitMQ`, `aio-pika`
- 🌐 **HTTP Client:** `httpx` (Asynchronous requests)
- 🐳 **Containerization:** `Docker`, `Docker Compose`
- ✅ **Data Validation:** `Pydantic v2`

## 🏗️ Architecture

The system operates on a zero-trust, decoupled flow inside a private container network:

1. **API Gateway:** Receives the incoming event via HTTP POST, validates the structure, and instantly drops it into the `router_queue`.
2. **Message Broker (RabbitMQ):** Orchestrates queues, delay exchanges, and dead letter routing.
3. **Router Worker:** Consumes from `router_queue`. Queries the PostgreSQL database for the target tenant's endpoint URL and HMAC secret key. Upgrades the message and publishes it to the `dispatch_bus`.
4. **Dispatch Worker:** The heavy lifter. Calculates the HMAC-SHA256 signature, attaches it to the headers, and fires the HTTP request. If the request fails (e.g., 500 Server Error), it calculates the retry strategy and pushes the event to delayed queues.
5. **Database (PostgreSQL):** Stores relational configurations (Tenants, Endpoints, Secret Keys).

## 📁 Project Structure

```text
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
```

## ⚙️ Installation

Follow these steps to set up the project locally using Docker.

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/webhook-dispatcher.git
cd webhook-dispatcher
```

### 2. Configure Environment Variables

Copy the template file to create your local `.env`.

```bash
cp .env.example .env
```

### 3. Build and Start the Cluster

Ensure Docker is installed and running, then boot the multi-container system in detached mode:

```bash
docker compose up --build -d
```

> **Note:** The containers are configured with `depends_on` health checks. The Python workers will automatically wait for PostgreSQL and RabbitMQ to be fully booted before starting.

## 🔐 Environment Variables

| Variable                 | Description                                | Required |
|--------------------------|--------------------------------------------|----------|
| `POSTGRES_USER`          | Database username                          | Yes      |
| `POSTGRES_PASSWORD`      | Database password                          | Yes      |
| `POSTGRES_DB`            | Target database name                       | Yes      |
| `DATABASE_URL`           | Asyncio connection string for SQLAlchemy   | Yes      |
| `RABBITMQ_DEFAULT_USER`  | RabbitMQ username                          | Yes      |
| `RABBITMQ_DEFAULT_PASS`  | RabbitMQ password                          | Yes      |
| `RABBITMQ_URL`           | AMQP connection string for workers         | Yes      |

## 🚀 Running the Project

Once the cluster is running, you can monitor the flow of messages in real-time.

### 1. View Worker Logs

Open a terminal and tail the logs of both decoupled workers:

```bash
docker compose logs -f router_worker dispatch_worker
```

### 2. Trigger a Test Event

Open a separate terminal and send a POST request to the API Gateway to trigger the webhook flow.

To simulate a Success (200 OK):

```bash
curl -X POST http://localhost:8000/events \
     -H "Content-Type: application/json" \
     -d '{
           "tenant_id": "ten_123",
           "event_type": "payment.success",
           "payload": {"order_id": "888", "status": "completed"}
         }'
```

To simulate a Failure and Retry Loop (Target server offline):

```bash
curl -X POST http://localhost:8000/events \
     -H "Content-Type: application/json" \
     -d '{
           "tenant_id": "ten_fail",
           "event_type": "payment.failed",
           "payload": {"order_id": "999", "status": "corrupted"}
         }'
```

### 3. Monitor the RabbitMQ Dashboard

Open your browser and navigate to `http://localhost:15672` (using the credentials defined in your `.env` file). Check the **Queues** tab to watch messages shift between the active dispatch queue, the delayed retry queues, and the final dead letter queue.

## 📘 API Documentation

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/events` | Ingests a new event payload. Validates and pushes to the routing broker. |

**Example Response for `POST /events`:**

```json
{
  "status": "accepted",
  "event_id": "evt_7f1c84b652",
  "message": "Webhook queued for processing."
}
```

## 🗄️ Database Schema

- **Tenant:** Represents a customer or system entity. Contains the `tenant_id` and the `hmac_secret_key` used for cryptographically signing their webhooks.
- **Endpoint:** Represents a webhook destination. Belongs to a Tenant. Contains the `target_url` and an array of `subscribed_events`.

## 🔏 Authentication Flow

- **Egress (Dispatch):** Implements a Zero-Trust approach. Every outgoing webhook includes an `X-Webhook-Signature` header. The signature is an HMAC-SHA256 hash of the raw JSON payload and the Tenant's secret key.

## 🛡️ Error Handling

- **API Level:** Pydantic enforces strict schema validation. Invalid payloads are rejected with HTTP 422 immediately.
- **Network Level:** The Dispatch worker traps `httpx.HTTPStatusError` and `httpx.RequestError` (timeouts, dropped connections).

### Retry Logic

- Attempt 1 fails -> Routed to `delay.1m` (60s hold).
- Attempt 2 fails -> Routed to `delay.5m` (300s hold).
- Attempt 3 fails -> Routed to `dlq.fatal` (Dead Letter Queue).

## 🔒 Security Features

- Cryptographic Outbound Payload Signing (HMAC-SHA256)
- Input Validation and Sanitization via Pydantic
- Environment Variable isolation (no hardcoded secrets)
- Isolated Docker bridge networking (Internal ports not exposed to host unnecessarily)

## 🛣️ Future Improvements

- **Database Migrations:** Integrate Alembic to manage database schema changes programmatically instead of relying on startup hooks.
- **Admin Dashboard:** Build a frontend interface to manually replay messages sitting in the Dead Letter Queue.
- **Dynamic TTL:** Allow tenants to configure their own custom retry limits and backoff intervals.

## 🤝 Contributing

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

> Please ensure your code passes standard linting and doesn't break existing broker typologies.
