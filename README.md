# Apache Kafka: Complete Tutorial & Roadmap

A comprehensive, hands-on tutorial and learning guide for **Apache Kafka**, covering foundational principles, core mechanics, ecosystem tooling, Python integration, distributed systems architecture, production operations, and hands-on projects.

---

## Quick Lab Environment Setup

This repository comes pre-configured with a complete, modern Kafka development environment running in Docker Compose with **KRaft mode (ZooKeeper-less)**, **Kafka Exporter**, **Prometheus**, and pre-provisioned **Grafana dashboards**.

### Architecture & Service Ports

```
+-------------------------------------------------------------------------------+
|                               Docker Host Network                             |
|                                                                               |
|   +-------------------+                   +-------------------------------+   |
|   |   Python Apps     |                   |       kafka-exporter          |   |
|   | (Producer/Consumer|                   |       (Port: 9308)            |   |
|   +---------+---------+                   +---------------+---------------+   |
|             |                                             |                   |
|   localhost:9092                               kafka:29092| (internal)        |
|             |                                             |                   |
|             v                                             v                   |
|   +---------------------------------------------------------------+           |
|   |                         Apache Kafka                          |           |
|   |               (KRaft Controller + Broker Node)                |           |
|   +---------------------------------------------------------------+           |
|                                                           ^                   |
|                                                           | Scrapes           |
|   +-------------------+  Data Source   +------------------+---------------+   |
|   |      Grafana      |<---------------+            Prometheus            |   |
|   |   (Port: 3000)    |                |           (Port: 9090)           |   |
|   +-------------------+                +----------------------------------+   |
+-------------------------------------------------------------------------------+
```

| Service | Port | Description | URL / Credentials |
| :--- | :--- | :--- | :--- |
| **Apache Kafka** | `9092` (host), `29092` (internal) | Kafka 4.1.0 broker + KRaft controller | `localhost:9092` |
| **Kafka Exporter** | `9308` | Scrapes topic offsets & consumer lag | [http://localhost:9308/metrics](http://localhost:9308/metrics) |
| **Prometheus** | `9090` | Time-series database scraping Kafka metrics | [http://localhost:9090](http://localhost:9090) |
| **Grafana** | `3000` | Pre-configured metrics visualization dashboard | [http://localhost:3000](http://localhost:3000) (`admin` / `admin`) |

### Starting the Environment

```bash
# 1. Start all containers in the background
docker compose up -d

# 2. Verify all 4 containers are running healthy
docker compose ps

# 3. View live Kafka logs
docker compose logs -f kafka
```

---

## Course Roadmap Overview

- [Part 1 — Foundations](#part-1--foundations)
- [Part 2 — Core Kafka Concepts](#part-2--core-kafka-concepts)
- [Part 3 — Kafka Ecosystem & Tooling](#part-3--kafka-ecosystem--tooling)
- [Part 4 — Python Development (`confluent-kafka`)](#part-4--python-development)
- [Part 5 — Advanced Kafka & Event-Driven Patterns](#part-5--advanced-kafka--event-driven-patterns)
- [Part 6 — Production & Operations](#part-6--production--operations)
- [Part 7 — Hands-on Progressive Projects (Levels 1–10)](#part-7--hands-on-progressive-projects)

---

## Part 1 — Foundations

### 1.1 What is Apache Kafka?
Apache Kafka is an open-source, distributed, fault-tolerant **event streaming platform**. Originally created at LinkedIn and later open-sourced under the Apache Software Foundation, Kafka is designed for high-throughput, low-latency ingestion and processing of real-time data feeds.

Unlike traditional databases that store static state, Kafka stores an append-only sequence of immutable events over time.

### 1.2 Why Kafka Exists
In traditional architectures, systems communicate through point-to-point connections:
- Web apps talk directly to databases, caches, analytics, and CRM systems.
- As systems grow, maintaining $N \times M$ connections becomes brittle, causing cascading failures and tight coupling.

Kafka acts as an enterprise **central nervous system**:
- Producers publish events once.
- Any number of downstream consumers subscribe to events at their own pace without impacting producers.

### 1.3 Kafka vs. Traditional Message Queues (RabbitMQ / ActiveMQ / SQS)

| Feature | Apache Kafka | Traditional MQ (RabbitMQ, SQS) |
| :--- | :--- | :--- |
| **Model** | Append-only distributed commit log (pull-based) | Transient message queue (push-based) |
| **Data Retention** | Persistent on disk for days/months/years | Deleted immediately upon consumer acknowledgement |
| **Replayability** | Yes, consumers can rewind offsets to re-read history | No, once consumed, messages are gone |
| **Throughput** | Millions of messages/sec via sequential disk I/O & zero-copy | Typically tens of thousands/sec |
| **Consumer Scaling** | Partition-level concurrency across consumer groups | Competing consumers on a single queue |
| **Ordering** | Guaranteed per-partition | FIFO queues available, but concurrency hurts ordering |

### 1.4 Real-World Use Cases
1. **Activity Tracking**: Recording page views, clicks, searches, and interactions in real time.
2. **Metrics & Logging**: Aggregating logs and performance metrics from thousands of microservices.
3. **Financial & Order Processing**: Capturing payments, fraud detection, and inventory updates.
4. **Change Data Capture (CDC)**: Streaming database mutations (`INSERT`, `UPDATE`, `DELETE`) to data lakes, search indexes (Elasticsearch), or caches (Redis).
5. **Stream Processing**: Powering real-time analytical pipelines with Flink, Spark, or Kafka Streams.

### 1.5 Kafka Architecture Components

```
                          KAFKA CLUSTER
+-----------------------------------------------------------------+
| TOPIC: "orders"                                                 |
|                                                                 |
| Partition 0: [0][1][2][3][4][5][6]...  (Leader: Broker 1)       |
| Partition 1: [0][1][2][3][4]...        (Leader: Broker 2)       |
| Partition 2: [0][1][2][3][4][5]...     (Leader: Broker 3)       |
+-----------------------------------------------------------------+
       ^                                                 |
       | Produces                                        | Consumes
+------+------+                                   +------+------+
|  Producers  |                                   |  Consumers  |
+-------------+                                   | (Group A)   |
                                                  +-------------+
```

- **Producers**: Client applications that publish (write) events into Kafka topics.
- **Consumers**: Applications that subscribe to topics and read events sequentially.
- **Topics**: A logical channel or category to which records are published (e.g., `orders`, `user-signups`).
- **Partitions**: Topics are broken down into partitions distributed across cluster brokers. Partitions enable horizontal parallelism and scalability.
- **Offsets**: Each message in a partition gets a monotonically increasing, immutable integer ID called an **offset**. Offsets define the exact message position.
- **Brokers**: Kafka servers that receive, persist, replicate, and serve messages.
- **Kafka Clusters**: A group of cooperating brokers sharing metadata and partitions.
- **Replication**: Partitions are replicated across multiple brokers for high availability (`replication.factor >= 3` in production).
- **Leaders & Followers**:
  - Each partition has exactly one **Leader** broker handling all reads and writes.
  - One or more **Followers** replicate data from the leader. If the leader fails, an In-Sync Replica (ISR) follower is automatically elected as the new leader.
- **Consumer Groups**: A collection of consumers cooperating to consume a topic. Kafka assigns each partition to exactly **one consumer** within the group:
  - If you have 3 partitions and 3 consumers in the same group, each consumer reads 1 partition.
  - If you have 4 consumers for 3 partitions, the 4th consumer remains idle as a hot standby.
- **Messages & Serialization**: Every record contains:
  - `key` (optional, bytes): Used for partition routing.
  - `value` (payload, bytes): JSON, Avro, Protobuf, or raw string.
  - `timestamp`: Event creation or broker append time.
  - `headers` (key-value metadata): Tracing IDs, tenant IDs, schema versions.

---

## Part 2 — Core Kafka Concepts

### 2.1 Producer Acknowledgements (`acks`)
Controls how many replicas must commit a message before the broker acknowledges success:
- `acks=0`: Producer sends without waiting for any response. Maximum throughput, high risk of data loss.
- `acks=1`: Leader writes to local log and acknowledges immediately. Safe against client crashes, but data can be lost if leader fails before followers replicate.
- `acks=all` (or `-1`): Leader waits for all In-Sync Replicas (`min.insync.replicas`) to commit. **Strongest durability guarantee**.

### 2.2 Producer Batching & Compression
Producers optimize throughput by batching records in memory:
- `linger.ms`: Maximum time to buffer records before sending (e.g., `5ms`–`20ms`).
- `batch.size`: Maximum memory size per batch in bytes (e.g., `32KB` or `64KB`).
- `compression.type`: Compress batches across network and disk:
  - `snappy` (balanced CPU & compression ratio, default recommendation)
  - `lz4` (fastest compression)
  - `zstd` (highest compression ratio)
  - `gzip` (higher CPU overhead)

### 2.3 Message Keys & Partitioning Strategy
- **With Key**: Kafka computes `hash(key) % total_partitions`. All messages with the same key (e.g., `customer_id=501`) are guaranteed to land on the **same partition**, preserving strict FIFO ordering for that entity.
- **Without Key** (`key=null`): Uses sticky partitioning (batches to a single partition until full, then rotates) for maximum batching efficiency.
- **Custom Partitioner**: Custom logic routing records based on business requirements (e.g., VIP tenant partition).

### 2.4 Consumer Offsets & Rebalancing
- Consumers commit their read progress to an internal Kafka topic named `__consumer_offsets`.
- **Auto-commit** (`enable.auto.commit=true`): Periodically commits the last read offset every `auto.commit.interval.ms`. Easy, but risks message loss or duplicates on consumer crash.
- **Manual commit**: Application explicitly commits offsets via `commit()` synchronously or asynchronously after processing.
- **Rebalancing**: Triggered when a consumer joins, leaves, or crashes, reallocating partitions among available group members:
  - *Eager Rebalance*: All consumers drop assignments and wait for full reallocation.
  - *Cooperative Sticky Rebalance*: Only partitions moving to another consumer are revoked; unaffected consumers keep processing.

### 2.5 Consumer Lag
Consumer lag represents the delta between the latest offset produced in a partition and the current offset committed by the consumer group:

$$\text{Lag} = \text{Log End Offset} - \text{Consumer Current Offset}$$

- **Lag = 0**: Consumer is real-time.
- **Lag > 0 and increasing**: Consumer is falling behind (slow processing, bottleneck, or unhandled errors). Monitored via Kafka Exporter & Prometheus.

### 2.6 Delivery Semantics
1. **At-most-once**: Offsets are committed *before* processing records. If the consumer crashes during processing, messages are lost.
2. **At-least-once**: Offsets are committed *after* records are successfully processed. If consumer crashes mid-process, uncommitted records are re-processed upon restart (duplicates possible; consumers should be idempotent).
3. **Exactly-once (EOS)**: Uses idempotent producers and transactional coordinators (`send_offsets_to_transaction`) to ensure end-to-end atomic processing without duplicates.

### 2.7 Retention & Log Compaction
- **Time Retention** (`log.retention.hours=168`): Deletes segments older than 7 days.
- **Size Retention** (`log.retention.bytes`): Deletes oldest segments when partition exceeds disk quota.
- **Log Compaction**: Instead of deleting by age, Kafka retains the **latest value for each key**. If an update arrives for key `user_10`, older versions are pruned during segment cleaning. A `null` value (tombstone) deletes the key permanently.

---

## Part 3 — Kafka Ecosystem & Tooling

### 3.1 Kafka CLI Tools Reference

Execute commands inside the running `kafka` container:

```bash
# 1. Create a Topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic orders --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# 2. List Topics
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:9092

# 3. Describe Topic Details (Partitions, Leader, Replicas, ISR)
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --describe --topic orders --bootstrap-server localhost:9092

# 4. Console Producer (Interactive terminal input)
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic orders --bootstrap-server localhost:9092 \
  --property "parse.key=true" --property "key.separator=:"

# 5. Console Consumer (Read from beginning)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders --from-beginning --bootstrap-server localhost:9092 \
  --property print.key=true --property print.timestamp=true

# 6. List Consumer Groups
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list

# 7. Inspect Consumer Group Lag & Offsets
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group order-processing-group

# 8. Reset Consumer Group Offsets to Earliest
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-processing-group --topic orders --reset-offsets --to-earliest --execute
```

> **Tip:** You can also run the ready-made helper scripts in the repository:
> - `./scripts/create-topic.sh orders`
> - `./scripts/produce.sh orders`
> - `./scripts/consume.sh orders`

### 3.2 Kafka Connect & Streams
- **Kafka Connect**: A scalable framework to reliably stream data between Kafka and external systems without writing code.
  - *Source Connectors*: Import data from databases (Postgres, MySQL via Debezium), S3, or Salesforce into Kafka.
  - *Sink Connectors*: Export data from Kafka into Elasticsearch, Snowflake, BigQuery, or MongoDB.
- **Kafka Streams**: A lightweight client library for Java/Kotlin that builds event-driven stream processing applications (filter, map, aggregate, join, windowed analytics) directly on top of Kafka without needing external clusters like Spark.

### 3.3 Schema Registry (Avro, JSON Schema, Protobuf)
In production, contracts between producers and consumers must not break:
- **Schema Registry**: A centralized service serving as the source of truth for schemas.
- Producers serialize data against a schema version; consumers download the schema to deserialize.
- Enforces compatibility modes (backward, forward, full) to prevent incompatible message formats from polluting topics.

### 3.4 Monitoring Kafka
The stack included in this repository provides full observability:
- **Kafka Exporter** (`:9308`): Queries Kafka broker APIs for cluster topology, topic offsets, and consumer group lag.
- **Prometheus** (`:9090`): Scrapes metrics every 5 seconds.
- **Grafana** (`:3000`): Auto-loaded **Kafka Overview** dashboard showing real-time msg/sec throughput, partition distribution, and consumer group health.

---

## Part 4 — Python Development

We use `confluent-kafka`, the official high-performance Python client built on top of the native C library (`librdkafka`).

### 4.1 Project Setup with Python & `uv`

```bash
# Install dependencies using uv or pip
uv sync
# OR
pip install confluent-kafka
```

### 4.2 Producer Implementation (`producer/producer.py`)

A production-ready producer handling JSON serialization, delivery callbacks, and partition keys:

```python
import json
from confluent_kafka import Producer

# Configuration
conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "order-service",
    "acks": "all",  # Wait for all replicas
    "enable.idempotence": True,  # Prevent duplicate sends
    "retries": 5,  # Retry transient network errors
    "linger.ms": 10,  # Batch messages for up to 10ms
    "compression.type": "snappy",  # Compress batch payloads
}

producer = Producer(conf)


def delivery_callback(err, msg):
    if err:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(
            f"✅ Delivered to {msg.topic()} "
            f"[partition {msg.partition()}] at offset {msg.offset()}"
        )


order = {
    "order_id": 1001,
    "customer_id": 501,
    "product": "Laptop",
    "amount": 85000,
    "status": "CREATED",
}

# Produce using order_id as key to preserve ordering per order
producer.produce(
    topic="orders",
    key=str(order["order_id"]),
    value=json.dumps(order),
    on_delivery=delivery_callback,
)

# Wait for all outstanding buffered messages to be delivered
producer.flush()
```

### 4.3 Consumer Implementation (`consumer/consumer.py`)

A robust consumer loop handling manual offset commits, graceful shutdowns, and error inspection:

```python
import json
import signal
import sys
from confluent_kafka import Consumer, KafkaError

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-processing-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # Manual commit for at-least-once safety
}

consumer = Consumer(conf)
consumer.subscribe(["orders"])

running = True


def shutdown(sig, frame):
    global running
    print("\nShutting down consumer cleanly...")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

print("🚀 Consumer running. Waiting for messages (Ctrl+C to exit)...")
try:
    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"❌ Consumer error: {msg.error()}")
            break

        # Process message
        order = json.loads(msg.value().decode("utf-8"))
        print(
            f"📦 Processing order #{order['order_id']} for customer {order['customer_id']}"
        )

        # Commit offset after successful business processing
        consumer.commit(message=msg, asynchronous=False)

finally:
    consumer.close()
    print("🔒 Consumer connection closed.")
```

### 4.4 Framework Integrations: FastAPI + Kafka

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from confluent_kafka import Producer
import json

producer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    yield
    producer.flush()


app = FastAPI(lifespan=lifespan)


@app.post("/orders")
async def create_order(order: dict):
    producer.produce("orders", key=str(order.get("order_id")), value=json.dumps(order))
    producer.poll(0)  # Serve delivery callbacks non-blocking
    return {"status": "Order submitted"}
```

---

## Part 5 — Advanced Kafka & Event-Driven Patterns

### 5.1 Idempotent & Transactional Processing
- **Idempotent Producer** (`enable.idempotence=true`): Prevents duplicate messages produced by retries upon network disconnects. Each producer receives a unique Producer ID (PID) and sequence numbers for each message.
- **Transactions (`read_committed`)**: Enables atomic writes across multiple topics and partitions (e.g. read an event from `raw-orders`, write transformed data to `processed-orders`, and commit consumer offsets in a single atomic transaction).

### 5.2 Hot Partitions & Partition Balancing
- If keys are skewed (e.g. 80% of orders belong to a single vendor or merchant ID), one partition receives disproportionate traffic, overwhelming the consumer assigned to it.
- **Solutions**:
  - Compound Keys: Combine key with a salt or secondary field (e.g., `vendorId_orderId`).
  - Round-Robin Partitioning when strict per-entity ordering is not strictly required.

### 5.3 Dead-Letter Queue (DLQ) & Retry Architecture

```
                       +-------------------+
                       |   Main Topic      |
                       |    ("orders")     |
                       +---------+---------+
                                 |
                                 v
                       +-------------------+
                       |    Consumer       |
                       +----+---------+----+
           Success          |         | Transient Error
              v             |         v
     [Commit Offset] <------+   +-------------------+
                                |    Retry Topic    | (with backoff delay)
                                | ("orders-retry")  |
                                +---------+---------+
                                          |
                                          | Max retries exceeded
                                          v
                                +-------------------+
                                | Dead-Letter Topic |
                                |   ("orders-dlq")  |
                                +-------------------+
```

1. **Main Topic**: Receives inbound requests.
2. **Retry Topic**: Transient failures (e.g., database timeout) are pushed to a retry topic with exponential backoff delay.
3. **Dead-Letter Topic (DLQ)**: Poison pills or permanent failures (e.g., malformed JSON) are sent to a DLQ for manual inspection and alerts without blocking other messages.

### 5.4 Microservice Patterns
- **Transactional Outbox Pattern**: Prevent dual-write bugs between Postgres and Kafka. The application writes business data AND an outbox record in a single database transaction. A CDC tool (like Debezium) reads the database write-ahead log (WAL) and publishes events to Kafka.
- **Event Sourcing**: State changes are saved as a sequence of events. Current state is reconstructed by replaying events from offset 0.
- **CQRS (Command Query Responsibility Segregation)**: Separate write models (producing domain events) from read-optimized views (consuming and indexing events into Elasticsearch or Read-Replicas).

---

## Part 6 — Production & Operations

### 6.1 Multi-Broker Cluster & KRaft Consensus
Apache Kafka 3.x+ introduced **KRaft (Kafka Raft Metadata Mode)**, fully deprecating Apache ZooKeeper:
- Eliminates external ZooKeeper dependencies.
- Scales clusters to millions of partitions with fast controller failover (<100ms).
- In production, deploy an odd number of KRaft controllers (e.g., 3 or 5) for cluster quorum consensus.

### 6.2 Security Setup (Authentication & Authorization)
- **Encryption in Transit**: Mutual TLS (mTLS / SSL) encrypts broker-to-broker and client-to-broker traffic.
- **Authentication**:
  - **SASL/SCRAM-SHA-512**: Salted challenge-response username/password.
  - **SASL/OAUTHBEARER**: Modern identity providers (Okta, Keycloak, Azure AD).
- **Authorization (ACLs)**: Fine-grained Access Control Lists restricting which clients can `READ` or `WRITE` to specific topics or consumer groups.

### 6.3 Capacity Planning & Performance Tuning
- **Zero-Copy Optimization**: Kafka uses the OS `sendfile()` system call to transfer bytes directly from OS PageCache to network socket without copying data into JVM user space.
- **Disk**: Sequential write speed matters more than random IOPS. Multiple striped NVMe SSDs or RAID 10.
- **JVM Heap**: Keep Kafka broker heap small (`4GB`–`8GB`) because Kafka relies heavily on the Linux OS PageCache for caching messages, rather than JVM memory.
- **OS Tuning**:
  ```bash
  # Increase open file descriptors & map count
  vm.max_map_count = 262144
  fs.file-max = 1000000
  ```

---

## Part 7 — Hands-on Progressive Projects

Follow these 10 hands-on project milestones to master Kafka:

- [x] **Level 1: Basic Producer → Consumer** *(Implemented)*
  - Setup single broker KRaft in Docker.
  - Send messages with `producer.py` and inspect them with `consumer.py`.
- [x] **Level 2: Multiple Consumers + Consumer Groups** *(Implemented)*
  - Create a topic with 3 partitions using `scripts/create-topic.sh orders 3`.
  - Launch multiple consumer processes under `order-processing-group` and observe partition assignment.
- [x] **Level 3: Order Processing System** *(Implemented)*
  - Structured event envelopes with `event_id`, `correlation_id`, `occurred_at`.
  - Delivery callbacks, PostgreSQL-backed idempotency table (`processed_events`), and graceful shutdown signals.
- [x] **Level 4: FastAPI + Kafka Event System** *(Implemented)*
  - REST API endpoint (`order_service`) with asynchronous non-blocking message publishing (`poll(0)`) and clean lifespan `flush()`.
- [x] **Level 5: Microservices Event-Driven Architecture** *(Implemented)*
  - Full event choreography with 5 decoupled services: `order_service`, `inventory_service`, `payment_service` (with retry & DLT topics), `order_processor`, and `notification_service`.
- [ ] **Level 6: Kafka + PostgreSQL + Outbox Pattern**
  - Eliminate distributed two-phase commit issues using an `outbox` database table and a publisher worker.
- [ ] **Level 7: Change Data Capture (CDC) with Debezium**
  - Stream Postgres WAL row-level changes automatically to Kafka topics.
- [ ] **Level 8: Kafka Streams / Real-Time Analytics**
  - Calculate real-time sliding window aggregates (e.g., top-selling products in the last 15 minutes).
- [ ] **Level 9: Production 3-Broker KRaft Cluster**
  - Build a 3-broker cluster with `min.insync.replicas=2` and simulate node failure / leader election.
- [ ] **Level 10: Complete Real-World Event-Driven Platform**
  - Combine Schema Registry, authentication, Prometheus alerting, dead-letter topics, and end-to-end tracing.

---

## Practical Exercises: Verify Your Environment

### 1. Provision All Topics (3 Partitions Each)
Run the automated topic provisioning script:
```bash
./scripts/create-all-topics.sh
```
Or create an individual topic:
```bash
./scripts/create-topic.sh orders 3
```

### 2. Level 1 & 2: Basic Producer, Consumer & Consumer Groups
Open two or more terminal tabs to observe partition scaling within `order-processing-group`:
- **Terminal 1 (Consumer A)**:
  ```bash
  CONSUMER_ID=consumer-1 uv run python consumer/consumer.py
  ```
- **Terminal 2 (Consumer B)**:
  ```bash
  CONSUMER_ID=consumer-2 uv run python consumer/consumer.py
  ```
- **Terminal 3 (Producer)**:
  ```bash
  uv run python producer/producer.py
  ```

---

## Level 5: Running the Microservices Event Choreography

The repository includes a complete event-driven microservices architecture communicating over Kafka topics with database-backed idempotency and retry queues:

```
                  +--------------------------------+
                  |  Order Service (FastAPI :8000) |
                  +---------------+----------------+
                                  | produces "order.created"
                                  v
                         ["order-events"]
                           |           |
            +--------------+           +--------------+
            | consumes                                | consumes
            v                                         v
+-----------------------+                 +-----------------------+
|   Inventory Service   |                 |    Payment Service    |
| (Reserves Inventory)  |                 | (Processes & Retries) |
+-----------+-----------+                 +-----------+-----------+
            | produces "inventory.reserved"           | produces "payment.completed"
            v                                         v
  ["inventory-events"]                       ["payment-events"]
            \                                         /
             +-------------------+-------------------+
                                 | consumes both
                                 v
                     +-----------------------+
                     |    Order Processor    |
                     | (Coordinates & Sagas) |
                     +-----------+-----------+
                                 | produces "order.completed"
                                 v
                         ["order-events"]
                                 | consumes
                                 v
                     +-----------------------+
                     | Notification Service  |
                     |  (Sends Confirmation) |
                     +-----------------------+
```

### Step-by-Step Microservices Startup

Open separate terminal tabs for each service:

1. **Start Kafka and Postgres** (if not already running):
   ```bash
   docker compose up -d
   ./scripts/create-all-topics.sh
   ```

2. **Start Order Processor Coordinator**:
   ```bash
   uv run python -m order_processor.app.main
   ```

3. **Start Inventory Service**:
   ```bash
   uv run python -m inventory_service.app.main
   ```

4. **Start Payment Service**:
   ```bash
   uv run python -m payment_service.app.main
   ```

5. **Start Notification Service**:
   ```bash
   uv run python -m notification_service.app.main
   ```

6. **Start Order API Service**:
   ```bash
   uv run uvicorn order_service.app.main:app --port 8000 --reload
   ```

7. **Submit a Test Order**:
   ```bash
   curl -X POST http://localhost:8000/orders \
     -H "Content-Type: application/json" \
     -d '{
       "customer_id": "cust_42",
       "product_id": "laptop_99",
       "quantity": 1,
       "amount": 1299.99
     }'
   ```

Watch the terminal logs across all services:
- **`order_service`**: Writes order to Postgres and publishes `order.created` with the `order_id` as correlation ID and partition key.
- **`inventory_service`**: Receives `order.created`, reserves stock, and publishes `inventory.reserved`.
- **`payment_service`**: Receives `order.created`, verifies idempotency in Postgres, charges the customer, marks event processed, and publishes `payment.completed`.
- **`order_processor`**: Aggregates both completion signals and publishes `order.completed`.
- **`notification_service`**: Consumes `order.completed` and emits the order confirmation notification!

### Inspect Live Metrics
- Open **Grafana**: [http://localhost:3000](http://localhost:3000) (User: `admin`, Pass: `admin`)
- Open **Prometheus**: [http://localhost:9090](http://localhost:9090)
- Navigate to **Dashboards** → **Kafka Overview** to see your produced messages, partitions, and consumer group lag updated in real time!