# Kafka Offset Demo — Startup Guide

A Spring Boot application demonstrating Kafka offset management, manual acknowledgment, dead-letter topics (DLT), exponential-backoff retry handling, and **Avro schema validation via Confluent Schema Registry**.

---

## Prerequisites

| Tool           | Version  | Notes                                |
|:---------------|:---------|:-------------------------------------|
| Java           | 24       | Set `JAVA_HOME` accordingly          |
| Maven          | 3.9+     | Or use the included `./mvnw` wrapper |
| Docker         | 24+      | Docker Desktop recommended on macOS  |
| Docker Compose | v2+      | Bundled with Docker Desktop          |

---

## Architecture Overview

### JSON Flow (existing)

```
REST Client
    │
    ▼
KafkaController  ──►  EventProducer  ──►  [events-topic]  (3 partitions)
                                                │
                                                ▼
                                       ManualAckConsumer
                                       (manual-ack-group)
                                                │
                                     ┌──────────┴──────────┐
                                     │  Success             │  Error (after 3 retries)
                                     │  ack.acknowledge()   │  DeadLetterPublishingRecoverer
                                     └──────────────────────┴──► [events-topic.DLT] (1 partition)
                                                                        │
                                                                        ▼
                                                               DeadLetterConsumer
                                                               (dlt-consumer-group)
                                                               – logs & deletes record
```

### Avro + Schema Registry Flow

```
REST Client
    │
    ▼  POST /api/kafka/avro/events
KafkaController  ──►  AvroEventProducer
                              │
                              │  KafkaAvroSerializer
                              ▼
                  ┌─────────────────────────┐
                  │  Confluent Schema        │
                  │  Registry :8081          │◄── registers schema on first send
                  │                          │    validates payload on every send
                  └──────────┬──────────────┘
                             │ ✅ schema valid → binary Avro bytes
                             │ ❌ schema invalid → SerializationException
                             │    (message NEVER reaches broker)
                             ▼
                    [avro-events-topic]  (3 partitions)
                             │
                             ▼
                   AvroEventConsumer
                   (avro-consumer-group)
                   KafkaAvroDeserializer
                   fetches schema from registry
                   → validates bytes → typed AvroEvent
```

### Kafka Topics

| Topic               | Partitions | Replicas | Consumer Group        | Purpose                            |
|:--------------------|:-----------|:---------|:----------------------|:-----------------------------------|
| `events-topic`      | 3          | 1        | `manual-ack-group`    | Main JSON event stream             |
| `events-topic.DLT`  | 1          | 1        | `dlt-consumer-group`  | Dead-letter / failed events        |
| `avro-events-topic` | 3          | 1        | `avro-consumer-group` | Avro schema-validated event stream |

> **Note:** `KafkaConfig` registers both topics as Spring beans (`NewTopic`). Spring Boot auto-creates them on startup — but only **after** Kafka is reachable. This is why the app fails if Kafka isn't ready first.

---

## Step-by-Step Startup

### Step 1 — Start Infrastructure (Zookeeper + Kafka + Schema Registry + Kafka-UI)

```bash
cd /path/to/Kafka-Learning

# Start all containers in the background
docker compose up -d
```

Expected containers:

| Container         | Port  | Role                                    |
|:------------------|:------|:----------------------------------------|
| `zookeeper`       | 2181  | Coordination                            |
| `kafka`           | 9092  | Broker (host access) / 29092 (internal) |
| `schema-registry` | 8081  | Confluent Schema Registry               |
| `kafka-ui`        | 8090  | Web UI → http://localhost:8090          |

### Step 2 — Wait for Kafka to be Healthy

Kafka takes ~15–30 seconds to fully start after Docker reports it as running. Verify it is ready before starting the app:

```bash
# Watch broker logs until you see "started (kafka.server.KafkaServer)"
docker logs -f kafka 2>&1 | grep -m1 "started (kafka.server.KafkaServer)"
```

You should see a line like:
```
[KafkaServer id=1] started (kafka.server.KafkaServer)
```
Press `Ctrl+C` once it appears.

**Alternative — quick health-check using kafka-topics:**
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```
If the command returns (even with an empty list) without error, Kafka is ready.

**Verify Schema Registry is up:**
```bash
curl -s http://localhost:8081/subjects
# Expected: [] (empty list on first run)
```

### Step 3 — (Optional but Recommended) Pre-create Kafka Topics

Although Spring Boot will auto-create the topics via `KafkaConfig`, you can create them manually to guarantee they exist before the app connects. This prevents startup failures on slow machines.

```bash
# Main topic — 3 partitions, replication factor 1
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic events-topic \
  --partitions 3 \
  --replication-factor 1

# Dead-letter topic — 1 partition, replication factor 1
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic events-topic.DLT \
  --partitions 1 \
  --replication-factor 1

# Avro topic — 3 partitions, replication factor 1
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic avro-events-topic \
  --partitions 3 \
  --replication-factor 1
```

Confirm all topics exist:
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Expected output:
```
avro-events-topic
events-topic
events-topic.DLT
```

### Step 4 — Start the Spring Boot Application

```bash
# Using Maven wrapper (no Maven installation needed)
./mvnw spring-boot:run

# OR with plain Maven
mvn spring-boot:run

# OR build a JAR first, then run it
./mvnw clean package -DskipTests
java -jar target/kafka-offset-demo-0.0.1-SNAPSHOT.jar
```

The application starts on **port 8080** by default.

Look for this in the logs to confirm successful startup:
```
Started KafkaOffsetDemoApplication in X.XXX seconds
```

### Step 5 — Verify Topics & Consumer Groups in Kafka-UI

Open **http://localhost:8090** in your browser.

- **Topics** tab → confirm `events-topic` (3 partitions), `events-topic.DLT` (1 partition), and `avro-events-topic` (3 partitions) are listed.
- **Consumer Groups** tab → confirm `manual-ack-group`, `dlt-consumer-group`, and `avro-consumer-group` are registered.
- **Schema Registry** tab → after sending your first Avro event, `avro-events-topic-value` will appear here with the full schema.

---


## Testing the Application

### Send a Normal Event

```bash
curl -s -X POST http://localhost:8080/api/kafka/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "test-001",
    "eventType": "ORDER_CREATED",
    "payload": "Test order payload",
    "timestamp": "2026-04-06T10:00:00",
    "simulateError": false,
    "errorType": "NONE"
  }' | jq
```

### Send a Quick Test Event (no body needed)

```bash
# Normal event
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_CREATED" | jq

# Trigger a transient error (retried 3x with exponential backoff, then sent to DLT)
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_FAILED&simulateError=true&errorType=TRANSIENT" | jq

# Trigger a permanent error (sent directly to DLT)
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_FAILED&simulateError=true&errorType=PERMANENT" | jq

# Trigger a validation error
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_INVALID&simulateError=true&errorType=VALIDATION" | jq
```

### Error Types

| `errorType`  | Behaviour                    | Retries                                 | Ends up in DLT? |
|:-------------|:-----------------------------|:----------------------------------------|:----------------|
| `NONE`       | Normal processing            | —                                       | No              |
| `TRANSIENT`  | Throws `TransientException`  | 3x (exponential backoff: 1s → 2s → 4s)  | Yes             |
| `PERMANENT`  | Throws `RuntimeException`    | 3x                                      | Yes             |
| `VALIDATION` | Throws `ValidationException` | 3x                                      | Yes             |

---

## Avro Schema Validation — Testing

The Avro flow uses **Confluent Schema Registry** to enforce the `Event.avsc` schema at the producer side. A `KafkaAvroSerializer` validates every message _before_ it reaches the broker.

### How it works

1. The first `POST /api/kafka/avro/events` call **registers** the schema with Schema Registry.
2. Every subsequent call **validates** the `AvroEvent` object against the registered schema.
3. If validation fails → `SerializationException` is thrown → message is **rejected** and never written to Kafka.
4. On the consumer side, `KafkaAvroDeserializer` fetches the writer schema from the registry and validates/converts the binary bytes back to a typed `AvroEvent`.

### Send a valid Avro event

```bash
# Minimal — only required fields
curl -s -X POST "http://localhost:8080/api/kafka/avro/events?eventType=ORDER_CREATED" | jq
```

Expected response:
```json
{
  "status": "ACCEPTED",
  "eventId": "3f7a1c2d-...",
  "eventType": "ORDER_CREATED",
  "topic": "avro-events-topic",
  "schemaValidation": "PASSED – KafkaAvroSerializer validated against Schema Registry"
}
```

```bash
# With optional payload
curl -s -X POST \
  "http://localhost:8080/api/kafka/avro/events?eventType=ORDER_CREATED&payload=order-123" | jq

# With all parameters
curl -s -X POST \
  "http://localhost:8080/api/kafka/avro/events?eventType=PAYMENT_PROCESSED&payload=txn-456&simulateError=false&errorType=NONE" | jq
```

### Send Avro events for all error types

```bash
# Transient error (retried 3x with backoff, then to DLT)
curl -s -X POST \
  "http://localhost:8080/api/kafka/avro/events?eventType=ORDER_FAILED&simulateError=true&errorType=TRANSIENT" | jq

# Permanent error (goes straight to DLT after retries)
curl -s -X POST \
  "http://localhost:8080/api/kafka/avro/events?eventType=ORDER_FAILED&simulateError=true&errorType=PERMANENT" | jq

# Validation error
curl -s -X POST \
  "http://localhost:8080/api/kafka/avro/events?eventType=ORDER_INVALID&simulateError=true&errorType=VALIDATION" | jq
```

### Inspect the registered schema in Schema Registry

```bash
# List all registered subjects (each topic gets a subject named <topic>-value)
curl -s http://localhost:8081/subjects | jq

# Fetch the latest schema for the Avro topic
curl -s http://localhost:8081/subjects/avro-events-topic-value/versions/latest | jq

# See just the schema JSON
curl -s http://localhost:8081/subjects/avro-events-topic-value/versions/latest \
  | jq '.schema | fromjson'
```

### Check schema compatibility

```bash
# List all versions of the schema (grows with every incompatible change)
curl -s http://localhost:8081/subjects/avro-events-topic-value/versions | jq

# Check the global compatibility level (default: BACKWARD)
curl -s http://localhost:8081/config | jq
```

### Avro event parameters

| Parameter       | Type     | Required | Default  | Description                                                               |
|:----------------|:---------|:---------|:---------|:--------------------------------------------------------------------------|
| `eventType`     | String   | ✅ Yes    | —        | Event category, e.g. `ORDER_CREATED`                                      |
| `payload`       | String   | No       | `null`   | Optional business payload string                                          |
| `simulateError` | Boolean  | No       | `false`  | Set `true` to trigger error simulation                                    |
| `errorType`     | Enum     | No       | `NONE`   | `NONE` \| `TRANSIENT` \| `PERMANENT` \| `DESERIALIZATION` \| `VALIDATION` |

### Watch Avro messages in Kafka-UI

1. Open **http://localhost:8090** → **Topics** → `avro-events-topic` → **Messages** tab.
2. Send an Avro event via `curl`.
3. Kafka-UI decodes the Avro binary using the Schema Registry and displays the message as readable JSON.
4. Go to **Schema Registry** tab → `avro-events-topic-value` to view and browse schema versions.

---

## Shutdown

```bash
# Stop the Spring Boot app
Ctrl+C   # in the terminal running spring-boot:run

# Stop Docker containers (keeps volumes/data)
docker compose stop

# Stop AND remove containers + networks (clean slate)
docker compose down

# Full clean — also removes volumes (wipes all Kafka data)
docker compose down -v
```

---

## Troubleshooting

### App fails to start — "Error creating bean" / Kafka connection refused

Kafka wasn't ready when the app started.

**Fix:**
1. Run `docker compose up -d` and wait 30 seconds.
2. Verify with: `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list`
3. Then start the app.

### Topics not found / consumer group lag is wrong

Topics may not have been created yet.

**Fix:** Run the manual topic creation commands in **Step 3** above, then restart the app.

### `PLAINTEXT_HOST` / advertised listener issues

The broker advertises `localhost:9092` for host access and `kafka:29092` for inter-container communication. Ensure nothing else is bound to port **9092** on your machine.

```bash
lsof -i :9092
```

### Schema Registry connection refused / Avro send fails

The `AvroEventProducer` contacts Schema Registry on every `send()`. If it is not running you will see:
```
SerializationException: Error registering Avro schema
```

**Fix:**
1. Confirm Schema Registry is up: `curl -s http://localhost:8081/subjects`
2. If the container is not running: `docker compose up -d schema-registry`
3. Check its logs: `docker logs schema-registry`

### Port 8090 already in use (Kafka-UI)

Change the host port in `docker-compose.yml`:
```yaml
ports:
  - "8091:8080"   # change 8090 → any free port
```

### Check running containers

```bash
docker compose ps
```

---


## Documentation

**View locally:**

Create and activate a virtual environment, then install dependencies:
```shell
python3 -m venv .venv && echo 'Created venv'
source .venv/bin/activate
```

Build and serve the documentation:
```bash
pip install -r requirements.txt
mkdocs build
mkdocs serve
# Open http://localhost:8000
```