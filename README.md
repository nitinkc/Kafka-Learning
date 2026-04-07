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

## Kafka-UI — Full Guide

Your `docker-compose.yml` already bundles **[Kafka-UI](https://github.com/provectuslabs/kafka-ui)** (by Provectus). No extra setup needed — it starts automatically with `docker compose up -d`.

### Access

```
http://localhost:8090
```

The UI connects to the broker at `kafka:29092` (internal Docker network) and is pre-configured via `docker-compose.yml`:

```yaml
KAFKA_CLUSTERS_0_NAME: local
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
```

---

### What You Can Do in Kafka-UI

#### 📋 Topics

| Action                                    | How                                            |
|:------------------------------------------|:-----------------------------------------------|
| List all topics                           | **Topics** menu in left sidebar                |
| See partition count, replication, offsets | Click on any topic name                        |
| Browse messages live                      | Topic → **Messages** tab → click **Load More** |
| Filter messages by offset, timestamp, key | Messages tab → use the filter bar at the top   |
| Produce a test message manually           | Topic → **Produce Message** button             |
| Delete a topic                            | Topic → **Delete Topic** button (top right)    |

#### 👥 Consumer Groups

| Action                   | How                                                 |
|:-------------------------|:----------------------------------------------------|
| List all consumer groups | **Consumers** menu in left sidebar                  |
| See lag per partition    | Click on a consumer group (e.g. `manual-ack-group`) |
| Reset consumer offsets   | Consumer group → **Reset Offsets** button           |

#### 🔎 Brokers

| Action                   | How                 |
|:-------------------------|:--------------------|
| See broker info & config | **Brokers** menu    |
| Check cluster health     | Dashboard home page |

---

### Useful Kafka-UI Workflows for This Project

**1. Watch a message flow end-to-end**
1. Open `events-topic` → **Messages** tab (keep it open)
2. In a terminal, send a test event:
   ```bash
   curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_CREATED" | jq
   ```
3. Refresh Messages — you should see the new record appear with its partition and offset.

**2. Watch a failed message land in the DLT**
1. Open `events-topic.DLT` → **Messages** tab (keep it open)
2. Send an error event:
   ```bash
   curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_FAILED&simulateError=true&errorType=PERMANENT" | jq
   ```
3. After ~3 retries, refresh Messages on the DLT — the failed record will appear with DLT headers (`dlt-original-topic`, `dlt-exception-message`).

**3. Check consumer lag**
1. Go to **Consumers** → `manual-ack-group`
2. After sending events, lag should return to **0** once all messages are successfully acknowledged.

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

## Project Structure

```
src/main/
├── avro/
│   └── Event.avsc                        # Avro schema definition (source of truth)
│                                         # → generates AvroEvent.java + ErrorType.java
└── java/com/demo/kafka/
    ├── KafkaOffsetDemoApplication.java   # Spring Boot entry point
    ├── config/
    │   └── KafkaConfig.java              # JSON + Avro producer/consumer factories,
    │                                     # topic definitions, error handler (DLT + retry)
    ├── controller/
    │   └── KafkaController.java          # REST endpoints:
    │                                     #   POST /api/kafka/events        (JSON)
    │                                     #   POST /api/kafka/events/test   (JSON)
    │                                     #   POST /api/kafka/avro/events   (Avro)
    ├── producer/
    │   ├── EventProducer.java            # Publishes JSON events to events-topic
    │   └── AvroEventProducer.java        # Publishes Avro events to avro-events-topic
    │                                     # (KafkaAvroSerializer validates schema on send)
    ├── consumer/
    │   ├── ManualAckConsumer.java        # Consumes events-topic (manual-ack-group)
    │   ├── AvroEventConsumer.java        # Consumes avro-events-topic (avro-consumer-group)
    │   │                                 # (KafkaAvroDeserializer validates schema on receive)
    │   └── DeadLetterConsumer.java       # Consumes events-topic.DLT (dlt-consumer-group)
    ├── service/
    │   └── EventProcessingService.java   # Business logic + error simulation
    ├── handler/
    │   └── CustomErrorHandler.java       # TransientException / ValidationException types
    └── model/
        ├── Event.java                    # JSON event model
        └── DeadLetterEvent.java          # DLT event model

# Generated at build time (do not edit):
target/generated-sources/avro/com/demo/kafka/avro/
    ├── AvroEvent.java                    # Generated from Event.avsc
    └── ErrorType.java                    # Generated from Event.avsc (inline enum)
```

---

## Key Configuration Properties

Add/override these in `src/main/resources/application.properties`:

```properties
spring.application.name=kafka-offset-demo

# Kafka broker
spring.kafka.bootstrap-servers=localhost:9092

# Topic names (JSON flow)
kafka.topics.main=events-topic
kafka.topics.dlt=events-topic.DLT

# Topic name (Avro flow)
kafka.topics.avro=avro-events-topic

# Confluent Schema Registry — used by KafkaAvroSerializer/Deserializer
kafka.schema-registry.url=http://localhost:8081
```

> If any of `kafka.topics.main`, `kafka.topics.dlt`, `kafka.topics.avro`, or `kafka.schema-registry.url` are missing from `application.properties`, the app will fail with `IllegalArgumentException: Could not resolve placeholder`.

---

## Consumer Acknowledgement

### Is Acknowledgement Necessary from the Consumer?

**It depends on the `AckMode` configured.** In this project, acknowledgement is **required and must be called manually**.

### Configuration

Both the JSON and Avro consumer factories are set to:

```java
factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
```

And auto-commit is disabled:

```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
```

This means Kafka will **NOT** commit the offset automatically — the consumer code **must** call `ack.acknowledge()` explicitly, which `ManualAckConsumer` does:

```java
processingService.processEvent(event);
ack.acknowledge();  // ✅ Required — commits the offset immediately
```

### What Happens If You Don't Call `ack.acknowledge()`?

| Scenario                                    | Result                                                                      |
|:--------------------------------------------|:----------------------------------------------------------------------------|
| Processing succeeds, no `ack.acknowledge()` | Offset is **NOT committed** → message will be **re-consumed** after restart |
| App crashes before `ack`                    | Message is **reprocessed** (at-least-once delivery)                         |
| `ack.acknowledge()` called                  | Offset is committed → message is marked as consumed                         |

### AckMode Options (Reference)

| AckMode            | Description                                                                    |
|:-------------------|:-------------------------------------------------------------------------------|
| `AUTO` (default)   | Spring auto-commits after the listener returns                                 |
| `MANUAL`           | You call `ack.acknowledge()`; committed at next poll interval                  |
| `MANUAL_IMMEDIATE` | ✅ **Used in this project** — offset committed immediately when `ack` is called |
| `RECORD`           | Auto-commits after each record is processed                                    |
| `BATCH`            | Auto-commits after all records in a poll batch are processed                   |

> **Summary:** Skipping `ack.acknowledge()` means offsets are never committed, and every consumer restart will re-read already-processed messages — causing **duplicate processing**.

