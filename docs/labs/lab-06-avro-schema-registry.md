# Avro & Schema Registry (Lab 06)

!!! info "Theory"
    Read [Avro & Schema Registry (06)](../theory/06-avro-and-schema-registry.md) first.

**Goal:** Send Avro-encoded events, verify schema auto-registration, inspect the Schema Registry API, and trigger a schema validation failure.

**Time:** ~30 minutes

---

## Prerequisites

- Lab 01 complete — Schema Registry running at [http://localhost:8081](http://localhost:8081)
- Spring Boot app running

---

## Step 1 — Verify Schema Registry is Empty

```bash
curl -s http://localhost:8081/subjects | jq
```

Expected: `[]` (no schemas yet — unless you've sent Avro events before)

---

## Step 2 — Send Your First Avro Event

```bash
curl -s -X POST \
  "http://localhost:8080/api/kafka/avro/events?eventType=ORDER_CREATED" | jq
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

App logs:
```
[AVRO] Sending event id=... type=ORDER_CREATED
[AVRO] Event accepted by Schema Registry and sent to topic 'avro-events-topic'
[AVRO] Received – Partition=0, Offset=0, EventId=..., EventType=ORDER_CREATED
[AVRO] Payload='null', SimulateError=false, ErrorType=NONE
[AVRO] Acknowledged offset=0
```

### ✅ Checkpoint 1: Avro event produced and consumed successfully

---

## Step 3 — Verify Schema Was Registered

```bash
# List all registered subjects (topics register as <topic>-value)
curl -s http://localhost:8081/subjects | jq
```

Expected: `["avro-events-topic-value"]`

```bash
# Get the registered schema
curl -s http://localhost:8081/subjects/avro-events-topic-value/versions/latest \
  | jq '.schema | fromjson'
```

You should see the full Avro schema JSON — exactly matching `src/main/avro/Event.avsc`.

### ✅ Checkpoint 2: Schema registered under subject `avro-events-topic-value`

---

## Step 4 — Explore Schema Registry API

```bash
# Schema ID and version number
curl -s http://localhost:8081/subjects/avro-events-topic-value/versions/latest | jq '{id, version}'

# List all schema versions (should be [1] so far)
curl -s http://localhost:8081/subjects/avro-events-topic-value/versions | jq

# Global compatibility level (default: BACKWARD)
curl -s http://localhost:8081/config | jq
```

---

## Step 5 — Send Avro Events with All Parameters

```bash
# With payload
curl -s -X POST \
  "http://localhost:8080/api/kafka/avro/events?eventType=PAYMENT_PROCESSED&payload=order-123-txn-456" | jq

# With all parameters
curl -s -X POST \
  "http://localhost:8080/api/kafka/avro/events?eventType=INVENTORY_UPDATED&payload=sku-789&simulateError=false&errorType=NONE" | jq
```

---

## Step 6 — Avro Error Simulation

Test error handling in the Avro consumer:

```bash
# Transient error — retried 3x then to DLT
curl -s -X POST \
  "http://localhost:8080/api/kafka/avro/events?eventType=ORDER_FAILED&simulateError=true&errorType=TRANSIENT" | jq
```

Wait ~7 seconds and observe:

- App logs: `[AVRO]` retry attempts
- DLT consumer logs: DLT message received

!!! note "Avro DLT"
    Currently, the Avro consumer uses the same DLT (`events-topic.DLT`) configured in `KafkaConfig.errorHandler()`. In production, you'd typically have a separate `avro-events-topic.DLT`.

---

## Step 7 — Inspect Avro Messages in Kafka-UI

1. Open [http://localhost:8090](http://localhost:8090)
2. **Topics → avro-events-topic → Messages**
3. Kafka-UI automatically deserialises Avro binary using Schema Registry → messages appear as **readable JSON**!
4. **Schema Registry tab** (if available in your Kafka-UI version) → `avro-events-topic-value`

### ✅ Checkpoint 3: Avro messages appear as JSON in Kafka-UI (not binary gibberish)

---

## Step 8 — Schema Compatibility Experiment

Let's understand BACKWARD compatibility by checking what changes are safe.

### Safe Change: Add an Optional Field

The current schema has these fields: `eventId`, `eventType`, `payload`, `timestamp`, `simulateError`, `errorType`.

A new optional field with a default is **BACKWARD compatible** — old consumers can ignore it:

```bash
# Verify current compatibility level
curl -s http://localhost:8081/config | jq '.compatibilityLevel'
# Expected: "BACKWARD"
```

Test if a proposed schema is compatible:
```bash
curl -s -X POST \
  http://localhost:8081/compatibility/subjects/avro-events-topic-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"AvroEvent\",\"namespace\":\"com.demo.kafka.avro\",\"fields\":[{\"name\":\"eventId\",\"type\":\"string\"},{\"name\":\"eventType\",\"type\":\"string\"},{\"name\":\"payload\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"timestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"simulateError\",\"type\":\"boolean\",\"default\":false},{\"name\":\"errorType\",\"type\":{\"type\":\"enum\",\"name\":\"ErrorType\",\"namespace\":\"com.demo.kafka.avro\",\"symbols\":[\"NONE\",\"TRANSIENT\",\"PERMANENT\",\"DESERIALIZATION\",\"VALIDATION\"]},\"default\":\"NONE\"},{\"name\":\"source\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
  }' | jq
```

Expected: `{"is_compatible": true}` — adding an optional `source` field is safe ✅

### Breaking Change: Remove a Required Field

```bash
# Schema missing the required 'eventType' field
curl -s -X POST \
  http://localhost:8081/compatibility/subjects/avro-events-topic-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"AvroEvent\",\"namespace\":\"com.demo.kafka.avro\",\"fields\":[{\"name\":\"eventId\",\"type\":\"string\"}]}"
  }' | jq
```

Expected: `{"is_compatible": false}` — removing a field breaks consumers ❌

### ✅ Checkpoint 4: Compatibility check returns true/false correctly

---

## Step 9 — The Confluent Wire Format

Every Avro message in Kafka has this 5-byte prefix:

```
[0x00][schema_id byte 3][schema_id byte 2][schema_id byte 1][schema_id byte 0][avro bytes...]
```

View the raw bytes:
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic avro-events-topic \
  --from-beginning \
  --max-messages 1 | xxd | head -5
```

The first byte is always `00` (magic byte), followed by 4 bytes of schema ID (e.g., `00 00 00 01` = schema ID 1).

---

## Step 10 — Schema Mismatch Scenario

!!! warning "This is a simulation — we won't actually break the running app"

If you were to change `Event.avsc` to rename `eventId` to `id` and rebuild:

```bash
# This would fail at serialization time:
# SerializationException: Error registering Avro schema: Schema being registered is incompatible
# with an earlier schema for subject "avro-events-topic-value"
```

The message **never reaches the broker** — schema validation at the producer prevents corrupt data from entering Kafka. This is a key advantage of Avro + Schema Registry over plain JSON.

---

## What You Learned

- ✅ Produced and consumed Avro events with automatic schema registration
- ✅ Inspected the registered schema via Schema Registry REST API
- ✅ Verified Kafka-UI renders Avro binary as human-readable JSON
- ✅ Tested schema compatibility — safe changes vs. breaking changes
- ✅ Understood the Confluent 5-byte wire format prefix
- ✅ Confirmed schema validation errors prevent corrupt data from entering Kafka

**Next:** [Advanced Scenarios (Lab 07)](lab-07-advanced-scenarios.md)

