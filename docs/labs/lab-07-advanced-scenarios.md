# Advanced Scenarios (Lab 07)

!!! info "Theory"
    Read [Advanced Patterns (07)](../theory/07-advanced-patterns.md) first.

**Goal:** Tune retry backoff, explore schema evolution, understand concurrency settings, and put it all together with an end-to-end event flow observation.

**Time:** ~30 minutes

---

## Prerequisites

- All previous labs complete
- Spring Boot app running

---

## Scenario A — Tuning Exponential Backoff

### Current Configuration

```java
ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
backOff.setInitialInterval(1000L);   // 1 second
backOff.setMultiplier(2.0);          // doubles each retry: 1s → 2s → 4s
```

### Experiment 1: Faster Retry (Dev Environment)

In `KafkaConfig.java`, temporarily change:

```java
backOff.setInitialInterval(200L);  // 200ms
backOff.setMultiplier(1.5);        // 200ms → 300ms → 450ms
```

Restart the app and trigger a TRANSIENT error:
```bash
curl -s -X POST \
  "http://localhost:8080/api/kafka/events/test?eventType=ORDER_FAILED&simulateError=true&errorType=TRANSIENT" | jq
```

Total DLT time should now be ~950ms instead of ~7 seconds.

### Experiment 2: More Retries

```java
ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(5); // 5 retries
```

Observe 6 total attempts (1 original + 5 retries) before DLT routing.

### ✅ Checkpoint A: Verify retry timing matches your configured values

Revert to original values (`3 retries, 1000ms, multiplier=2.0`) after the experiment.

---

## Scenario B — Schema Evolution

### Adding a Backward-Compatible Field

Edit `src/main/avro/Event.avsc` to add an optional `source` field:

```json title="src/main/avro/Event.avsc (add after errorType field)"
{
  "name": "source",
  "type": ["null", "string"],
  "default": null,
  "doc": "Origin service of this event, e.g. order-service"
}
```

Rebuild:
```bash
./mvnw clean compile
```

The Avro Maven plugin regenerates `AvroEvent.java` with the new field.

Restart the app and send an event:
```bash
curl -s -X POST \
  "http://localhost:8080/api/kafka/avro/events?eventType=ORDER_CREATED" | jq
```

Check Schema Registry — a **new version** should be registered:
```bash
# Should now show [1, 2]
curl -s http://localhost:8081/subjects/avro-events-topic-value/versions | jq
```

### ✅ Checkpoint B1: Two schema versions registered — old consumers can still read new messages

Old messages (written with schema v1) are still readable by the new consumer (schema v2 with default `source=null`). **BACKWARD compatible** ✅

### ✅ Checkpoint B2: Revert the schema change

Remove the `source` field from `Event.avsc` and rebuild before continuing. This would be a **breaking change** in production (removing a field), but since this is a fresh learning environment, it's fine to revert.

```bash
# After reverting, the schema registry still has v1 and v2
# The app will re-register v1 (which is still compatible since we're going backward)
./mvnw clean compile
./mvnw spring-boot:run
```

---

## Scenario C — Concurrency and Partition Assignment

### Current: Single Thread

The project uses default concurrency (1 thread per listener). One thread handles all 3 partitions sequentially within a poll loop.

### Enable Multi-Thread Consuming

In `KafkaConfig.java`, add concurrency to the JSON consumer factory:

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, Event> kafkaListenerContainerFactory(...) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, Event>();
    factory.setConsumerFactory(consumerFactory);
    factory.setConcurrency(3); // ← add this line: 1 thread per partition
    // ...
}
```

Restart the app. In Kafka-UI → Consumers → `manual-ack-group`, you'll see **3 individual members** instead of 1, each assigned one partition.

Send 9 events rapidly:
```bash
for i in {1..9}; do
  curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=CONCURRENT_$i" &
done
wait
```

### ✅ Checkpoint C: With concurrency=3, all 3 partitions consumed simultaneously

Revert `setConcurrency(3)` to avoid unexpected parallel behaviour in later labs.

---

## Scenario D — End-to-End Event Flow Monitoring

Set up a "mission control" view with everything open simultaneously.

### Open 4 terminal panes

```bash
# Pane 1: App logs (already running)

# Pane 2: Watch main topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events-topic \
  --from-beginning \
  --property print.partition=true \
  --property print.offset=true \
  --property print.key=true

# Pane 3: Watch DLT
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events-topic.DLT \
  --from-beginning

# Pane 4: Watch Avro topic (binary, not human-readable without Avro tools)
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic avro-events-topic \
  --from-beginning
```

### Run a full sequence

```bash
# Normal JSON event
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=FLOW_NORMAL" | jq

# Failing JSON event → DLT
curl -s -X POST \
  "http://localhost:8080/api/kafka/events/test?eventType=FLOW_FAIL&simulateError=true&errorType=PERMANENT" | jq

# Avro event
curl -s -X POST "http://localhost:8080/api/kafka/avro/events?eventType=FLOW_AVRO" | jq
```

### ✅ Checkpoint D: Observe each event's journey in the correct pane

| Event | Appears In |
|-------|-----------|
| `FLOW_NORMAL` | Main topic pane + App log (consumed, acknowledged) |
| `FLOW_FAIL` | Main topic pane (briefly) → DLT pane (after retries) |
| `FLOW_AVRO` | Avro topic pane (binary) |

---

## Scenario E — Produce with Kafka CLI Directly

Bypass the Spring Boot app and produce raw JSON to Kafka:

```bash
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic events-topic \
  --property "key.separator=:" \
  --property "parse.key=true"
```

Type:
```
cli-key-001:{"eventId":"cli-001","eventType":"CLI_EVENT","simulateError":false,"errorType":"NONE","timestamp":"2026-04-11T10:00:00","payload":"from CLI"}
```

Press `Ctrl+C`.

Watch the app logs — `ManualAckConsumer` will pick it up and try to deserialise. If the JSON is valid, it processes normally; if malformed, it goes to DLT.

### ✅ Checkpoint E: CLI-produced JSON event consumed by Spring Boot app

---

## Scenario F — Kafka-UI Produce & Consume

1. Open [http://localhost:8090](http://localhost:8090) → **Topics → events-topic**
2. Click **Produce Message**
3. Set:
   - **Key:** `ui-key-001`
   - **Value:** `{"eventId":"ui-001","eventType":"UI_EVENT","simulateError":false,"errorType":"NONE","timestamp":"2026-04-11T10:00:00","payload":"from Kafka-UI"}`
4. Click **Produce**
5. Watch the app logs consume it

### ✅ Checkpoint F: Kafka-UI produced message consumed by the Spring Boot app

---

## What You Learned

- ✅ Tuned exponential backoff parameters and observed the timing difference
- ✅ Added a backward-compatible Avro field and observed schema versioning in the registry
- ✅ Set `concurrency=3` and saw 3 consumer threads assigned to 3 partitions
- ✅ Ran an end-to-end flow with all 4 topics visible simultaneously
- ✅ Produced messages via CLI and Kafka-UI (bypassing the Spring Boot producer)

---

## 🎉 Labs Complete!

You've completed all 7 labs. Head to the [Interview Guide](../interview-guide/index.md) to prepare for interviews.

