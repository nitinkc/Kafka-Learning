# Lab 02 · First Producer & Consumer

!!! info "Theory"
    Read [02 · Spring Kafka Setup](../theory/02-spring-kafka-setup.md) and [03 · Producers & Consumers](../theory/03-producers-and-consumers.md) first.

**Goal:** Start the Spring Boot app, produce JSON events via REST, and observe them consumed with partition and offset metadata in the logs.

**Time:** ~20 minutes

---

## Prerequisites

- Lab 01 complete — Docker containers running
- `events-topic` (3 partitions) exists

---

## Step 1 — Start the Spring Boot App

```bash
cd /path/to/Kafka-Learning
./mvnw spring-boot:run
```

Watch the startup logs. You should see:

```
INFO  KafkaConfig - Topics registered: events-topic, events-topic.DLT, avro-events-topic
INFO  Started KafkaOffsetDemoApplication in X.XXX seconds
```

!!! failure "Common error: Kafka connection refused"
    If you see `Connection refused` or `TimeoutException`, Kafka isn't ready yet.
    Run: `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list`
    and retry `./mvnw spring-boot:run` once it responds.

---

## Step 2 — Send Your First JSON Event

Open a new terminal:

```bash
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_CREATED" | jq
```

Expected response:
```json
{
  "status": "ACCEPTED",
  "eventId": "3f7a1c2d-...",
  "eventType": "ORDER_CREATED",
  "topic": "events-topic"
}
```

### ✅ Checkpoint 1: Consumer log shows partition + offset

In the app terminal, look for:
```
INFO  ManualAckConsumer - Received - Partition: 1, Offset: 0, EventId: 3f7a1c2d-...
INFO  EventProcessingService - Processing event: 3f7a1c2d-... - Type: ORDER_CREATED
INFO  ManualAckConsumer - Acknowledged offset: 0
```

Note which partition the message landed on.

---

## Step 3 — Send Multiple Events and Observe Distribution

Send 6 events in a loop:

```bash
for i in {1..6}; do
  curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_CREATED_$i" \
    | jq .eventId
done
```

Watch the app logs — events should distribute across **Partition 0, 1, and 2** (since each eventId is a random UUID, the Murmur2 hash distributes them).

### ✅ Checkpoint 2: Events spread across partitions

You should see `Partition: 0`, `Partition: 1`, and `Partition: 2` across the 6 messages.

---

## Step 4 — Send a Full Event Body

```bash
curl -s -X POST http://localhost:8080/api/kafka/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "manual-001",
    "eventType": "PAYMENT_PROCESSED",
    "payload": "Order #42 payment $99.99",
    "timestamp": "2026-04-11T10:00:00",
    "simulateError": false,
    "errorType": "NONE"
  }' | jq
```

### ✅ Checkpoint 3: Specific eventId always lands on same partition

Note the partition for `manual-001`. Send the same event again:

```bash
curl -s -X POST http://localhost:8080/api/kafka/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "manual-001",
    "eventType": "PAYMENT_PROCESSED_V2",
    "payload": "Same key, different payload",
    "timestamp": "2026-04-11T10:00:00",
    "simulateError": false,
    "errorType": "NONE"
  }' | jq
```

The **same partition** appears in the log — same key `manual-001` → same partition (key-based partitioning).

---

## Step 5 — Inspect in Kafka-UI

1. Open [http://localhost:8090](http://localhost:8090)
2. **Topics → events-topic → Messages tab**
3. Click **Load More** to see all messages
4. Notice each message has:
   - **Partition** (0, 1, or 2)
   - **Offset** (incrementing within each partition)
   - **Key** (the eventId UUID)
   - **Value** (the JSON body)

### ✅ Checkpoint 4: Messages visible in Kafka-UI with metadata

---

## Step 6 — Check Consumer Group Offsets

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group manual-ack-group
```

Expected output (approximately):
```
GROUP            TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
manual-ack-group events-topic   0          3               3               0
manual-ack-group events-topic   1          2               2               0
manual-ack-group events-topic   2          2               2               0
```

**LAG = 0** means all messages are consumed and acknowledged. The committed offsets match the log end offsets.

---

## Step 7 — Observe Offset Commit in Action

Stop the app (`Ctrl+C`), send a new event:

```bash
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=WHILE_APP_DOWN" | jq
```

This fails (app is down). Now restart the app:

```bash
./mvnw spring-boot:run
```

!!! question "Will this event be consumed?"
    With `auto.offset.reset=latest`, the consumer starts from the newest message **at the time the consumer group first connects**. Since the event was sent while the app was down:
    - If the consumer group existed before → it resumes from the last committed offset → **the new message IS consumed** ✅
    - If this were a brand-new group → it would miss messages sent before first connection

Check the app logs — the `WHILE_APP_DOWN` event should be processed on startup.

---

## Step 8 — Verify the REST Endpoints

```bash
# Test all event types
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_UPDATED" | jq
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_CANCELLED" | jq
```

---

## What You Learned

- ✅ Started the Spring Boot Kafka application
- ✅ Produced JSON events via REST and observed them consumed
- ✅ Verified key-based partitioning (same key → same partition)
- ✅ Confirmed LAG=0 after all messages are acknowledged
- ✅ Understood how `auto.offset.reset=latest` affects new vs. existing consumer groups

**Next:** [Lab 03 · Consumer Groups](lab-03-consumer-groups.md)

