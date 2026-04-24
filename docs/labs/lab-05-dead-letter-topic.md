# Dead Letter Topic Deep Dive (Lab 05)

!!! info "Theory"
    Read [Error Handling & Dead Letter Topics (05)](../theory/05-error-handling-and-dlt.md) first.

**Goal:** Deeply explore the DLT consumer pattern — observe DLT headers, AdminClient record deletion, and understand how the DLT prevents stuck partitions.

**Time:** ~20 minutes

---

## Prerequisites

- Lab 04 complete — familiar with error simulation
- Spring Boot app running

---

## Step 1 — Watch the DLT in Real Time

Open two terminals side by side.

**Terminal 1 — Watch app logs:**
```bash
# Already running, or start with:
./mvnw spring-boot:run
```

**Terminal 2 — Watch DLT topic continuously:**
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events-topic.DLT \
  --from-beginning \
  --property print.key=true \
  --property print.offset=true \
  --property print.headers=true
```

---

## Step 2 — Send Multiple Failing Events

In a third terminal:
```bash
for errorType in TRANSIENT PERMANENT VALIDATION; do
  curl -s -X POST \
    "http://localhost:8080/api/kafka/events/test?eventType=FAIL_TEST&simulateError=true&errorType=$errorType" \
    | jq -r '"Sent: \(.eventId)"'
  sleep 10  # Wait for retries to exhaust before next event
done
```

In Terminal 2 (Kafka console consumer), you should briefly see each DLT message appear before the `DeadLetterConsumer` deletes it.

### ✅ Checkpoint 1: DLT messages appear momentarily in CLI consumer

---

## Step 3 — Inspect DLT Headers Programmatically

Run this command while a DLT message exists (between routing and deletion):

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events-topic.DLT \
  --from-beginning \
  --property print.headers=true \
  --max-messages 1
```

You'll see headers like:
```
kafka_dlt-exception-message:Transient error,kafka_dlt-original-topic:events-topic,...
```

---

## Step 4 — Understand Record Deletion

The `DeadLetterConsumer` uses `AdminClient.deleteRecords()` to physically remove the message:

```java
// In DeadLetterConsumer.deleteMessage():
long beforeOffset = consumerRecord.offset() + 1;
Map<TopicPartition, RecordsToDelete> toDelete =
    Map.of(topicPartition, RecordsToDelete.beforeOffset(beforeOffset));
adminClient.deleteRecords(toDelete).all().get();
```

Verify the low-watermark (earliest available offset) advances after deletion:

```bash
# Before sending a failing event, check low watermark:
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic events-topic.DLT \
  --time -2   # -2 = earliest offset

# Send a failing event (wait ~10s for retries + deletion)
curl -s -X POST \
  "http://localhost:8080/api/kafka/events/test?eventType=DELETE_TEST&simulateError=true&errorType=TRANSIENT" | jq

sleep 15

# Check low watermark again — it should have advanced
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic events-topic.DLT \
  --time -2
```

### ✅ Checkpoint 2: DLT low watermark advances after record deletion

---

## Step 5 — The Stuck Partition Problem (Proof That DLT Unblocks It)

Without a DLT, a permanent error would cause the consumer to retry infinitely, blocking all subsequent messages on that partition.

Let's simulate this concept by checking the main topic:

```bash
# Send a failing event + several normal events
curl -s -X POST \
  "http://localhost:8080/api/kafka/events/test?eventType=FAIL_ME&simulateError=true&errorType=TRANSIENT" | jq &

sleep 0.5  # Small gap

# These normal events on OTHER partitions still process normally
for i in {1..5}; do
  curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=NORMAL_$i" | jq .eventId
done
```

Watch the logs — normal events continue processing while the TRANSIENT event is retrying.

After ~7 seconds, the TRANSIENT event moves to DLT and **the partition is unblocked**.

### ✅ Checkpoint 3: Normal events on other partitions never pause during error handling

---

## Step 6 — DLT Consumer Ack Before Delete

Notice the critical ordering in `DeadLetterConsumer`:

```java
ack.acknowledge();      // 1. Commit DLT offset first
deleteMessage(...);     // 2. Then delete the physical record
```

**Why this order?**

If deletion fails and we haven't acked, the DLT consumer will **re-process** the same message on restart. With ack-first:

- The DLT offset advances regardless of deletion success
- Deletion failure is logged as an error but doesn't block the DLT consumer
- The record may remain in the DLT (delete retried on a second attempt or cleaned up manually)

### ✅ Checkpoint 4: Both ack and delete success appear in logs, ack line always comes first

---

## Step 7 — What Happens if DLT Consumer Is Down?

1. **Stop the app** (`Ctrl+C`)
2. **Send a failing event** — this won't work since the producer is in the app. Instead, simulate by producing directly:

```bash
# Simulate a message already in the DLT by producing to it directly
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic events-topic.DLT
# Type: {"eventId":"test-dlt","eventType":"ORPHAN"}
# Press Ctrl+C
```

3. **Restart the app** — `ManualAckConsumer` won't process DLT messages, but `DeadLetterConsumer` will pick up the orphan:

```
=== DEAD LETTER RECEIVED ===
Original Topic: null    ← no DLT headers since we produced manually
Error: null
Payload: {eventId: test-dlt, ...}
```

### ✅ Checkpoint 5: DLT consumer processes messages published directly to DLT (no headers)

---

## Step 8 — Monitor DLT in Kafka-UI

1. **Topics → events-topic.DLT → Messages** — watch messages appear and disappear
2. **Topics → events-topic.DLT → Partitions** — observe the low-watermark (Start offset) advancing as records are deleted
3. **Consumers → dlt-consumer-group** — lag should always be 0 (processed immediately)

---

## Summary: DLT Flow Chart

```
Main topic message → Consumer exception → Retry backoff × 3
    ↓ (all retries exhausted)
DeadLetterPublishingRecoverer
    → copy message to events-topic.DLT
    → add diagnostic headers
    → advance main topic offset (unblock partition)
        ↓
DeadLetterConsumer.consumeDeadLetter()
    → log: original topic, error message, payload
    → ack.acknowledge() (advance DLT committed offset)
    → AdminClient.deleteRecords() (remove from log)
```

---

## What You Learned

- ✅ Monitored DLT in real-time with CLI consumer and Kafka-UI
- ✅ Observed diagnostic headers added by `DeadLetterPublishingRecoverer`
- ✅ Verified physical record deletion via low-watermark advancement
- ✅ Proved that DLT routing unblocks the main partition for subsequent messages
- ✅ Understood the ack-before-delete ordering decision and its implications

**Next:** [Avro & Schema Registry (Lab 06)](lab-06-avro-schema-registry.md)

