# Error Handling & Exponential Backoff Retry (Lab 04)

!!! info "Theory"
    Read [Error Handling & Dead Letter Topics (05)](../theory/05-error-handling-and-dlt.md) first.

**Goal:** Trigger transient, permanent, and validation errors. Observe the exponential backoff retry sequence in logs and confirm failed messages land in the DLT.

**Time:** ~25 minutes

---

## Prerequisites

- Lab 01–02 complete — Docker running, Spring Boot app running
- Kafka-UI open at [http://localhost:8090](http://localhost:8090)

---

## Step 1 — Baseline: Normal Event (No Error)

```bash
curl -s -X POST \
  "http://localhost:8080/api/kafka/events/test?eventType=ORDER_CREATED" | jq
```

App log:
```
Received - Partition: X, Offset: Y, EventId: ...
Processing event: ... - Type: ORDER_CREATED
Acknowledged offset: Y
```

### ✅ Checkpoint 1: Normal processing — one log line per stage, no retries

---

## Step 2 — Trigger a Transient Error

A `TRANSIENT` error simulates a temporary failure (e.g., downstream service timeout):

```bash
curl -s -X POST \
  "http://localhost:8080/api/kafka/events/test?eventType=ORDER_FAILED&simulateError=true&errorType=TRANSIENT" | jq
```

Watch the app logs carefully. You should see:

```
Received - Partition: X, Offset: Y, EventId: ...
Processing event: ... - Type: ORDER_FAILED
Transient error, will retry. Offset: Y         ← attempt 1
                                                (wait 1 second)
Received - Partition: X, Offset: Y, EventId:  ← retry attempt 2
Transient error, will retry. Offset: Y
                                                (wait 2 seconds)
Received - Partition: X, Offset: Y, EventId:  ← retry attempt 3
Transient error, will retry. Offset: Y
                                                (wait 4 seconds)
Received - Partition: X, Offset: Y, EventId:  ← retry attempt 4
Transient error, will retry. Offset: Y
                                                ← MAX RETRIES EXCEEDED → DLT
```

Total time: approximately **7 seconds** (1+2+4).

### ✅ Checkpoint 2: Exactly 4 attempts in logs (3 retries + original), with increasing delay

---

## Step 3 — Verify Message Landed in DLT

After the retries exhaust, check the DLT consumer log:

```
=== DEAD LETTER RECEIVED ===
Original Topic: events-topic
Error: Transient error
Payload: {eventId: ..., eventType: ORDER_FAILED, ...}
Deleted DLT message at topic=events-topic.DLT partition=0 offset=0
```

Also check Kafka-UI:
1. **Topics → events-topic.DLT → Messages**
2. The failed message appears briefly, then disappears (deleted by `DeadLetterConsumer.deleteMessage()`)

### ✅ Checkpoint 3: DLT consumer logged the failed message with original topic and error

---

## Step 4 — Trigger a Permanent Error

A `PERMANENT` error simulates a non-recoverable bug or corrupted data:

```bash
curl -s -X POST \
  "http://localhost:8080/api/kafka/events/test?eventType=ORDER_CORRUPT&simulateError=true&errorType=PERMANENT" | jq
```

Observe the logs — same retry pattern (3 retries), same DLT outcome.

!!! note "Same retry count, different exception"
    Both `TRANSIENT` (TransientException) and `PERMANENT` (RuntimeException) go through the same retry policy by default. The distinction is conceptual — in a production system you'd configure permanent errors to skip retries:
    ```java
    errorHandler.addNotRetryableExceptions(PermanentException.class);
    ```

---

## Step 5 — Trigger a Validation Error

```bash
curl -s -X POST \
  "http://localhost:8080/api/kafka/events/test?eventType=ORDER_INVALID&simulateError=true&errorType=VALIDATION" | jq
```

Same retry pattern and DLT routing.

---

## Step 6 — Interleave Normal and Error Events

```bash
# Send multiple events — mix of success and failure
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_CREATED" | jq
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_FAILED&simulateError=true&errorType=TRANSIENT" | jq
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_CREATED" | jq
```

### ✅ Checkpoint 4: Normal events processed immediately; error events retry in parallel

Observe that while the TRANSIENT event is retrying (waiting 1s, 2s, 4s), the normal events on **other partitions** continue processing — only the failing partition is blocked.

---

## Step 7 — Observe DLT Headers in Kafka-UI

1. Trigger another TRANSIENT error but **before the DLT consumer deletes it**, open Kafka-UI
2. **Topics → events-topic.DLT → Messages**
3. Click on the DLT message to expand it
4. Look at the **Headers** tab — you'll see:

| Header | Value |
|--------|-------|
| `kafka_dlt-exception-message` | `Transient error` |
| `kafka_dlt-exception-fqcn` | `com.demo.kafka.handler.CustomErrorHandler$TransientException` |
| `kafka_dlt-original-topic` | `events-topic` |
| `kafka_dlt-original-partition` | `1` (or whichever) |
| `kafka_dlt-original-offset` | The original message offset |

### ✅ Checkpoint 5: DLT headers are visible in Kafka-UI before deletion

---

## Step 8 — Count Main Topic vs DLT

```bash
# Current offsets for events-topic (total messages written)
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic events-topic

# Current offsets for DLT
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic events-topic.DLT
```

---

## Understanding the Error Flow

```
errorType=TRANSIENT → TransientException thrown in ManualAckConsumer
    → caught by DefaultErrorHandler
        → wait 1s → retry → TransientException again
        → wait 2s → retry → TransientException again
        → wait 4s → retry → TransientException again
        → MAX RETRIES (3) EXCEEDED
        → DeadLetterPublishingRecoverer.recover()
            → write to events-topic.DLT with headers
        → commit main topic offset (partition unblocked)
            → DeadLetterConsumer.consumeDeadLetter()
                → log error
                → ack.acknowledge()
                → AdminClient.deleteRecords()
```

---

## What You Learned

- ✅ Observed the 4-attempt retry sequence (1 original + 3 retries) with exponential backoff
- ✅ Confirmed failed messages route to DLT after retries are exhausted
- ✅ Verified the main topic offset advances after DLT routing (partition unblocked)
- ✅ Inspected DLT diagnostic headers in Kafka-UI
- ✅ Understood the difference between TRANSIENT, PERMANENT, and VALIDATION error types

**Next:** [Dead Letter Topic Deep Dive (Lab 05)](lab-05-dead-letter-topic.md)

