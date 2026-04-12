# Lab 03 · Consumer Groups & Partition Assignment

!!! info "Theory"
    Read [04 · Consumer Groups & Offsets](../theory/04-consumer-groups-and-offsets.md) first.

**Goal:** Observe partition assignment, rebalancing, and consumer lag. Compare behaviour of different consumer groups consuming the same topic independently.

**Time:** ~20 minutes

---

## Prerequisites

- Lab 02 complete — app running, events sent
- Kafka-UI open at [http://localhost:8090](http://localhost:8090)

---

## Step 1 — Inspect Existing Consumer Groups

```bash
# List all groups
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --list
```

Expected:
```
avro-consumer-group
dlt-consumer-group
manual-ack-group
```

Describe the main group:
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group manual-ack-group
```

### ✅ Checkpoint 1: Three partitions assigned, LAG=0

---

## Step 2 — Send a Burst of Messages

```bash
for i in {1..20}; do
  curl -s -X POST \
    "http://localhost:8080/api/kafka/events/test?eventType=BURST_EVENT_$i" > /dev/null
done
echo "Done sending 20 events"
```

Immediately check lag (run quickly after the loop):
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group manual-ack-group
```

If the consumer is fast enough, you'll already see LAG=0. To see non-zero lag, you could slow the consumer down — but for now, observe in Kafka-UI instead.

---

## Step 3 — Observe in Kafka-UI

1. Open [http://localhost:8090](http://localhost:8090)
2. **Consumers → manual-ack-group**
3. See the **lag per partition** and **partition assignment**
4. **Topics → events-topic → Messages** — observe messages spread across 3 partitions

### ✅ Checkpoint 2: All 20 messages visible, distributed across partitions

---

## Step 4 — Multiple Consumer Groups Read Independently

Each consumer group maintains **its own offset**. Let's prove this by creating a new group that reads `events-topic` from the beginning:

```bash
# New consumer group reads from the very beginning
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events-topic \
  --group replay-group \
  --from-beginning \
  --max-messages 10 \
  --property print.partition=true \
  --property print.offset=true
```

This creates a new group `replay-group` that reads all historical messages — **independent of `manual-ack-group`** which has already committed all offsets.

### ✅ Checkpoint 3: replay-group reads old messages without affecting manual-ack-group

Verify `manual-ack-group` offsets are unchanged:
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group manual-ack-group
# LAG should still be 0
```

---

## Step 5 — Trigger a Rebalance (Optional — Requires Terminal Multiplexing)

!!! tip "This step requires starting a second app instance on a different port"

Open a **second terminal**. Export a different port and start another instance:

```bash
# Terminal 2
SERVER_PORT=8081 ./mvnw spring-boot:run \
  -Dspring-boot.run.jvmArguments="-Dserver.port=8082"
```

!!! note "Alternative: just observe in Kafka-UI"
    If running two instances is complex in your setup, open **Kafka-UI → Consumers → manual-ack-group** and watch — when the second instance connects, partition assignments change in real time.

While the second instance is starting, quickly check:
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group manual-ack-group
```

During rebalance you'll see `REBALANCING` state. After it completes, the 3 partitions are split between the two instances.

Stop the second instance (`Ctrl+C` in Terminal 2) — another rebalance occurs and the first instance claims all 3 partitions again.

---

## Step 6 — Reset Offsets (Replay)

Reset `manual-ack-group` to the beginning to replay all events:

```bash
# Stop the app first (Ctrl+C)

# Reset offsets to beginning
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group manual-ack-group \
  --topic events-topic \
  --reset-offsets --to-earliest \
  --execute
```

Restart the app:
```bash
./mvnw spring-boot:run
```

### ✅ Checkpoint 4: App re-processes all historical events from offset 0

In the logs you should see all previously sent events being consumed again, starting from `Offset: 0`.

---

## Step 7 — auto.offset.reset Experiment

Reset the offset back to latest so the group is caught up:
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group manual-ack-group \
  --topic events-topic \
  --reset-offsets --to-latest \
  --execute
```

---

## What You Learned

- ✅ Listed and described consumer groups and their per-partition offsets
- ✅ Observed LAG and confirmed it returns to 0 after processing
- ✅ Proved different consumer groups read independently (replay-group vs manual-ack-group)
- ✅ Triggered a rebalance by starting/stopping a second consumer instance
- ✅ Used `--reset-offsets` to replay historical messages

**Next:** [Lab 04 · Error Handling & Retry](lab-04-error-handling.md)

