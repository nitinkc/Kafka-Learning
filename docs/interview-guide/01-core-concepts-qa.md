# Interview Q&A · Core Kafka Concepts

Master these 15 core Kafka concepts that appear in **every** Kafka interview.

---

## 1. 🟢 What is a Kafka Broker?

**Q:** Explain what a Kafka broker is and its role in a cluster.

**Answer:**

A **Kafka broker** is a single server process that:

- **Stores** partitions (copies of topic data) on disk
- **Receives messages** from producers via `send()`
- **Serves messages** to consumers via `fetch()`
- **Replicates** data to other brokers for fault tolerance
- **Participates** in a cluster under ZooKeeper/controller coordination

**In this project:**
```yaml
kafka:
  image: confluentinc/cp-kafka:7.5.0
  ports:
    - "9092:9092"  # ← Broker listening for producers/consumers
```

A single broker handles all topics in the cluster. A production cluster has **3+ brokers** for high availability.

---

## 2. 🟢 What is a Topic?

**Q:** What is a Kafka topic? How does it differ from a message queue?

**Answer:**

A **topic** is a **logical channel** for related events. Unlike message queues (which delete messages after consumption), Kafka topics are **durable append-only logs**.

**Key differences:**

| Aspect | Kafka Topic | Message Queue |
|--------|------------|---------------|
| Deletion | After retention period (days/bytes) | After consumption |
| Multiple consumers | ✅ Yes (same topic) | ❌ Only one queue drains messages |
| Replay | ✅ Reset offset to replay | ❌ Lost forever |
| Use case | Event streaming, audit logs | Work distribution |

**In this project:**
```java
@Bean
public NewTopic mainTopic() {
    return TopicBuilder.name("events-topic")
        .partitions(3)
        .replicas(1)
        .build();
}
```

Three topics exist:

- `events-topic` — main event stream
- `events-topic.DLT` — dead-letter (failed events)
- `avro-events-topic` — Avro-serialized events

---

## 3. 🟢 What is a Partition?

**Q:** Why does Kafka partition topics? What is a partition?

**Answer:**

A **partition** is a **physical shard** of a topic — an ordered, immutable log stored on broker disk.

**Partitions enable:**

1. **Parallelism** — Multiple consumers read different partitions simultaneously
2. **Ordering guarantee** — Messages within a partition are ordered by offset
3. **Throughput** — Data distributed across multiple disks / CPUs

**Example:** `events-topic` has **3 partitions**

```
Partition 0: [Message 0] [Message 1] [Message 2] [Message 3] ...
Partition 1: [Message 0] [Message 1] [Message 2] ...
Partition 2: [Message 0] [Message 1] ...
```

When a producer sends a message with key `"order-123"`:
```
Hash("order-123") % 3 = 0  → Goes to Partition 0 (deterministic)
```

All messages with the same key go to the same partition → **order guaranteed**.

---

## 4. 🟢 What is an Offset?

**Q:** What is an offset and why is it important?

**Answer:**

An **offset** is a **unique sequence number** assigned to each message within a partition.

```
Partition 0:
  Offset 0: {eventId: "evt-001", ...}
  Offset 1: {eventId: "evt-002", ...}
  Offset 2: {eventId: "evt-003", ...}
  ↑ Next message written = Offset 3
```

**Offsets enable:**

1. **Positioning** — "Give me messages from offset 100 onwards"
2. **Replay** — Consumer can reset offset to re-read events
3. **At-least-once semantics** — If consumer crashes before committing offset 100, it restarts at 100

**In this project:**
```java
@KafkaListener(topics = "${kafka.topics.main}", groupId = "manual-ack-group")
public void consume(@Payload Event event, 
                    @Header(KafkaHeaders.OFFSET) long offset,
                    Acknowledgment ack) {
    log.info("Offset: {}", offset);  // ← Sequence number within partition
    processingService.processEvent(event);
    ack.acknowledge();  // ← Commit this offset to __consumer_offsets
}
```

---

## 5. 🟡 How are Messages Assigned to Partitions?

**Q:** How does Kafka decide which partition a message goes to?

**Answer:**

### With a Key (Deterministic)

If the message has a **key**, Kafka uses a **hash function**:

```
partition = Hash(key) % numPartitions
```

**Example:**
```java
kafkaTemplate.send(topic, "order-123", event);  // ← key: "order-123"
```

```
Hash("order-123") % 3 = 0  → Partition 0
Hash("order-456") % 3 = 2  → Partition 2
Hash("order-789") % 3 = 1  → Partition 1
Hash("order-123") % 3 = 0  → Partition 0 (again — same key, same partition)
```

All messages with the same key go to **the same partition**, ensuring **order**.

### Without a Key (Round-Robin)

```java
kafkaTemplate.send(topic, null, event);  // ← no key
```

Kafka round-robins across partitions:
```
Message 1 → Partition 0
Message 2 → Partition 1
Message 3 → Partition 2
Message 4 → Partition 0
```

**No ordering guarantee** across messages.

---

## 6. 🟡 What is a Consumer Group?

**Q:** Explain consumer groups and how Kafka balances load.

**Answer:**

A **consumer group** is a **set of consumers reading from the same topic together**. Each partition is assigned to at most **one consumer** in the group.

**How load balancing works:**

```
Topic: events-topic (3 partitions)
Consumer Group: manual-ack-group (2 consumers)

Consumer 1: reads Partition 0, Partition 1
Consumer 2: reads Partition 2

If Consumer 3 joins:
Consumer 1: reads Partition 0
Consumer 2: reads Partition 1
Consumer 3: reads Partition 2
```

**In this project:**

```java
// ManualAckConsumer
@KafkaListener(topics = "${kafka.topics.main}", 
               groupId = "manual-ack-group")
public void consume(Event event, Acknowledgment ack) { ... }

// DeadLetterConsumer
@KafkaListener(topics = "${kafka.topics.dlt}", 
               groupId = "dlt-consumer-group")
public void consume(Event event) { ... }
```

Each group maintains **independent offsets** per partition, so multiple groups can independently read the same topic.

---

## 7. 🟡 What Happens During Rebalancing?

**Q:** What is rebalancing? When does it occur?

**Answer:**

**Rebalancing** is the process of redistributing partitions among consumers when the group composition changes.

**When it happens:**
- Consumer joins the group
- Consumer leaves the group (crash or shutdown)
- Consumer times out (not sending heartbeats)

**What happens:**
1. A **Stop-the-World** event: **all consumers pause**, commit their offsets
2. ZooKeeper/controller detects the change
3. Partitions are **reassigned** to consumers
4. Consumers **resume** reading from their new partitions

**Impact:**
- Latency spike: All processing pauses for 1–30 seconds
- **No message loss** (offsets committed before rebalance)

**How to minimize rebalancing:**
- Set long `session.timeout.ms` (default: 10 seconds)
- Implement graceful shutdown (commit final offsets before exit)

---

## 8. 🟡 What is Offset Commit?

**Q:** What does it mean to commit an offset? What happens if you don't?

**Answer:**

**Committing an offset** = recording "we've successfully processed messages up to this offset in this partition".

Kafka stores committed offsets in the `__consumer_offsets` internal topic.

**Automatic Commit (Default):**
```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
```

Every 5 seconds, Spring auto-commits the offset of the last received message (even if processing isn't done yet). Risk: message lost if it crashes between receive and commit.

**Manual Commit (This Project):**
```java
factory.getContainerProperties()
    .setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
```

```java
processingService.processEvent(event);
ack.acknowledge();  // ← Only commit after successful processing
```

**If you don't commit:**
- On restart, consumer reads from the **last committed offset**
- If you never commit, consumer always reads from the beginning (or latest, depending on `auto.offset.reset`)

---

## 9. 🟡 What is Consumer Lag?

**Q:** What is consumer lag and why does it matter?

**Answer:**

**Consumer lag** = the difference between the **latest offset in the partition** and the **consumer's committed offset**.

```
Partition 0:
  Latest offset: 1000
  Consumer committed offset: 995
  Lag: 5 messages
```

**Why it matters:**
- **Lag = 0** → Consumer is caught up, processing in real-time
- **Lag > 0** → Consumer is behind, backlog exists
- **Lag growing** → Consumer can't keep up, may need more instances

**In this project:**
Use Kafka-UI to monitor lag:
1. Open http://localhost:8090
2. Go to **Consumers** → `manual-ack-group`
3. See lag per partition (should be 0 if no backlog)

---

## 10. 🟡 What is `auto.offset.reset`?

**Q:** What does the `auto.offset.reset` config do?

**Answer:**

`auto.offset.reset` determines **what the consumer does if there's no committed offset** (first run or offset expired).

**Options:**

| Value | Behavior |
|-------|----------|
| `earliest` | Start from offset 0 (read all messages) |
| `latest` | Start from the latest offset (skip old messages) |
| `none` | Throw error (fail fast) |

**In this project:**
```java
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
```

**Example:**
- Consumer `manual-ack-group` starts for the first time
- No committed offset exists
- Consumer starts reading from the **latest** offset (new messages only)
- It skips all past events

**Common pattern:** Use `latest` for new services (don't replay history), `earliest` for critical services (replay all events).

---

## 11. 🔴 What is Replication?

**Q:** How does Kafka achieve fault tolerance via replication?

**Answer:**

**Replication** = each partition is copied to multiple brokers. A partition has:
- **Leader**: Primary replica, receives all writes/reads
- **In-Sync Replicas (ISR)**: Replicas fully caught up with the leader

**Example: 3 brokers, replication factor = 2**

```
Partition 0:
  Leader: Broker 1
  ISR: [Broker 1, Broker 2]  ← Broker 3 is not replicated

Partition 1:
  Leader: Broker 2
  ISR: [Broker 2, Broker 3]

Partition 2:
  Leader: Broker 3
  ISR: [Broker 3, Broker 1]
```

**Fault tolerance:**
- Broker 1 dies → Broker 2 becomes leader for Partition 0 (leader election)
- Messages written to Broker 1 are already on Broker 2 (no data loss)

**In this project:** replication factor = **1** (single copy per partition). Not production-safe, but fine for learning.

---

## 12. 🔴 What is ZooKeeper's Role?

**Q:** What does ZooKeeper do in a Kafka cluster?

**Answer:**

**ZooKeeper** is a **coordination service** that Kafka relies on for:

1. **Broker registration** — Track which brokers are alive
2. **Leader election** — If partition leader dies, elect new leader from ISRs
3. **Configuration storage** — Store topic configs, ACLs, quotas
4. **Consumer group coordination** — Store consumer group metadata (pre-3.1) or KRaft (3.1+)

**In this project:**
```yaml
zookeeper:
  image: confluentinc/cp-zookeeper:7.5.0
  ports:
    - "2181:2181"
```

You rarely interact with ZooKeeper directly. Kafka handles it internally.

**Note:** Kafka 3.3+ introduces **KRaft mode**, which replaces ZooKeeper with Kafka's own consensus mechanism.

---

## 13. 🔴 What is Message Retention?

**Q:** How long does Kafka keep messages? How can you control it?

**Answer:**

Kafka retains messages based on **time or size**:

**Time-based Retention:**
```properties
log.retention.hours=168  # Keep messages for 7 days
```

**Size-based Retention:**
```properties
log.retention.bytes=1073741824  # Keep until partition exceeds 1 GB
```

**Whichever limit is reached first, oldest messages are deleted.**

**In production:**
- High-throughput topics: 1–3 days retention (to limit storage)
- Audit logs: 1–2 years (compliance)
- DLT: 1 day (don't keep failures forever)

**Impact:**
- If consumer lag exceeds retention period, consumer can't replay old messages
- If `auto.offset.reset=earliest` but messages are deleted, consumer errors out

**Kafka never overwrites a committed offset** — data is always safe once written.

---

## 14. 🔴 What is the Difference Between `min.insync.replicas` and `acks`?

**Q:** How do `acks` and `min.insync.replicas` affect producer durability?

**Answer:**

These configs control **how many brokers must acknowledge** before a producer considers the message "sent".

**Producer `acks` config:**

| Value | Durability | Latency |
|-------|-----------|---------|
| `0` | No ack | Fastest (fire and forget) |
| `1` | Leader acks | Fast (leader only) |
| `all` | All ISRs ack | Slowest (strongest guarantee) |

**`min.insync.replicas` (broker config):**
```properties
min.insync.replicas=2
```

When `acks=all`, Kafka waits for **at least 2 in-sync replicas** to acknowledge before responding to producer.

**Example:**
```
Producer sends with acks=all
Partition 0: Leader (Broker 1) + ISR [Broker 2]
Broker 1 receives message ✓
Broker 2 replicates message ✓
min.insync.replicas=2 satisfied → Producer gets success ✓
```

**Production setting:** `acks=all` + `min.insync.replicas=2` = **no data loss** (at cost of latency).

---

## 15. 🔴 What Happens When a Consumer Falls Behind?

**Q:** Describe what happens when a consumer is too slow to keep up with incoming messages. How do you recover?

**Answer:**

When a consumer **cannot keep up**, lag grows exponentially.

**Causes:**
- Slow processing logic (db calls, API calls, etc.)
- Hardware bottleneck (CPU, disk, network)
- Consumer crash + restart (temporary pauses)

**What happens:**

```
Partition 0:
  Latest offset: 10000
  Consumer offset: 5000
  Lag: 5000 messages (accumulating)
```

**Recovery options:**

1. **Add more consumers** (scale horizontally)
   ```java
   // Instead of 1 consumer reading all 3 partitions:
   // Deploy 3 consumer instances → each reads 1 partition
   ```

2. **Optimize processing** (scale vertically)
   - Cache database queries
   - Use async I/O
   - Batch processing

3. **Increase consumer concurrency**
   ```java
   factory.setConcurrency(4);  // 4 concurrent threads per consumer
   ```

4. **If messages are old, skip them** (dangerous!)
   ```bash
   # Reset consumer group offset to latest (lose history)
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --group manual-ack-group --reset-offsets --to-latest --execute
   ```

**Best practice:** Monitor lag in Kafka-UI; set up alerts when lag > threshold.

---

## Summary Table

| Concept | Definition |
|---------|-----------|
| **Broker** | Server storing partitions |
| **Topic** | Named log stream (persistent, replay-able) |
| **Partition** | Physical shard; enables parallelism & ordering |
| **Offset** | Message sequence number within partition |
| **Consumer Group** | Set of consumers load-balancing partitions |
| **Rebalancing** | Reassigning partitions when group changes |
| **Offset Commit** | Recording "we've processed up to this offset" |
| **Consumer Lag** | Latest offset - committed offset |
| **Replication** | Copying partitions across brokers |
| **ZooKeeper** | Coordination service (broker registration, leader election) |
| **Retention** | How long messages are kept |
| **`acks`** | How many replicas must acknowledge before success |
| **`min.insync.replicas`** | Minimum in-sync replicas for durability |

---

## Next Steps

- **Lab 01–07**: Implement these concepts hands-on
- **Spring Kafka Q&A**: [Spring Kafka Setup (02)](02-spring-kafka-qa.md) — How Spring Boot integrates with Kafka
- **Theory Deep Dive**: [Consumer Groups & Offsets (04)](../theory/04-consumer-groups-and-offsets.md)


