# Lab 01 · Docker Setup & Kafka Fundamentals

!!! info "Theory"
    Read [01 · Kafka Fundamentals](../theory/01-kafka-fundamentals.md) first.

**Goal:** Start the full Kafka infrastructure with Docker, verify every service is healthy, and explore the Kafka-UI dashboard.

**Time:** ~15 minutes

---

## Prerequisites

- Docker Desktop installed and running
- Port 2181, 9092, 8081, 8090 are free on your machine

---

## Step 1 — Start All Containers

```bash
cd /path/to/Kafka-Learning

docker compose up -d
```

Expected output:
```
✔ Container zookeeper       Started
✔ Container kafka           Started
✔ Container schema-registry Started
✔ Container kafka-ui        Started
```

### ✅ Checkpoint 1: All containers running

```bash
docker compose ps
```

All four containers should show `running` (or `Up`):
```
NAME              IMAGE                                      STATUS
kafka             confluentinc/cp-kafka:7.5.0                Up
kafka-ui          provectuslabs/kafka-ui:latest              Up
schema-registry   confluentinc/cp-schema-registry:7.5.0     Up
zookeeper         confluentinc/cp-zookeeper:7.5.0            Up
```

---

## Step 2 — Wait for Kafka to Be Ready

```bash
docker logs -f kafka 2>&1 | grep -m1 "started (kafka.server.KafkaServer)"
```

Press `Ctrl+C` once you see the line. This typically takes 15–30 seconds.

### ✅ Checkpoint 2: Kafka responding to commands

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Should return (possibly empty) without error.

---

## Step 3 — Verify Schema Registry

```bash
curl -s http://localhost:8081/subjects | jq
```

Expected: `[]` (empty list — no schemas registered yet)

---

## Step 4 — Create Topics Manually

```bash
# Main event topic — 3 partitions
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic events-topic --partitions 3 --replication-factor 1

# Dead-letter topic — 1 partition
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic events-topic.DLT --partitions 1 --replication-factor 1

# Avro topic — 3 partitions
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic avro-events-topic --partitions 3 --replication-factor 1
```

### ✅ Checkpoint 3: Confirm topics exist

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Expected:
```
avro-events-topic
events-topic
events-topic.DLT
```

---

## Step 5 — Inspect Topics

```bash
# See partition details for events-topic
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic events-topic
```

Expected output:
```
Topic: events-topic    PartitionCount: 3    ReplicationFactor: 1
    Partition: 0    Leader: 1    Replicas: 1    Isr: 1
    Partition: 1    Leader: 1    Replicas: 1    Isr: 1
    Partition: 2    Leader: 1    Replicas: 1    Isr: 1
```

Key fields:

- **PartitionCount** — 3, as configured in `KafkaConfig`
- **Leader** — broker ID 1 (our single broker)
- **ISR** — In-Sync Replicas (1 = just the leader, since RF=1)

---

## Step 6 — Produce & Consume with CLI

Send a message using the Kafka CLI producer:

```bash
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic events-topic \
  --property "key.separator=:" \
  --property "parse.key=true"
```

Type a message and press Enter:
```
key1:{"eventId":"test-001","eventType":"ORDER_CREATED"}
```
Press `Ctrl+C` to exit.

Consume it:
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events-topic \
  --from-beginning \
  --property print.key=true \
  --property print.offset=true \
  --max-messages 5
```

---

## Step 7 — Explore Kafka-UI

Open [http://localhost:8090](http://localhost:8090)

1. **Dashboard** → see the `local` cluster with 1 broker
2. **Topics** → click `events-topic` → **Messages** tab → find the message you produced
3. **Brokers** → click `Broker ID 1` → see configuration
4. Click `events-topic` → **Partitions** tab → see the 3 partitions with their offsets

### ✅ Checkpoint 4: Message visible in Kafka-UI

The CLI-produced message should be visible in the Messages tab with key, value, partition, and offset.

---

## Step 8 — Inspect Consumer Offsets (No Consumer Yet)

```bash
# List all consumer groups
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --list
```

No groups yet (empty output) — the Spring Boot app hasn't started.

---

## Cleanup

Keep the containers running for the next labs. If you need a clean slate:

```bash
docker compose down -v   # removes containers AND data volumes
docker compose up -d     # fresh start
```

---

## What You Learned

- ✅ Started a full Kafka stack with 4 containers using `docker compose up -d`
- ✅ Created topics with specific partition counts using the Kafka CLI
- ✅ Produced and consumed messages with the CLI tools
- ✅ Used Kafka-UI to inspect topics, partitions, and messages
- ✅ Understood the relationship between Topics → Partitions → Offsets

**Next:** [Lab 02 · First Producer & Consumer](lab-02-first-producer-consumer.md)

