# Interview Q&A · Spring Kafka

Master these 15 Spring Kafka concepts — key to Spring Boot microservices interviews.

---

## 1. 🟢 What is KafkaTemplate?

**Q:** What is KafkaTemplate and how do you use it to send messages?

**Answer:**

**KafkaTemplate** is Spring's abstraction for sending messages to Kafka. It wraps the native Kafka `KafkaProducer` and provides:
- Synchronous send with callback
- Asynchronous send with `ListenableFuture`
- Serialization/deserialization

**In this project:**

```java
@Bean
public KafkaTemplate<String, Object> kafkaTemplate(
        ProducerFactory<String, Object> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
}
```

**Usage in EventProducer:**

```java
@Service
public class EventProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.main}")
    private String mainTopic;

    public Event sendEvent(Event event) {
        kafkaTemplate.send(mainTopic, event.getEventId(), event);
        log.info("Sent event: {}", event.getEventId());
        return event;
    }
}
```

**Parameters:**
- `mainTopic` — topic name
- `event.getEventId()` — message key (determines partition)
- `event` — message value (serialized to JSON)

---

## 2. 🟢 What is @KafkaListener?

**Q:** Explain @KafkaListener. How does it work?

**Answer:**

**@KafkaListener** is Spring's annotation for consuming messages from Kafka. It:
- Declares a **consumer group**
- Specifies **which topics** to subscribe to
- Handles **serialization/deserialization**
- Manages **offset commits** (configurable)

**In this project:**

```java
@Component
public class ManualAckConsumer {
    @KafkaListener(topics = "${kafka.topics.main}", 
                   groupId = "manual-ack-group")
    public void consume(
            @Payload Event event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {
        log.info("Received - Partition: {}, Offset: {}", partition, offset);
        processingService.processEvent(event);
        ack.acknowledge();  // ← Manual offset commit
    }
}
```

**Key annotations:**
- `@Payload Event event` — Deserialized message value
- `@Header(...)` — Message metadata (partition, offset, timestamp, etc.)
- `Acknowledgment ack` — Manual offset control (only in `MANUAL_IMMEDIATE` mode)

**How it works internally:**
1. Spring creates a `ConcurrentKafkaListenerContainerFactory`
2. It spawns a **listener container** that polls Kafka in the background
3. On each poll, the container invokes the `@KafkaListener` method
4. After the method returns (or throws), offsets are committed (if auto-commit enabled)

---

## 3. 🟢 What is ProducerFactory?

**Q:** What is ProducerFactory? Why do you need it?

**Answer:**

**ProducerFactory** is a Spring factory that creates **KafkaProducer** instances. It:
- Configures producer settings (bootstrap servers, serializers, acks, etc.)
- Caches producer instances for reuse (lightweight)
- Manages producer lifecycle

**In this project (JSON):**

```java
@Bean
public ProducerFactory<String, Object> producerFactory(
        ObjectMapper kafkaObjectMapper) {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Custom serializer for JSON
    Serializer<Object> valueSerializer = new Serializer<>() {
        @Override
        public byte[] serialize(String topic, Object data) {
            if (data == null) return null;
            try {
                return kafkaObjectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing JSON", e);
            }
        }
    };
    return new DefaultKafkaProducerFactory<>(props, 
        new StringSerializer(), valueSerializer);
}
```

**In this project (Avro):**

```java
@Bean
public ProducerFactory<String, SpecificRecord> avroProducerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
        KafkaAvroSerializer.class);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 
        schemaRegistryUrl);
    return new DefaultKafkaProducerFactory<>(props);
}
```

**Why separate factories?**
- JSON producer uses custom Jackson serializer
- Avro producer uses Confluent's KafkaAvroSerializer (different config)
- Each can have different retry policies, timeouts, etc.

---

## 4. 🟢 What is ConsumerFactory?

**Q:** What is ConsumerFactory? How is it different from ProducerFactory?

**Answer:**

**ConsumerFactory** is a Spring factory that creates **KafkaConsumer** instances. It:
- Configures consumer settings (bootstrap servers, deserializers, group id, offsets, etc.)
- Caches consumer instances
- Manages consumer lifecycle

**In this project (JSON):**

```java
@Bean
public ConsumerFactory<String, Event> consumerFactory(
        ObjectMapper kafkaObjectMapper) {
    Deserializer<Event> valueDeserializer = new Deserializer<>() {
        @Override
        public Event deserialize(String topic, byte[] data) {
            if (data == null) return null;
            try {
                return kafkaObjectMapper.readValue(data, Event.class);
            } catch (Exception e) {
                throw new SerializationException(
                    "Error deserializing JSON to Event", e);
            }
        }
    };

    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    return new DefaultKafkaConsumerFactory<>(props, 
        new StringDeserializer(), valueDeserializer);
}
```

**Key differences:**
- **ProducerFactory** → sends data (serializes Event → bytes)
- **ConsumerFactory** → receives data (deserializes bytes → Event)

---

## 5. 🟡 What is ConcurrentKafkaListenerContainerFactory?

**Q:** What does ConcurrentKafkaListenerContainerFactory do?

**Answer:**

**ConcurrentKafkaListenerContainerFactory** creates **listener containers** — background threads that:
- **Poll** Kafka for messages
- **Invoke** `@KafkaListener` methods
- **Manage** offsets and error handling

**In this project:**

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, Event> 
        kafkaListenerContainerFactory(
            ConsumerFactory<String, Event> consumerFactory,
            DefaultErrorHandler errorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, Event> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties()
        .setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    factory.setCommonErrorHandler(errorHandler);
    return factory;
}
```

**Key configurations:**

| Setting | Purpose |
|---------|---------|
| `setConsumerFactory()` | Which deserializer to use |
| `setAckMode()` | When to commit offsets (AUTO, MANUAL, etc.) |
| `setCommonErrorHandler()` | What to do on errors (retry, DLT, etc.) |
| `setConcurrency()` | Number of consumer threads |

**How it works:**
1. Spring detects `@KafkaListener` method
2. Creates a listener container using this factory
3. Container spawns N threads (concurrency = N)
4. Each thread polls Kafka in a loop, invokes the method

---

## 6. 🟡 What are Serializers and Deserializers?

**Q:** Explain serializers and deserializers in Kafka. Why are they needed?

**Answer:**

Kafka stores **bytes**, not objects. Serializers/deserializers convert between Java objects and bytes.

**Producer: Serialize (Object → bytes)**
```java
Event event = new Event("evt-001", "ORDER_CREATED", ...);
byte[] bytes = serializer.serialize(topic, event);
// bytes = {0x7B, 0x22, 0x65, ...} (JSON: {"e"...})
kafkaTemplate.send(topic, bytes);
```

**Consumer: Deserialize (bytes → Object)**
```java
byte[] bytes = {0x7B, 0x22, 0x65, ...};
Event event = deserializer.deserialize(topic, bytes);
// event = Event(eventId="evt-001", eventType="ORDER_CREATED", ...)
```

**Common Serializers:**

| Type | Class | Format |
|------|-------|--------|
| JSON | Jackson's `JsonSerializer` | `{"eventId":"...", ...}` (text) |
| Avro | `KafkaAvroSerializer` | Binary (validated against schema) |
| String | `StringSerializer` | Plain text |
| Integer | `IntegerSerializer` | 4 bytes |

**In this project:**

```java
// JSON serializer (custom Jackson-based)
Serializer<Object> jsonSerializer = new Serializer<>() {
    @Override
    public byte[] serialize(String topic, Object data) {
        return kafkaObjectMapper.writeValueAsBytes(data);
    }
};

// Avro serializer (Confluent)
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
    KafkaAvroSerializer.class);
```

---

## 7. 🟡 What is AckMode and Why Does It Matter?

**Q:** Explain the different AckMode options and when to use each.

**Answer:**

**AckMode** controls **when offsets are committed** (recorded as "processed").

**Options:**

| Mode | When Offset Commits | Risk | Use Case |
|------|-------------------|------|----------|
| `AUTO` | After listener returns | May commit before processing done | Simple, non-critical apps |
| `MANUAL` | When `ack.acknowledge()` called | Manual code required | Exactly-once semantics |
| `MANUAL_IMMEDIATE` | ✅ **This project** — Immediate when `ack()` called | None (offset committed immediately) | Critical apps, guaranteed processing |
| `RECORD` | After each message processed | May lose batches on crash | Batch processing |
| `BATCH` | After all polled messages processed | Lag spike on rebalance | High-throughput apps |

**In this project (MANUAL_IMMEDIATE):**

```java
factory.getContainerProperties()
    .setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
```

```java
@KafkaListener(topics = "${kafka.topics.main}", groupId = "manual-ack-group")
public void consume(@Payload Event event, Acknowledgment ack) {
    try {
        processingService.processEvent(event);  // Might throw
        ack.acknowledge();  // ← Only commit if successful
    } catch (Exception e) {
        // Don't call ack() → offset not committed → message re-read on restart
        throw e;
    }
}
```

**Semantics:**
- **At-most-once**: Commit offset immediately (risk: process, crash, offset committed → message lost)
- **At-least-once**: ✅ Commit offset after processing (risk: process, crash, message re-processed on restart)
- **Exactly-once**: At-least-once + idempotent processing (application logic must be idempotent)

---

## 8. 🟡 What is ENABLE_AUTO_COMMIT_CONFIG?

**Q:** What does `ENABLE_AUTO_COMMIT_CONFIG` do? Should you enable it?

**Answer:**

**`ENABLE_AUTO_COMMIT_CONFIG`** controls whether Spring **automatically commits offsets** at regular intervals.

**Enabled (default):**
```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
```

Every 5 seconds, Kafka commits the offset of the **last received message**, even if processing isn't done. Risk: message lost if it crashes between receive and commit.

**Disabled (this project):**
```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
```

Offsets are **never auto-committed**. You must call `ack.acknowledge()` manually. More control, less magic.

**Rule of thumb:**
- **Critical apps** (financial, health, fraud): Disable auto-commit + use MANUAL_IMMEDIATE
- **Non-critical apps** (analytics, logging): Enable auto-commit (simpler)

---

## 9. 🟡 What is AUTO_OFFSET_RESET_CONFIG?

**Q:** What does `AUTO_OFFSET_RESET_CONFIG` do?

**Answer:**

**`AUTO_OFFSET_RESET_CONFIG`** determines what happens when a consumer has **no committed offset** (first run or offset expired).

**Options:**

| Value | Behavior |
|-------|----------|
| `earliest` | Start from offset 0 (read all messages from partition start) |
| `latest` | Start from latest offset (skip old messages) |
| `none` | Throw error (fail fast) |

**In this project:**
```java
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
```

**Scenario:**
```
Consumer group "manual-ack-group" starts for the first time.
Partition 0 has messages at offsets 0–999.
Latest offset = 999.

With AUTO_OFFSET_RESET_CONFIG = "latest":
  → Consumer starts reading from offset 1000 (next message only)
  → Old messages are skipped

With AUTO_OFFSET_RESET_CONFIG = "earliest":
  → Consumer starts reading from offset 0
  → All old messages are replayed
```

**Common patterns:**
- New service (don't need history): `latest`
- Critical service (replay all history): `earliest`
- Audit logs (must not skip): `earliest`

---

## 10. 🔴 What is DefaultErrorHandler?

**Q:** How does DefaultErrorHandler work? When should you use it?

**Answer:**

**DefaultErrorHandler** is Spring's configurable error handler for consumers. It:
- **Retries** failed messages with configurable backoff
- **Publishes** failed messages to a Dead-Letter Topic (DLT)
- **Logs** errors for debugging

**In this project:**

```java
@Bean
public DefaultErrorHandler errorHandler(
        KafkaTemplate<String, Object> kafkaTemplate) {
    DeadLetterPublishingRecoverer recoverer = 
        new DeadLetterPublishingRecoverer(kafkaTemplate,
            (r, ex) -> new org.apache.kafka.common.TopicPartition(dltTopic, 0));
    
    ExponentialBackOffWithMaxRetries backOff = 
        new ExponentialBackOffWithMaxRetries(3);
    backOff.setInitialInterval(1000L);      // 1 second
    backOff.setMultiplier(2.0);             // 2x each retry
    
    return new DefaultErrorHandler(recoverer, backOff);
}
```

**Backoff pattern:**
```
Retry 1: Wait 1 second, retry
Retry 2: Wait 2 seconds, retry
Retry 3: Wait 4 seconds, retry
After 3 retries: Publish to DLT
```

**How it works:**
1. `@KafkaListener` throws exception
2. ErrorHandler catches it
3. Waits (backoff), then container re-invokes the listener with the same message
4. After max retries, `DeadLetterPublishingRecoverer` sends message to DLT topic

---

## 11. 🔴 What is DeadLetterPublishingRecoverer?

**Q:** What does DeadLetterPublishingRecoverer do?

**Answer:**

**DeadLetterPublishingRecoverer** is a **recovery strategy** that publishes failed messages to a **Dead-Letter Topic (DLT)**.

**In this project:**

```java
DeadLetterPublishingRecoverer recoverer = 
    new DeadLetterPublishingRecoverer(kafkaTemplate,
        (r, ex) -> new org.apache.kafka.common.TopicPartition(
            dltTopic,  // "events-topic.DLT"
            0));       // Send to partition 0
```

**Flow:**
```
Original Topic: events-topic
  ↓
@KafkaListener
  ↓
processingService.processEvent(event)  → throws exception
  ↓
DefaultErrorHandler catches + retries 3x
  ↓
After retries exhausted:
  ↓
DeadLetterPublishingRecoverer.sendToDlt()
  ↓
DLT Topic: events-topic.DLT
  ↓
DeadLetterConsumer reads from DLT
```

**DLT headers added by Spring:**
```
dlt-original-topic: events-topic
dlt-original-partition: 0
dlt-original-offset: 1234
dlt-exception-message: java.lang.RuntimeException: Validation failed
dlt-exception-fqcn: java.lang.RuntimeException
```

**Consumed in DeadLetterConsumer:**
```java
@KafkaListener(topics = "${kafka.topics.dlt}", groupId = "dlt-consumer-group")
public void consumeDlt(Event event, @Headers Map<String, Object> headers) {
    String originalTopic = (String) headers.get("dlt-original-topic");
    long originalOffset = (long) headers.get("dlt-original-offset");
    log.error("DLT message from topic={}, offset={}", originalTopic, originalOffset);
    // Alert/delete/retry logic
}
```

---

## 12. 🔴 What is ExponentialBackOffWithMaxRetries?

**Q:** Explain exponential backoff and why it's better than immediate retry.

**Answer:**

**ExponentialBackOffWithMaxRetries** increases the **wait time between retries** to avoid overwhelming a struggling service.

**In this project:**
```java
ExponentialBackOffWithMaxRetries backOff = 
    new ExponentialBackOffWithMaxRetries(3);  // Max 3 retries
backOff.setInitialInterval(1000L);             // Start at 1 second
backOff.setMultiplier(2.0);                    // Double each time
```

**Retry timeline:**
```
Attempt 1: fails immediately
  ↓ Wait 1 second
Attempt 2: fails again
  ↓ Wait 2 seconds (1 * 2)
Attempt 3: fails again
  ↓ Wait 4 seconds (2 * 2)
Attempt 4: fails again
  ↓ No more retries → DLT
```

**Why exponential backoff?**

**Immediate retry (bad):**
```
Request fails → Retry immediately → Fails → Retry → Fails → Retry
Service is still struggling, hammered with retries → Makes it worse
```

**Exponential backoff (good):**
```
Request fails → Wait 1s → Retry → Fails → Wait 2s → Retry → Fails → Wait 4s
Gives service time to recover (e.g., connection timeout, DB slowness)
```

**Common patterns:**
- **API timeouts**: 1s, 2s, 4s, 8s (4 retries)
- **Database contention**: 100ms, 200ms, 400ms (3 retries) — faster
- **External service flakiness**: 5s, 10s, 20s (3 retries) — slower

---

## 13. 🔴 What is KafkaAvroSerializer?

**Q:** What does KafkaAvroSerializer do? How is it different from JsonSerializer?

**Answer:**

**KafkaAvroSerializer** serializes objects as **Avro binary format** and **validates against Schema Registry**.

**Differences from JSON:**

| Aspect | JSON Serializer | Avro Serializer |
|--------|-----------------|-----------------|
| Format | Text (readable) | Binary (compact) |
| Schema validation | None | Requires Schema Registry |
| Size | Larger | Smaller (2–5x compression) |
| Speed | Slower | Faster |
| Breaking changes | Not detected | Detected (compatibility modes) |

**In this project:**

```java
@Bean
public ProducerFactory<String, SpecificRecord> avroProducerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
        KafkaAvroSerializer.class);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 
        schemaRegistryUrl);  // http://localhost:8081
    return new DefaultKafkaProducerFactory<>(props);
}
```

**On first send:**
1. Producer creates an `AvroEvent` object
2. KafkaAvroSerializer contacts **Schema Registry**
3. Registry checks if `avro-events-topic-value` schema exists
4. If not, **registers** the schema
5. Validates `AvroEvent` against the schema
6. Serializes to Avro binary bytes
7. Sends to Kafka

**If validation fails:**
```
SerializationException: Error registering Avro schema
→ Message is REJECTED → Never reaches Kafka
```

---

## 14. 🔴 What is KafkaAvroDeserializer?

**Q:** How does KafkaAvroDeserializer work on the consumer side?

**Answer:**

**KafkaAvroDeserializer** deserializes **Avro binary bytes** and **validates against Schema Registry**.

**In this project:**

```java
@Bean
public ConsumerFactory<String, SpecificRecord> avroConsumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
        KafkaAvroDeserializer.class);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 
        schemaRegistryUrl);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    return new DefaultKafkaConsumerFactory<>(props);
}
```

**On consume:**
1. Consumer receives Avro binary bytes from Kafka
2. KafkaAvroDeserializer contacts **Schema Registry**
3. Fetches the **writer schema** (schema used to write the data)
4. Validates bytes against writer schema
5. Deserializes to `AvroEvent` object
6. Hands to `@KafkaListener` method

**Schema Registry ensures:**
- Producer and consumer schemas are **compatible**
- Old consumers can read messages written by new producers (backward compatibility)
- New consumers can read messages written by old producers (forward compatibility)

---

## 15. 🔴 What is the Difference Between Producer Throughput and Durability?

**Q:** How do you balance throughput vs durability in Kafka producers?

**Answer:**

**Throughput** = messages sent per second
**Durability** = guarantee that messages aren't lost

**Producer `acks` config controls this tradeoff:**

| Config | Behavior | Throughput | Durability |
|--------|----------|-----------|-----------|
| `acks=0` | Fire & forget (no ack) | 🚀 Fastest | ❌ None (message may be lost) |
| `acks=1` | Leader acks | ⚡ Fast | ⚠️ Data loss if leader dies |
| `acks=all` | All ISRs ack | 🐌 Slowest | ✅ No loss (if min.insync.replicas≥2) |

**Example:**

```java
// Low durability, high throughput
props.put(ProducerConfig.ACKS_CONFIG, "0");
kafkaTemplate.send(topic, event);  // Returns immediately, no ack

// High durability, low throughput
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put("min.insync.replicas", 2);
kafkaTemplate.send(topic, event);  // Waits for 2 replicas, then returns
```

**Recommendation for this project:**
```java
props.put(ProducerConfig.ACKS_CONFIG, "all");  // Durability-first
props.put(ProducerConfig.RETRIES_CONFIG, 3);   // Retry on transient failures
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");  // Compress for throughput
```

**Typical production settings:**
- **Financial transactions**: `acks=all` (no message loss)
- **Analytics**: `acks=1` (lose <0.1% is acceptable)
- **Logs**: `acks=0` (maximum throughput, loss acceptable)

---

## Summary Table

| Component | Purpose |
|-----------|---------|
| **KafkaTemplate** | Send messages to Kafka |
| **@KafkaListener** | Consume messages from Kafka |
| **ProducerFactory** | Create/configure KafkaProducer |
| **ConsumerFactory** | Create/configure KafkaConsumer |
| **ConcurrentKafkaListenerContainerFactory** | Create listener containers (polling threads) |
| **AckMode** | Control when offsets are committed |
| **DefaultErrorHandler** | Retry logic + DLT |
| **DeadLetterPublishingRecoverer** | Publish failed messages to DLT |
| **ExponentialBackOffWithMaxRetries** | Increase wait time between retries |
| **KafkaAvroSerializer** | Serialize to Avro + validate via Schema Registry |
| **KafkaAvroDeserializer** | Deserialize from Avro + validate via Schema Registry |

---

## Next Steps

- **Lab 02–07**: Implement these patterns hands-on
- **Error Handling Q&A**: [03 · Error Handling & Schema](03-error-handling-qa.md)
- **Theory Deep Dive**: [Theory 02 · Spring Kafka Setup](../theory/02-spring-kafka-setup.md)


