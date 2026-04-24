# Interview Q&A · Error Handling & Avro Schema

Master these 12 concepts on error handling, dead-letter topics, and Avro schema validation — critical for production Kafka systems.

---

## 1. 🟡 What is a Dead-Letter Topic (DLT)?

**Q:** What is a dead-letter topic and why is it important?

**Answer:**

A **Dead-Letter Topic (DLT)** is a special topic where **failed messages are sent** after retries are exhausted.

**Flow:**
```
events-topic (normal processing)
    ↓
@KafkaListener
    ↓
processingService.processEvent(event)  → throws exception
    ↓
DefaultErrorHandler.retries(3x with backoff)
    ↓
Still fails → DeadLetterPublishingRecoverer.sendToDlt()
    ↓
events-topic.DLT (failed messages)
    ↓
DeadLetterConsumer (logs, alerts, manual intervention)
```

**Why important:**
- **Prevents poison pills** — One bad message shouldn't block the entire consumer
- **Enables recovery** — Operators can investigate and fix issues
- **Separates concerns** — Main pipeline processes good messages; DLT handles exceptions

**In this project:**

```java
@Bean
public NewTopic deadLetterTopic() {
    return TopicBuilder.name("events-topic.DLT")
        .partitions(1)
        .replicas(1)
        .build();
}

@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
    DeadLetterPublishingRecoverer recoverer = 
        new DeadLetterPublishingRecoverer(kafkaTemplate,
            (r, ex) -> new org.apache.kafka.common.TopicPartition(
                "events-topic.DLT",  // ← DLT name
                0));                  // ← Partition
    return new DefaultErrorHandler(recoverer, backOff);
}
```

---

## 2. 🟡 How Does a Message End Up in the DLT?

**Q:** Describe the complete flow of a message going to the DLT.

**Answer:**

**Scenario:** Message arrives at consumer with bad data (e.g., invalid JSON field).

**Step-by-step:**

```
1. Producer sends Event with simulateError=true, errorType=VALIDATION
   → Message written to events-topic, partition 0, offset 100

2. Consumer group "manual-ack-group" polls from partition 0
   → Receives message at offset 100
   → Calls ManualAckConsumer.consume(event)

3. Inside consume():
   processingService.processEvent(event);  // Checks event.simulateError
   → Detects simulateError=true, errorType=VALIDATION
   → Throws CustomErrorHandler.ValidationException

4. Exception propagates to Spring:
   → ManualAckConsumer.consume() throws exception
   → ack.acknowledge() NOT called
   → Exception reaches DefaultErrorHandler

5. DefaultErrorHandler.handle():
   Retry 1: Wait 1 second, re-invoke consume() → throws again
   Retry 2: Wait 2 seconds, re-invoke consume() → throws again
   Retry 3: Wait 4 seconds, re-invoke consume() → throws again
   Retries exhausted

6. DeadLetterPublishingRecoverer.sendToDlt():
   Sends original message + exception headers to events-topic.DLT
   Headers:
     - dlt-original-topic: events-topic
     - dlt-original-partition: 0
     - dlt-original-offset: 100
     - dlt-exception-message: ValidationException: ...

7. Message appears in DLT, DeadLetterConsumer reads it
   → Logs the error
   → Operator can manually investigate
```

**Code path:**

```java
// 1. Producer (KafkaController)
@PostMapping("/api/kafka/events")
public Event createEvent(@RequestBody Event event) {
    return eventProducer.sendEvent(event);  // → events-topic
}

// 2. Consumer (ManualAckConsumer)
@KafkaListener(topics = "events-topic", groupId = "manual-ack-group")
public void consume(@Payload Event event, Acknowledgment ack) {
    try {
        processingService.processEvent(event);  // ← throws
        ack.acknowledge();  // NOT reached
    } catch (Exception e) {
        throw e;  // ← ErrorHandler catches
    }
}

// 3. Error handler (KafkaConfig)
@Bean
public DefaultErrorHandler errorHandler(...) {
    DeadLetterPublishingRecoverer recoverer = 
        new DeadLetterPublishingRecoverer(kafkaTemplate, ...);
    return new DefaultErrorHandler(recoverer, backOff);  // ← Sends to DLT
}

// 4. DLT consumer (DeadLetterConsumer)
@KafkaListener(topics = "events-topic.DLT", groupId = "dlt-consumer-group")
public void consumeDlt(Event event, @Headers Map<String, Object> headers) {
    log.error("DLT message: {}", event);  // ← Logs for investigation
}
```

---

## 3. 🟡 What Exception Types Should Trigger Retry vs DLT?

**Q:** How do you decide which exceptions should be retried vs sent directly to DLT?

**Answer:**

**Retryable exceptions** — transient, may succeed on retry:
- `IOException` — network hiccup
- `TimeoutException` — service slow but recovering
- `DataAccessException` — database temporarily locked

**Non-retryable exceptions** — permanent, will keep failing:
- `ValidationException` — invalid data, retrying won't help
- `MessageFormatException` — malformed message
- `ClassNotFoundException` — schema mismatch

**In this project, CustomErrorHandler differentiates:**

```java
public class CustomErrorHandler {
    public static class TransientException extends RuntimeException { }
    // More transient exceptions...
}
```

```java
@KafkaListener(topics = "${kafka.topics.main}", groupId = "manual-ack-group")
public void consume(@Payload Event event, Acknowledgment ack) {
    try {
        processingService.processEvent(event);
        ack.acknowledge();
    } catch (CustomErrorHandler.TransientException e) {
        log.warn("Transient error, will retry. Offset: {}", offset);
        throw e;  // ← DefaultErrorHandler retries
    } catch (Exception e) {
        log.error("Non-retryable error: {}", e.getMessage());
        throw e;  // ← Still retried (but could skip if non-retryable)
    }
}
```

**Better approach (skip non-retryable):**

```java
catch (ValidationException e) {
    // Don't throw — consume the message silently, mark as DLT manually
    log.error("Validation failed: {}", e.getMessage());
    kafkaTemplate.send(dltTopic, event);  // Publish to DLT manually
    // No throw → message is acknowledged, not retried
} catch (TransientException e) {
    throw e;  // Retry
}
```

---

## 4. 🟡 What Information is Available in DLT Headers?

**Q:** What headers does Spring add to DLT messages? How do you extract them?

**Answer:**

When a message is sent to DLT, Spring adds **exception metadata headers**:

| Header Key | Value | Example |
|-----------|-------|---------|
| `dlt-original-topic` | Source topic | `events-topic` |
| `dlt-original-partition` | Source partition | `0` |
| `dlt-original-offset` | Source offset | `1234` |
| `dlt-original-timestamp` | Message timestamp | `1681234567890` |
| `dlt-original-timestamp-type` | Timestamp type | `CREATE_TIME` |
| `dlt-exception-fqcn` | Exception class | `java.lang.ValidationException` |
| `dlt-exception-message` | Exception message | `Event validation failed` |
| `dlt-exception-stacktrace` | Stack trace | `at com.demo... (truncated)` |

**Consuming with headers:**

```java
@KafkaListener(topics = "${kafka.topics.dlt}", groupId = "dlt-consumer-group")
public void consumeDlt(
        @Payload Event event,
        @Headers Map<String, Object> headers,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset) {
    
    String originalTopic = (String) headers.get("dlt-original-topic");
    Integer originalPartition = (Integer) headers.get("dlt-original-partition");
    Long originalOffset = (Long) headers.get("dlt-original-offset");
    String exceptionMessage = (String) headers.get("dlt-exception-message");
    
    log.error("DLT message: topic={}, partition={}, offset={}, " +
              "error={}, current_offset={}",
              originalTopic, originalPartition, originalOffset, 
              exceptionMessage, offset);
    
    // Alert monitoring system
    // Save to database for manual review
    // Trigger human intervention
}
```

---

## 5. 🟡 How Do You Prevent Duplicate Processing in DLT?

**Q:** If a consumer crashes before deleting a DLT message, how do you prevent reprocessing?

**Answer:**

DLT messages can be **reprocessed multiple times** if the consumer crashes. To prevent duplicates:

### Option 1: Idempotent Processing (Recommended)

Make business logic idempotent — processing the same message twice has the same effect as once.

```java
// Bad: Not idempotent
public void processOrder(OrderEvent event) {
    deductBalance(event.amount);  // If called twice → balance deducted twice
}

// Good: Idempotent
public void processOrder(OrderEvent event) {
    if (isAlreadyProcessed(event.orderId)) {
        log.info("Order {} already processed, skipping", event.orderId);
        return;
    }
    deductBalance(event.amount);
    markAsProcessed(event.orderId);
}
```

### Option 2: Persistent Offset Tracking (DLT Consumer)

Track which DLT messages you've processed:

```java
@KafkaListener(topics = "${kafka.topics.dlt}", groupId = "dlt-consumer-group")
public void consumeDlt(@Payload Event event, 
                        @Header(KafkaHeaders.OFFSET) long offset,
                        Acknowledgment ack) {
    // Check if already processed
    if (dltProcessedRepository.exists(event.getEventId())) {
        log.info("DLT message {} already processed, skipping", event.getEventId());
        ack.acknowledge();
        return;
    }
    
    // Process & record
    try {
        alertOperations(event);
        dltProcessedRepository.save(event.getEventId());
        ack.acknowledge();
    } catch (Exception e) {
        // Don't acknowledge → will retry on restart
        throw e;
    }
}
```

### Option 3: Delete After Processing

Explicitly remove processed messages from DLT topic:

```java
@KafkaListener(topics = "${kafka.topics.dlt}")
public void consumeDlt(@Payload Event event) {
    try {
        // Process
        alertOperations(event);
        
        // Delete from DLT (not recommended — loses audit trail)
        kafkaTemplate.send("events-topic.DLT", event.getEventId(), null);
    } catch (Exception e) {
        log.error("Failed to process DLT message", e);
    }
}
```

---

## 6. 🔴 What is Avro?

**Q:** What is Avro and why is it useful in Kafka?

**Answer:**

**Avro** is a **schema-based, binary serialization format** developed by Apache. It:
- Defines structure using schemas (`.avsc` files)
- Serializes to **compact binary** (2–5x smaller than JSON)
- Enables **schema versioning and evolution**

**Why Avro in Kafka:**

| Aspect | JSON | Avro |
|--------|------|------|
| Format | Text | Binary |
| Size | Large | Small |
| Schema | Implicit (guessed) | Explicit (validated) |
| Breaking changes | Detected at runtime | Detected at schema registration |
| Version evolution | Hard | Built-in compatibility modes |

**In this project:**

```
src/main/avro/Event.avsc:
{
  "type": "record",
  "name": "Event",
  "fields": [
    {"name": "eventId", "type": "string"},
    {"name": "eventType", "type": "string"},
    {"name": "payload", "type": ["null", "string"], "default": null},
    ...
  ]
}
```

Avro Maven plugin generates:
```
target/generated-sources/avro/com/demo/kafka/avro/
  ├── AvroEvent.java (generated)
  └── ErrorType.java (generated)
```

---

## 7. 🔴 What is Schema Registry?

**Q:** What is Schema Registry and how does it work with Kafka?

**Answer:**

**Schema Registry** is a **centralized repository** for managing Avro schemas. It:
- **Stores** schema versions
- **Validates** compatibility between versions
- **Enables** producers/consumers to evolve independently

**How it works:**

**Producer sends Avro message:**
```
1. Producer creates AvroEvent object
2. KafkaAvroSerializer.serialize()
3. Contacts Schema Registry: "Register schema for avro-events-topic-value"
4. Registry checks compatibility
5. Returns schema ID
6. Serializes AvroEvent to binary bytes + schema ID
7. Sends to Kafka
```

**Consumer reads Avro message:**
```
1. Consumer receives bytes + schema ID from Kafka
2. KafkaAvroDeserializer.deserialize()
3. Contacts Schema Registry: "Get schema with ID 1"
4. Registry returns schema
5. Deserializes bytes → AvroEvent object using schema
6. Hands to @KafkaListener
```

**In this project:**

```yaml
schema-registry:
  image: confluentinc/cp-schema-registry:7.5.0
  environment:
    SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    SCHEMA_REGISTRY_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    SCHEMA_REGISTRY_DEBUG: "true"
  ports:
    - "8081:8081"
```

**API endpoints:**
```bash
# List all subjects (topics with schemas)
curl -s http://localhost:8081/subjects

# Get latest version of avro-events-topic schema
curl -s http://localhost:8081/subjects/avro-events-topic-value/versions/latest

# Get schema versions history
curl -s http://localhost:8081/subjects/avro-events-topic-value/versions
```

---

## 8. 🔴 What is Schema Compatibility?

**Q:** What are the different schema compatibility modes? How do they work?

**Answer:**

**Compatibility mode** determines if **new schemas can coexist** with old ones.

**Modes:**

| Mode | Producer | Consumer | Rule | Use Case |
|------|----------|----------|------|----------|
| `BACKWARD` | ✅ Any | 🔴 Old only | New schema reads old data | Default — safe for consumers |
| `FORWARD` | 🔴 Old only | ✅ Any | Old schema reads new data | Safe for producers |
| `FULL` | ✅ Any | ✅ Any | Both directions work | Safest but most restrictive |
| `NONE` | ✅ Any | ✅ Any | No validation | Dangerous — breaks easily |

**BACKWARD (default):**
```
Old message: {eventId: "evt-001", eventType: "ORDER"}
New schema:  {eventId, eventType, timestamp}

New consumer with new schema can read old messages
(timestamp is optional with default value)
```

**FORWARD:**
```
Old message: {eventId: "evt-001", eventType: "ORDER", timestamp: "2024-01-01"}
Old schema:  {eventId, eventType}

Old consumer with old schema can read new messages
(ignores unknown timestamp field)
```

**FULL:**
```
Both BACKWARD + FORWARD work
= Safe for both old/new producers and consumers
```

**Check compatibility in Schema Registry:**
```bash
curl -s http://localhost:8081/config
# Returns: {"compatibilityLevel":"BACKWARD"}
```

---

## 9. 🔴 When Does Schema Validation Happen?

**Q:** When are Avro messages validated against the schema?

**Answer:**

**Producer side (before Kafka):**
```java
AvroEvent event = new AvroEvent();
event.setEventId("evt-001");
// Missing required field "eventType"!

kafkaTemplate.send(avroTopic, event);
// KafkaAvroSerializer.serialize() called
// Contacts Schema Registry
// Validates: eventType is required
// SerializationException thrown ← Message NEVER reaches Kafka
```

**Consumer side (after Kafka):**
```java
// Bytes received from Kafka partition
byte[] bytes = {0xAB, 0xCD, ...};

KafkaAvroDeserializer.deserialize()
// Contacts Schema Registry
// Fetches writer schema
// Validates bytes against schema
// Deserializes to AvroEvent
// Hands to @KafkaListener (safe to use)
```

**Key difference:**
- **Producer**: Validation is **blocking** — bad message rejected before send
- **Consumer**: Validation is **informational** — ensures compatibility

**In this project, validation occurs on first send:**

```bash
# First Avro send
curl -s -X POST "http://localhost:8080/api/kafka/avro/events?eventType=ORDER_CREATED"

# 1. AvroEventProducer.sendEvent()
# 2. KafkaAvroSerializer validates against schema
# 3. If valid: serializes to binary + registers schema
# 4. If invalid: SerializationException (message never reaches broker)

# Check registered schema
curl -s http://localhost:8081/subjects/avro-events-topic-value/versions/latest | jq
```

---

## 10. 🔴 What Happens If a Consumer Crashes During DLT Processing?

**Q:** Design a resilient DLT processing strategy.

**Answer:**

**Problem:** DLT consumer crashes while processing a message → message re-read on restart → potential reprocessing.

**Solution: Resilient DLT Processing**

```java
@KafkaListener(topics = "${kafka.topics.dlt}", groupId = "dlt-consumer-group")
public void consumeDlt(
        @Payload Event event,
        @Header(KafkaHeaders.OFFSET) long offset,
        Acknowledgment ack) {
    
    String messageId = event.getEventId();
    
    try {
        // 1. Check if already processed (idempotency)
        if (dltProcessedRepository.exists(messageId)) {
            log.info("Message {} already processed, skipping", messageId);
            ack.acknowledge();  // Commit offset
            return;
        }
        
        // 2. Start transaction (if using database)
        // Everything below is transactional
        
        // 3. Record that we're processing
        dltProcessedRepository.markProcessing(messageId);
        
        // 4. Do the work (alert ops, save for review, etc.)
        alertOperationsTeam(event);
        dltMessageRepository.save(event);  // Audit trail
        
        // 5. Mark as completed
        dltProcessedRepository.markCompleted(messageId);
        
        // 6. Commit offset (after all work done)
        ack.acknowledge();
        log.info("DLT message {} processed successfully", messageId);
        
    } catch (Exception e) {
        log.error("Error processing DLT message {}: {}", messageId, e.getMessage());
        
        // Don't acknowledge → offset not committed
        // Message will be reprocessed on restart
        // Idempotency check (step 1) prevents double-processing
        
        throw e;  // Let error handler decide (might go to another DLT!)
    }
}
```

**Database schema (tracking):**
```sql
CREATE TABLE dlt_processed (
    event_id VARCHAR(255) PRIMARY KEY,
    status ENUM('PROCESSING', 'COMPLETED'),
    processed_at TIMESTAMP,
    error_message TEXT
);
```

---

## 11. 🔴 How Do You Test Error Handling?

**Q:** How do you test error handling and DLT flow?

**Answer:**

**Unit tests (mock dependencies):**

```java
@SpringBootTest
class ManualAckConsumerTest {
    
    @Mock private EventProcessingService processingService;
    @Mock private Acknowledgment ack;
    @InjectMocks private ManualAckConsumer consumer;
    
    @Test
    void should_acknowledge_on_success() {
        Event event = createTestEvent();
        consumer.consume(event, 0, ack);
        
        verify(processingService).processEvent(event);
        verify(ack).acknowledge();
    }
    
    @Test
    void should_not_acknowledge_on_error() {
        Event event = createTestEvent();
        doThrow(new RuntimeException("Processing failed"))
            .when(processingService).processEvent(event);
        
        assertThrows(RuntimeException.class, 
            () -> consumer.consume(event, 0, ack));
        verify(ack, never()).acknowledge();
    }
}
```

**Integration tests (with Kafka):**

```java
@SpringBootTest
class DLTIntegrationTest {
    
    @Autowired private KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired private DeadLetterConsumer dltConsumer;
    @Spy private List<Event> dltMessages = new ArrayList<>();
    
    @Test
    void should_send_to_dlt_after_retries() {
        Event failingEvent = Event.builder()
            .eventId("fail-001")
            .simulateError(true)
            .errorType(Event.ErrorType.PERMANENT)
            .build();
        
        kafkaTemplate.send("events-topic", failingEvent);
        
        // Wait for retries + DLT send
        Thread.sleep(10_000);
        
        // Assert message appeared in DLT
        assertTrue(dltMessages.stream()
            .anyMatch(e -> e.getEventId().equals("fail-001")));
    }
}
```

**End-to-end tests (via REST):**

```bash
# 1. Send event that will fail
curl -s -X POST "http://localhost:8080/api/kafka/events/test?eventType=ORDER_FAILED&simulateError=true&errorType=PERMANENT" | jq

# 2. Wait for retries (~7 seconds: 1s + 2s + 4s)
sleep 8

# 3. Check DLT via Kafka-UI or query
curl -s -X GET "http://localhost:8090/api/topics/events-topic.DLT/messages" | jq
```

---

## 12. 🔴 What's the Difference Between Retry and Resubmit?

**Q:** When should you retry a failed message vs resubmit it?

**Answer:**

| Aspect | Retry | Resubmit |
|--------|-------|----------|
| **When** | Transient error (timeout, network) | Permanent error (validation, schema) |
| **How** | DefaultErrorHandler + ExponentialBackOff | DLT consumer sends corrected message back to main topic |
| **Flow** | Same message, same consumer | Same or different message, possibly different processing |
| **Use case** | Recover from temporary failures | Fix & reprocess after manual intervention |

**Retry (this project):**
```java
// Message processing fails due to DB timeout
catch (DataAccessException e) {
    throw e;  // DefaultErrorHandler retries 1s→2s→4s
}
```

**Resubmit (manual recovery):**
```java
// DLT consumer discovers the issue
@KafkaListener(topics = "events-topic.DLT")
public void processDlt(Event event, @Headers Map<String, Object> headers) {
    String error = (String) headers.get("dlt-exception-message");
    
    if (error.contains("Validation")) {
        // Operator manually fixes the event
        Event corrected = fixEvent(event);
        
        // Resubmit to main topic (not DLT)
        kafkaTemplate.send("events-topic", corrected.getEventId(), corrected);
        log.info("Resubmitted corrected event {}", corrected.getEventId());
    }
}
```

**Flow comparison:**

```
Retry:
  Main topic
    ↓ (error)
  Retry 1 (1s wait) ↓
  Retry 2 (2s wait) ↓
  Retry 3 (4s wait) ↓ (still fails)
  DLT

Resubmit:
  Main topic
    ↓ (error)
  DLT (manual fix)
    ↓ (corrected)
  Main topic (reprocessed)
    ↓
  Success
```

---

## Summary Table

| Concept | Purpose |
|---------|---------|
| **DLT** | Capture permanently failed messages |
| **Transient vs Permanent** | Decide retry vs DLT |
| **DLT headers** | Metadata for debugging (original topic, offset, exception) |
| **Idempotent processing** | Prevent duplicates if DLT consumer restarts |
| **Avro** | Schema-based binary serialization |
| **Schema Registry** | Centralized schema versioning & validation |
| **Compatibility modes** | Control how schemas can evolve (BACKWARD, FORWARD, FULL) |
| **Producer validation** | Schemas validated before sending to Kafka |
| **Consumer validation** | Schemas validated when reading from Kafka |
| **Resilient DLT processing** | Track processed messages, handle crashes gracefully |
| **Error testing** | Unit + integration + E2E tests |
| **Retry vs Resubmit** | Transient errors retry; permanent errors go to DLT for manual fix |

---

## Next Steps

- **Labs 04–06**: Test error handling, DLT, and Avro hands-on
- **Scenario Questions**: [Scenario & System Design (04)](04-scenario-questions.md)
- **Theory Deep Dive**: [Error Handling & Dead Letter Topics (05)](../theory/05-error-handling-and-dlt.md)


