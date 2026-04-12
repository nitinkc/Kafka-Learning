# 02 · Spring Kafka Setup

!!! abstract "Learning Objectives"
    - Add the correct Spring Kafka dependencies
    - Configure `application.properties` for broker & schema registry
    - Understand `@EnableKafka`, `KafkaTemplate`, and `@KafkaListener`
    - Wire producer and consumer factories in `KafkaConfig`

---

## Maven Dependencies

```xml title="pom.xml (relevant section)"
<!-- Spring Kafka -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>

<!-- Confluent Avro serialiser / Schema Registry client -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.5.0</version>
</dependency>

<!-- Avro runtime -->
<dependency>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro</artifactId>
    <version>1.11.3</version>
</dependency>
```

The Confluent dependency requires their Maven repository:
```xml
<repository>
    <id>confluent</id>
    <url>https://packages.confluent.io/maven/</url>
</repository>
```

---

## Application Properties

```properties title="src/main/resources/application.properties"
spring.application.name=kafka-offset-demo

# Kafka broker
spring.kafka.bootstrap-servers=localhost:9092

# Topic names
kafka.topics.main=events-topic
kafka.topics.dlt=events-topic.DLT
kafka.topics.avro=avro-events-topic

# Confluent Schema Registry
kafka.schema-registry.url=http://localhost:8081
```

!!! warning "All four kafka properties are required"
    If any of `kafka.topics.main`, `kafka.topics.dlt`, `kafka.topics.avro`, or `kafka.schema-registry.url` are absent, Spring will throw `IllegalArgumentException: Could not resolve placeholder` at startup.

---

## The `@EnableKafka` Annotation

```java title="KafkaConfig.java"
@Configuration
@EnableKafka  // (1)!
@Slf4j
public class KafkaConfig {
    // ...
}
```

1. Activates detection of `@KafkaListener` annotations on Spring beans. Without this, listener methods will never fire.

`@EnableKafka` is equivalent to adding `spring.kafka.listener.type=...` in properties but is more explicit in configuration classes.

---

## KafkaTemplate — The Producer API

`KafkaTemplate<K, V>` is the primary Spring abstraction for **sending messages**:

```java title="KafkaConfig.java — JSON producer factory"
@Bean
public ProducerFactory<String, Object> producerFactory(ObjectMapper kafkaObjectMapper) {
    Serializer<Object> valueSerializer = new Serializer<>() {
        @Override
        public byte[] serialize(String topic, Object data) {
            return kafkaObjectMapper.writeValueAsBytes(data); // (1)!
        }
    };
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    return new DefaultKafkaProducerFactory<>(props,
            new StringSerializer(), valueSerializer); // (2)!
}

@Bean
public KafkaTemplate<String, Object> kafkaTemplate(
        ProducerFactory<String, Object> producerFactory) {
    return new KafkaTemplate<>(producerFactory); // (3)!
}
```

1. Custom Jackson-based value serialiser converts any Java object → JSON bytes.
2. Key serialiser is always `StringSerializer`; value serialiser is our custom one.
3. `KafkaTemplate` wraps the `ProducerFactory` and adds Spring-friendly `send()` methods.

Usage in `EventProducer`:
```java
kafkaTemplate.send(mainTopic, event.getEventId(), event);
//                   topic       key (for partitioning)  value
```

---

## ConcurrentKafkaListenerContainerFactory — The Consumer API

The container factory creates and manages `@KafkaListener` containers:

```java title="KafkaConfig.java — JSON consumer factory"
@Bean
public ConsumerFactory<String, Event> consumerFactory(ObjectMapper kafkaObjectMapper) {
    Deserializer<Event> valueDeserializer = new Deserializer<>() {
        @Override
        public Event deserialize(String topic, byte[] data) {
            return kafkaObjectMapper.readValue(data, Event.class); // (1)!
        }
    };
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");   // (2)!
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);     // (3)!
    return new DefaultKafkaConsumerFactory<>(props,
            new StringDeserializer(), valueDeserializer);
}

@Bean
public ConcurrentKafkaListenerContainerFactory<String, Event> kafkaListenerContainerFactory(
        ConsumerFactory<String, Event> consumerFactory,
        DefaultErrorHandler errorHandler) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, Event>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties()
           .setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE); // (4)!
    factory.setCommonErrorHandler(errorHandler); // (5)!
    return factory;
}
```

1. Jackson deserialises the raw bytes back to our `Event` POJO.
2. `"latest"` — new consumer group starts reading from the newest messages.
3. Disable Kafka's auto-commit so Spring controls exactly when offsets are committed.
4. `MANUAL_IMMEDIATE` — offset committed immediately when `ack.acknowledge()` is called.
5. Wires the DLT + retry error handler to every listener using this factory.

---

## @KafkaListener

The `@KafkaListener` annotation turns a method into a Kafka message handler:

```java
@KafkaListener(
    topics = "${kafka.topics.main}",   // (1)!
    groupId = "manual-ack-group"       // (2)!
)
public void consume(
        @Payload Event event,          // (3)!
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition, // (4)!
        @Header(KafkaHeaders.OFFSET) long offset,
        Acknowledgment ack) {          // (5)!
    // ...
}
```

1. Topic name resolved from properties — `events-topic`.
2. Consumer group ID — each group tracks offsets independently.
3. `@Payload` binds the message value, deserialised by the factory's deserialiser.
4. Kafka metadata headers (partition number, offset, key, topic…) are accessible via `@Header`.
5. `Acknowledgment` is injected by Spring when `AckMode.MANUAL_IMMEDIATE` is set.

---

## Topic Auto-Creation with `NewTopic`

```java title="KafkaConfig.java — topic beans"
@Bean
public NewTopic mainTopic() {
    return TopicBuilder.name(mainTopic)
            .partitions(3)
            .replicas(1)
            .build();
}

@Bean
public NewTopic deadLetterTopic() {
    return TopicBuilder.name(dltTopic)
            .partitions(1)
            .replicas(1)
            .build();
}
```

Spring's `KafkaAdmin` bean (auto-configured) detects `NewTopic` beans and creates them on the broker **at startup** — but only if Kafka is reachable. This is why the app fails if Kafka isn't ready first.

---

## ObjectMapper Configuration

The project uses a dedicated Jackson `ObjectMapper` for Kafka (not the default Spring MVC one) to handle Java 8 time types:

```java
@Bean(name = "kafkaObjectMapper")
public ObjectMapper kafkaObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());                        // (1)!
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);    // (2)!
    return mapper;
}
```

1. Enables serialisation of `LocalDateTime`, `Instant`, etc.
2. Writes dates as ISO-8601 strings (`"2026-04-11T10:00:00"`) not epoch numbers.

---

## Key Takeaways

!!! success "What to remember"
    1. `spring-kafka` + `kafka-avro-serializer` are the two key dependencies
    2. `@EnableKafka` activates listener detection
    3. Each serialisation format (JSON, Avro) needs its **own `ProducerFactory`** and **`ConsumerFactory`**
    4. `ConcurrentKafkaListenerContainerFactory` ties together the consumer factory, ack mode, and error handler
    5. `NewTopic` beans auto-create topics — they require Kafka to be up first

---

## Up Next

➡️ [03 · Producers & Consumers](03-producers-and-consumers.md)

**Hands-on now?** → [Lab 02 · First Producer & Consumer](../labs/lab-02-first-producer-consumer.md)

