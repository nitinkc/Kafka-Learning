# Theory — Learning Path

Progress through these modules **in order**. Each builds on the previous one.

| Module | Title | Key Concepts |
|--------|-------|-------------|
| [01](01-kafka-fundamentals.md) | Kafka Fundamentals | Broker, Topic, Partition, Offset, Log, Retention |
| [02](02-spring-kafka-setup.md) | Spring Kafka Setup | `KafkaTemplate`, `@KafkaListener`, `KafkaConfig` |
| [03](03-producers-and-consumers.md) | Producers & Consumers | `EventProducer`, `ManualAckConsumer`, `AckMode` |
| [04](04-consumer-groups-and-offsets.md) | Consumer Groups & Offsets | Rebalancing, `ENABLE_AUTO_COMMIT`, offset strategies |
| [05](05-error-handling-and-dlt.md) | Error Handling & DLT | `DefaultErrorHandler`, exponential backoff, DLT |
| [06](06-avro-and-schema-registry.md) | Avro & Schema Registry | `.avsc`, code-gen, `KafkaAvroSerializer`, compatibility |
| [07](07-advanced-patterns.md) | Advanced Patterns | Multi-factory, idempotent producers, event-driven arch |

!!! tip "Pair with Labs"
    Every theory module has a **corresponding lab** in the [Labs](../labs/index.md) section.

