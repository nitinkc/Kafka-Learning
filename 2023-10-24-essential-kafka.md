---
title: Kafka Essentials
date: 2023-10-24 23:41:00
categories:
- Microservices
tags:
- Real-time
---

Kafka can be used in both messaging and event-driven architectures, but the
terminology and concepts fit both scenarios

[Distributed Messaging]({% post_url /systems_design/2024-06-13-Distributed-messaging %})

[course: Apache Kafka 101](https://developer.confluent.io/courses/apache-kafka/events/)
# 4 actors of Messaging
> **Producer** --> Sends message --> to an **exchange** --> Routed to --> **Queue** --> Delivered to --> a **Consumer**

# Kafka Terminology

- **Producer**: A producer is a client that sends messages to the Kafka server
  to the specified topic.
    - Sends messages to Kafka topics. In an event-driven architecture, a
      producer might publish events (e.g., "OrderPlaced"), while in a messaging
      architecture, it might send messages with specific instructions (e.g., "
      ProcessOrder").
    - [Diff between Event and Message]({% post_url /systems_design/2024-06-13-Distributed-messaging %}#messages-vs-events)
- **Consumer**: Consumers are the recipients who receive messages from the Kafka
  server.
    - Receives messages from Kafka topics. Consumers process the data or events
      they receive. In an event-driven architecture, consumers react to events,
      while in a messaging system, consumers handle specific instructions or
      requests.
- **Broker**: A Kafka broker is a server that stores and serves messages. Also handles
  the distribution of messages across partitions.
  - Brokers can **create a Kafka cluster** by sharing information using
    Zookeeper. 
  - A broker **receives messages from producers** and consumers fetch
    messages from the broker **by topic, partition, and offset**.
    
- **Cluster**: Kafka is a distributed system. A Kafka cluster contains multiple
  brokers sharing the workload.
    - A Kafka cluster is a group of brokers working together to provide fault
      tolerance and scalability. Both messaging and event-driven systems benefit
      from Kafka's distributed nature.
- **Topic**: A topic is a category name to which messages are published and from
  which consumers can receive messages.
    - Topics are categories or channels to which messages are published. Topics
      can be used for both event streams (e.g., "UserActivity") and messaging
      queues (e.g., "OrderRequests").
- **Partition**: Messages published to a topic are spread across a Kafka cluster
  into several partitions. Each partition can be associated with a broker to
  allow consumers to read from a topic in parallel.
    - Topics are divided into partitions to allow parallel processing and
      scalability. Partitions enable Kafka to handle high-throughput messaging
      and event streaming.
- **Offset**: Offset is a pointer to the last message that Kafka has already
  sent to a consumer.
  -Each message in a partition has a unique offset. This allows consumers to
  keep track of which messages they have processed, which is useful for both
  event processing and message handling.

In an **event-driven system**, Kafka's role is **to broadcast events to multiple
consumers**, often with the assumption that consumers might process these events
independently.

In a **messaging system**, Kafka serves more as a traditional queue where
messages are consumed and processed, potentially requiring acknowledgments or
responses.

## Kafka Message Processing
Messages are stores as **LOGS** & are **immutable**.

In Kafka, messages are not "queued" in the traditional sense.
Instead, they are **published to Kafka topics** and stored in **log files**(flat files) within Kafka
brokers.

Kafka uses a distributed commit log to store messages, and consumers read
messages from these log files.

**Producers Publish Messages**: Producers send messages to Kafka topics. Kafka
appends these messages to the end of the topic's log.

**Messages are Retained**: Messages in Kafka logs are **retained** for **a configurable
period**, even after consumers have read them. This retention period is typically
set by the Kafka configuration.

**Consumers Read Messages**: Consumers(multiple) subscribe to topics and read messages from**Kafka logs**. They maintain an offset to keep track of which messages they have
consumed.

**Parallel Processing**: Multiple consumers can read messages from the same topic in
parallel. Each message is read by only one consumer within a consumer group.

**Offset Management**: Kafka keeps track of the offset (position) of each consumer
within a topic. This allows consumers to resume reading from where they left
off, making Kafka a distributed message processing system.

Kafka is a **distributed streaming** platform, designed for high-throughput,
fault-tolerant, and distributed data streaming. While messages are not queued in
the traditional sense, Kafka's design allows for efficient and scalable message
processing.

Messages are not removed immediately after being consumed, but they will
eventually be **deleted** based on the **retention policy**. The retention policy
ensures that Kafka can handle large volumes of data while allowing consumers to
catch up on messages they might have missed.

### Test Locally

Download kafka : [https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)

Run the following commands

```shell
cd /Applications/Development/kafka_2.13-3.6.0
bin/zookeeper-server-start.sh config/zookeeper.properties
```

In another tab

```shell
cd /Applications/Development/kafka_2.13-3.6.0
bin/kafka-server-start.sh config/server.properties
```

## Conduktor
Download and install [https://docs.conduktor.io/gateway/](https://docs.conduktor.io/gateway/)

Run once the kafka server is running on local

## Naming conventions
{consuming system}-{kafka topic name}-{consuming service}-group

`register-reg.student.engineering-hostelservice-group`

## Interview Questions

### Explain the use case you are currently being used and explain how Kafka message failures or schema validations were being handled in your use case?
**Example use case:** order events from a checkout service to downstream billing, fulfillment, and email services.

**Handling schema validation and failures:**
- Validate payloads against an Avro/JSON Schema in the producer **before publish** and in the consumer before process.
- Use Schema Registry with compatibility checks to prevent breaking changes.
- On validation failure, route to a Dead Letter Topic (DLT) with a reason header and the original payload.
- Track DLT volume in monitoring; set alert thresholds.

### Gave an use case where message or communication was being failed and asked about what happens then?
If a consumer cannot process a message:
- It **retries with backoff** for transient errors (timeouts, 5xx).
- After max retries, it writes to DLT and commits the offset to avoid a stuck partition.
- For non-retryable errors (validation, contract violation), it goes directly to DLT.
- A replay process or support workflow handles DLT items.

### Did you use Dead letter topic?
Yes. DLT is essential for isolating poison messages and keeping the main topic flowing. Each DLT message includes:
- Original topic/partition/offset.
- Error type and error message.
- Correlation or trace id.

### How do you notify support or upstream teams when message processing fails?
- Alerts from metrics dashboards (DLT count, consumer error rate, lag).
- Ticket creation or Slack/Teams notifications from alert rules.
- If the issue is upstream contract violation, send a message with sample payload and schema version.
