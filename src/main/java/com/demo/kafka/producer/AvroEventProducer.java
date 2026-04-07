package com.demo.kafka.producer;

import com.demo.kafka.avro.AvroEvent;
import com.demo.kafka.avro.ErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * Avro-based Kafka producer.
 *
 * <p>How schema validation works here:
 * <ol>
 *   <li>We build an {@link AvroEvent} – a class generated from {@code Event.avsc}.</li>
 *   <li>On {@code kafkaTemplate.send()}, the {@code KafkaAvroSerializer} serialises the
 *       object to binary Avro format AND registers / validates the schema against the
 *       Confluent Schema Registry at {@code kafka.schema-registry.url}.</li>
 *   <li>If the object does not conform to the registered schema (e.g. a required field is
 *       missing or has the wrong type), a {@code SerializationException} is thrown
 *       <em>before</em> the message ever reaches the Kafka broker.</li>
 * </ol>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AvroEventProducer {

    // Injected by KafkaConfig – uses KafkaAvroSerializer
    private final KafkaTemplate<String, SpecificRecord> avroKafkaTemplate;

    @Value("${kafka.topics.avro}")
    private String avroTopic;

    /**
     * Send an already-built {@link AvroEvent}.
     * The KafkaAvroSerializer will validate against the Schema Registry on send.
     */
    public AvroEvent sendAvroEvent(AvroEvent event) {
        log.info("[AVRO] Sending event id={} type={}", event.getEventId(), event.getEventType());
        avroKafkaTemplate.send(avroTopic, event.getEventId(), event);
        log.info("[AVRO] Event accepted by Schema Registry and sent to topic '{}'", avroTopic);
        return event;
    }

    /**
     * Convenience factory – builds an {@link AvroEvent} and sends it.
     */
    public AvroEvent createAndSendAvroEvent(String eventType,
                                             String payload,
                                             boolean simulateError,
                                             ErrorType errorType) {
        AvroEvent event = AvroEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(eventType)
                .setPayload(payload)
                .setTimestamp(Instant.now())
                .setSimulateError(simulateError)
                .setErrorType(errorType == null ? ErrorType.NONE : errorType)
                .build();
        return sendAvroEvent(event);
    }
}

