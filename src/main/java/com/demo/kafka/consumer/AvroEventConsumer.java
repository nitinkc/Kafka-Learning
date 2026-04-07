package com.demo.kafka.consumer;

import com.demo.kafka.avro.AvroEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Listens on the Avro topic using the {@code avroKafkaListenerContainerFactory}
 * (configured in {@link com.demo.kafka.config.KafkaConfig}).
 *
 * <p>The {@code KafkaAvroDeserializer} fetches the writer schema from the Schema Registry
 * and validates/converts the binary Avro bytes into a strongly-typed {@link AvroEvent}
 * before this method is called. Any schema mismatch raises a
 * {@code SerializationException} and the message goes to the DLT.
 */
@Component
@Slf4j
public class AvroEventConsumer {

    @KafkaListener(
            topics = "${kafka.topics.avro}",
            groupId = "avro-consumer-group",
            containerFactory = "avroKafkaListenerContainerFactory"
    )
    public void consume(
            @Payload AvroEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("[AVRO] Received – Partition={}, Offset={}, EventId={}, EventType={}",
                partition, offset, event.getEventId(), event.getEventType());
        log.info("[AVRO] Payload='{}', SimulateError={}, ErrorType={}",
                event.getPayload(), event.getSimulateError(), event.getErrorType());

        // Business logic would go here
        ack.acknowledge();
        log.info("[AVRO] Acknowledged offset={}", offset);
    }
}

