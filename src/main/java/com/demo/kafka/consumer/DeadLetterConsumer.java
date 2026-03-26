package com.demo.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DeadLetterConsumer {

    @KafkaListener(topics = "${kafka.topics.dlt}", groupId = "dlt-consumer-group")
    public void consumeDeadLetter(
            ConsumerRecord<String, Object> record,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
            Acknowledgment ack) {
        log.error("=== DEAD LETTER RECEIVED ===");
        log.error("Original Topic: {}", originalTopic);
        log.error("Error: {}", exceptionMessage);
        log.error("Payload: {}", record.value());
        ack.acknowledge();
    }
}
