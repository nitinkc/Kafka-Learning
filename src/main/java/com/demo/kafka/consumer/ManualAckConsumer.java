package com.demo.kafka.consumer;

import com.demo.kafka.handler.CustomErrorHandler;
import com.demo.kafka.model.Event;
import com.demo.kafka.service.EventProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class ManualAckConsumer {
    private final EventProcessingService processingService;

    @KafkaListener(topics = "${kafka.topics.main}", groupId = "manual-ack-group")
    public void consume(
            @Payload Event event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {
        log.info("Received - Partition: {}, Offset: {}, EventId: {}", partition, offset, event.getEventId());
        try {
            processingService.processEvent(event);
            ack.acknowledge();
            log.info("Acknowledged offset: {}", offset);
        } catch (CustomErrorHandler.TransientException e) {
            log.warn("Transient error, will retry. Offset: {}", offset);
            throw e;
        } catch (Exception e) {
            log.error("Error at offset {}: {}", offset, e.getMessage());
            throw e;
        }
    }
}
