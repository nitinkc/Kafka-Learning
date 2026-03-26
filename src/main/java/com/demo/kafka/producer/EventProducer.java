package com.demo.kafka.producer;

import com.demo.kafka.model.Event;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class EventProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Value("${kafka.topics.main}")
    private String mainTopic;

    public Event sendEvent(Event event) {
        kafkaTemplate.send(mainTopic, event.getEventId(), event);
        log.info("Sent event: {}", event.getEventId());
        return event;
    }

    public Event createAndSendTestEvent(String eventType, boolean simulateError, Event.ErrorType errorType) {
        Event event = Event.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(eventType)
                .payload("Test payload")
                .timestamp(LocalDateTime.now())
                .simulateError(simulateError)
                .errorType(errorType)
                .build();
        return sendEvent(event);
    }
}
