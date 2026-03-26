package com.demo.kafka.service;

import com.demo.kafka.handler.CustomErrorHandler;
import com.demo.kafka.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EventProcessingService {

    public void processEvent(Event event) {
        log.info("Processing event: {} - Type: {}", event.getEventId(), event.getEventType());
        if (event.isSimulateError()) {
            switch (event.getErrorType()) {
                case TRANSIENT -> throw new CustomErrorHandler.TransientException("Transient error");
                case PERMANENT -> throw new RuntimeException("Permanent error");
                case VALIDATION -> throw new CustomErrorHandler.ValidationException("Validation error");
                default -> {}
            }
        }
        log.info("Event processed: {}", event.getEventId());
    }
}
