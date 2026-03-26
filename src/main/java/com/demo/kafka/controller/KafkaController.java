package com.demo.kafka.controller;

import com.demo.kafka.model.Event;
import com.demo.kafka.producer.EventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaController {
    private final EventProducer eventProducer;

    @PostMapping("/events")
    public ResponseEntity<Event> sendEvent(@RequestBody Event event) {
        return ResponseEntity.ok(eventProducer.sendEvent(event));
    }

    @PostMapping("/events/test")
    public ResponseEntity<Event> sendTestEvent(
            @RequestParam String eventType,
            @RequestParam(defaultValue = "false") boolean simulateError,
            @RequestParam(defaultValue = "NONE") Event.ErrorType errorType) {
        return ResponseEntity.ok(eventProducer.createAndSendTestEvent(eventType, simulateError, errorType));
    }
}
