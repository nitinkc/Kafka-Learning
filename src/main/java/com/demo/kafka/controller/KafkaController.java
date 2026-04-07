package com.demo.kafka.controller;

import com.demo.kafka.avro.AvroEvent;
import com.demo.kafka.avro.ErrorType;
import com.demo.kafka.model.Event;
import com.demo.kafka.producer.AvroEventProducer;
import com.demo.kafka.producer.EventProducer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaController {

    private final EventProducer eventProducer;
    private final AvroEventProducer avroEventProducer;

    // ── Existing JSON endpoints ───────────────────────────────────────────────

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

    // ── Avro endpoints (Schema Registry validated) ───────────────────────────

    /**
     * Send a valid Avro event. The KafkaAvroSerializer will:
     *  1. Register the schema in the Schema Registry (first time only).
     *  2. Validate the AvroEvent object against the schema.
     *  3. Serialize to binary Avro and send to Kafka.
     *
     * Try: POST /api/kafka/avro/events?eventType=ORDER_CREATED&payload=hello
     */
    @PostMapping("/avro/events")
    public ResponseEntity<?> sendAvroEvent(
            @RequestParam String eventType,
            @RequestParam(required = false) String payload,
            @RequestParam(defaultValue = "false") boolean simulateError,
            @RequestParam(defaultValue = "NONE") ErrorType errorType) {
        try {
            AvroEvent sent = avroEventProducer.createAndSendAvroEvent(
                    eventType, payload, simulateError, errorType);
            return ResponseEntity.ok(Map.of(
                    "status", "ACCEPTED",
                    "eventId", sent.getEventId(),
                    "eventType", sent.getEventType(),
                    "topic", "avro-events-topic",
                    "schemaValidation", "PASSED – KafkaAvroSerializer validated against Schema Registry"
            ));
        } catch (SerializationException ex) {
            // This is thrown BEFORE the message reaches Kafka when schema validation fails
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                    "status", "REJECTED",
                    "reason", "Schema validation failed at producer",
                    "detail", ex.getMessage()
            ));
        }
    }
}

