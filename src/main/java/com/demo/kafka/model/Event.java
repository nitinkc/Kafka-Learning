package com.demo.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    private String eventId;
    private String eventType;
    private String payload;
    private LocalDateTime timestamp;
    private boolean simulateError;
    private ErrorType errorType;
    
    public enum ErrorType {
        NONE, TRANSIENT, PERMANENT, DESERIALIZATION, VALIDATION
    }
}
