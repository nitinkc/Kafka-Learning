package com.demo.kafka.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CustomErrorHandler {

    public enum ErrorClassification {
        TRANSIENT, PERMANENT, DESERIALIZATION, VALIDATION
    }

    public static class TransientException extends RuntimeException {
        public TransientException(String message) { super(message); }
    }

    public static class ValidationException extends RuntimeException {
        public ValidationException(String message) { super(message); }
    }
}
