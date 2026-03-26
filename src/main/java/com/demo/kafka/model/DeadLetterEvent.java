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
public class DeadLetterEvent {
    private String originalTopic;
    private Integer originalPartition;
    private Long originalOffset;
    private String originalKey;
    private Object originalPayload;
    private String errorMessage;
    private String errorClassName;
    private String stackTrace;
    private LocalDateTime failedAt;
    private Integer retryCount;
    private String consumerGroup;
}
