package com.demo.kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class DeadLetterConsumer {

    private final KafkaAdmin kafkaAdmin;

    @KafkaListener(topics = "${kafka.topics.dlt}", groupId = "dlt-consumer-group")
    public void consumeDeadLetter(
            ConsumerRecord<String, Object> consumerRecord,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
            Acknowledgment ack) {
        log.error("=== DEAD LETTER RECEIVED ===");
        log.error("Original Topic: {}", originalTopic);
        log.error("Error: {}", exceptionMessage);
        log.error("Payload: {}", consumerRecord.value());

        // Acknowledge the message first to advance the consumer offset
        ack.acknowledge();

        // Delete the message by truncating records up to (offset + 1) on this partition
        deleteMessage(consumerRecord);
    }

    private void deleteMessage(ConsumerRecord<String, Object> consumerRecord) {
        TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
        // deleteRecords removes all records with offset < beforeOffset, so offset+1 deletes this record
        long beforeOffset = consumerRecord.offset() + 1;
        Map<TopicPartition, RecordsToDelete> recordsToDelete =
                Map.of(topicPartition, RecordsToDelete.beforeOffset(beforeOffset));

        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            adminClient.deleteRecords(recordsToDelete)
                    .all()
                    .get();
            log.info("Deleted DLT message at topic={} partition={} offset={}",
                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while deleting DLT message at topic={} partition={} offset={}",
                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), e);
        } catch (Exception e) {
            log.error("Failed to delete DLT message at topic={} partition={} offset={}",
                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), e);
        }
    }
}


