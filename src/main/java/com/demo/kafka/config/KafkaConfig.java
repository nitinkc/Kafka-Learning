package com.demo.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafka
@Slf4j
public class KafkaConfig {

    @Value("${kafka.topics.main}")
    private String mainTopic;

    @Value("${kafka.topics.dlt}")
    private String dltTopic;

    @Bean
    public NewTopic mainTopic() {
        return TopicBuilder.name(mainTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(dltTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new org.apache.kafka.common.TopicPartition(dltTopic, 0));
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
