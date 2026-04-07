package com.demo.kafka.config;

import com.demo.kafka.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class KafkaConfig {

    @Value("${kafka.topics.main}")
    private String mainTopic;

    @Value("${kafka.topics.dlt}")
    private String dltTopic;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ── Jackson 2.x ObjectMapper ─────────────────────────────────────────────
    // spring-kafka 4.0 dropped its Jackson 2.x JsonSerializer/JsonDeserializer.
    // We implement our own inline using this mapper.
    @Bean(name = "kafkaObjectMapper")
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    // ── Producer ─────────────────────────────────────────────────────────────
    @Bean
    public ProducerFactory<String, Object> producerFactory(ObjectMapper kafkaObjectMapper) {
        Serializer<Object> valueSerializer = new Serializer<>() {
            @Override
            public byte[] serialize(String topic, Object data) {
                if (data == null) return null;
                try {
                    return kafkaObjectMapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new SerializationException("Error serializing JSON", e);
                }
            }
        };

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), valueSerializer);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    // ── Consumer ─────────────────────────────────────────────────────────────
    @Bean
    public ConsumerFactory<String, Event> consumerFactory(ObjectMapper kafkaObjectMapper) {
        Deserializer<Event> valueDeserializer = new Deserializer<>() {
            @Override
            public Event deserialize(String topic, byte[] data) {
                if (data == null) return null;
                try {
                    return kafkaObjectMapper.readValue(data, Event.class);
                } catch (Exception e) {
                    throw new SerializationException("Error deserializing JSON to Event", e);
                }
            }
        };

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Event> kafkaListenerContainerFactory(
            ConsumerFactory<String, Event> consumerFactory,
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Event> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    // ── Topics ───────────────────────────────────────────────────────────────
    @Bean
    public NewTopic mainTopic() {
        return TopicBuilder.name(mainTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(dltTopic).partitions(1).replicas(1).build();
    }

    // ── Error Handler (DLT + exponential backoff) ────────────────────────────
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, ex) -> new org.apache.kafka.common.TopicPartition(dltTopic, 0));
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
