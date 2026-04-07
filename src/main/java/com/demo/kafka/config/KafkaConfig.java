package com.demo.kafka.config;

import com.demo.kafka.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.avro.specific.SpecificRecord;
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

    @Value("${kafka.topics.avro}")
    private String avroTopic;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.schema-registry.url}")
    private String schemaRegistryUrl;

    // ── Jackson 2.x ObjectMapper ─────────────────────────────────────────────
    @Bean(name = "kafkaObjectMapper")
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    // ── JSON Producer ────────────────────────────────────────────────────────
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

    // ── Avro Producer ────────────────────────────────────────────────────────
    // KafkaAvroSerializer contacts the Schema Registry on every send().
    // If the AvroEvent object does NOT match the registered schema,
    // a SerializationException is raised BEFORE the message is written to Kafka.
    @Bean
    public ProducerFactory<String, SpecificRecord> avroProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        // Schema Registry endpoint – serialiser registers/validates the schema here
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, SpecificRecord> avroKafkaTemplate(
            ProducerFactory<String, SpecificRecord> avroProducerFactory) {
        return new KafkaTemplate<>(avroProducerFactory);
    }

    // ── Avro Consumer ────────────────────────────────────────────────────────
    // KafkaAvroDeserializer fetches the schema from the registry and validates
    // the binary Avro payload before handing it to the listener method.
    @Bean
    public ConsumerFactory<String, SpecificRecord> avroConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Return the generated AvroEvent class, not a GenericRecord
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SpecificRecord> avroKafkaListenerContainerFactory(
            ConsumerFactory<String, SpecificRecord> avroConsumerFactory,
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, SpecificRecord> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(avroConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    // ── JSON Consumer ────────────────────────────────────────────────────────
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

    @Bean
    public NewTopic avroTopic() {
        return TopicBuilder.name(avroTopic).partitions(3).replicas(1).build();
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


