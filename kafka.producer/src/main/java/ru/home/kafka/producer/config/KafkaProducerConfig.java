package ru.home.kafka.producer.config;

import lombok.val;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;

@Configuration
public class KafkaProducerConfig {

    public static final String TOPIC_WITHOUT_PARTITIONS_NAME = "kafka.withoutPartitions.topic";
    public static final String TOPIC_WITH_PARTITIONS_NAME = "kafka.withPartitions.topic";

    @Value("${spring.kafka.bootstrap-servers}")
    private String serverAddress;

    @Bean
    public NewTopic topicWithoutPartitions() {
        return TopicBuilder.name(TOPIC_WITHOUT_PARTITIONS_NAME)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic topicWithPartitions() {
        return TopicBuilder.name(TOPIC_WITH_PARTITIONS_NAME)
                .replicas(1)
                .partitions(10)
                .build();
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        val config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddress);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

}
