package ru.home.kafka.producer.service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.home.kafka.producer.config.KafkaProducerConfig;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class MessageSender {

    KafkaTemplate<String, String> kafkaTemplate;
    public boolean send(String message, String topicName, Integer partition) {
        val future = kafkaTemplate.send(topicName,
                partition == null ? 0 : partition,
                null,
                message);

        try {
            val result = future.get(2, TimeUnit.SECONDS);
            log.info("Successful send to {} with offset {} to partition {}",
                    result.getProducerRecord().topic(), result.getRecordMetadata().offset(),
                    result.getRecordMetadata().partition());
            return true;
        } catch (TimeoutException | ExecutionException | InterruptedException e) {
            log.error("Cannot send message to Kafka Topic {}", topicName, e);
        }
        return false;
    }


    public boolean sendToTopicWithoutPartitions(String message) {
        val future = kafkaTemplate.send(KafkaProducerConfig.
                TOPIC_WITHOUT_PARTITIONS_NAME, message);

        try {
            val result = future.get();
            log.info("Successful send to {} with offset {} to partition {}", result.getProducerRecord().topic(), result.
                    getRecordMetadata().offset(), result.getRecordMetadata().partition());
            return true;
        } catch (ExecutionException | InterruptedException e) {
            log.error("Cannot send message to Kafka topic {}", KafkaProducerConfig.TOPIC_WITHOUT_PARTITIONS_NAME);
        }

        return false;
    }

    public boolean sendToTopicWithPartitions(String message) {
        val future = kafkaTemplate.send(KafkaProducerConfig.
                TOPIC_WITH_PARTITIONS_NAME, message);

        try {
            val result = future.get();
            log.info("Successful send to {} with offset {} to partition {}", result.getProducerRecord().topic(), result.
                    getRecordMetadata().offset(), result.getRecordMetadata().partition());
            return true;
        } catch (ExecutionException | InterruptedException e) {
            log.error("Cannot send message to Kafka topic {}", KafkaProducerConfig.TOPIC_WITH_PARTITIONS_NAME);
        }

        return false;
    }
}
