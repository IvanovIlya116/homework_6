package ru.home.kafka.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaConsumer {

    public static final String TOPIC_WITHOUT_PARTITIONS_NAME = "kafka.withoutPartitions.topic";
    public static final String TOPIC_WITH_PARTITIONS_NAME = "kafka.withPartitions.topic";

    @KafkaListener(topics = {TOPIC_WITH_PARTITIONS_NAME, TOPIC_WITHOUT_PARTITIONS_NAME}, topicPartitions =
            {
                    @TopicPartition(
                            topic = TOPIC_WITH_PARTITIONS_NAME,
                            partitionOffsets = @PartitionOffset(
                                    partition = "0-9", initialOffset = "0"
                            )),
                    @TopicPartition(
                            topic = TOPIC_WITHOUT_PARTITIONS_NAME,
                            partitionOffsets = @PartitionOffset(
                                    partition = "0", initialOffset = "0"
                            ))
            })
    public void consume(@Payload String message,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) String receivedPartition,
                        @Header(KafkaHeaders.OFFSET) String receivedOffset) {
        log.info("Received from Topic {} by partition {}, offset {}: {}", receivedTopic, receivedPartition,
                receivedOffset, message);
    }
}
