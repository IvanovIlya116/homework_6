package ru.home.kafka.producer.controller;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.home.kafka.producer.model.Message;
import ru.home.kafka.producer.service.MessageSender;

import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.ResponseEntity.status;

@RestController
@RequiredArgsConstructor
@RequestMapping("/message")
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SenderController {

    MessageSender messageSender;
    @PostMapping("/send")
    public ResponseEntity<String> send(@RequestParam(value = "part", required = false) Integer partition,
                                       @RequestParam(value = "topic") String topicName,
                                       @RequestBody Message message) {
        if (messageSender.send(message.getMessageText(), topicName, partition)) {
            return ResponseEntity.ok("ok");
        }
        return status(INTERNAL_SERVER_ERROR)
                .body("kafka isn't available");
    }

    @PostMapping("/sendToTopicWithoutPartitions")
    public ResponseEntity<String> sendToTopicWithoutPartitions(@RequestBody Message message) {
        if (messageSender.sendToTopicWithoutPartitions(message.getMessageText())) {
            return ResponseEntity.ok("ok");
        }
        return status(INTERNAL_SERVER_ERROR)
                .body("kafka isn't available");
    }

    @PostMapping("/sendToTopicWithPartitions")
    public ResponseEntity<String> sendToTopicWithPartitions(@RequestBody Message message) {
        if (messageSender.sendToTopicWithPartitions(message.getMessageText())) {
            return ResponseEntity.ok("ok");
        }
        return status(INTERNAL_SERVER_ERROR)
                .body("kafka isn't available");
    }

}
