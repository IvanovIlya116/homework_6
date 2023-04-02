package ru.home.kafka.producer.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class Message implements Serializable {

    String messageText;
}
