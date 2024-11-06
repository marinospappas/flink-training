package org.example.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.txnprocessor.common.Alert;
import org.example.txnprocessor.common.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ConsumerAlert {

    @KafkaListener(topics = "#{'${spring.kafka.topic-alert}'}", containerFactory = "kafkaListenerContainerFactoryAlert")
    public void listen(ConsumerRecord<String, Alert> message) {
        log.info("Consumer Alert - Received {} in group test: {} - {}, offset: {}", message.value().getClass().getName(), message.key(), message.value(), message.offset());
    }

}
