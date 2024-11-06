package org.example.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.txnprocessor.common.Transaction;
import org.example.txnprocessor.common.TransactionData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class Producer implements CommandLineRunner {

    private static final List<String> data = List.of(
            "Hello World!", "Again", "...and again", "...and again"
    );

    @Value(value = "${spring.kafka.topic-transaction}")
    private String topicName;

    private final KafkaTemplate<Long, Transaction> kafkaTemplate;

    public Producer(KafkaTemplate<Long, Transaction> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(Transaction txn) {
        ProducerRecord<Long, Transaction> record = new ProducerRecord<>(topicName, txn.getAccountId(), txn);
        CompletableFuture<SendResult<Long, Transaction>> future = kafkaTemplate.send(record);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
               log.info("Sent message=[{}] with offset=[{}], partition[{}], hash of key {}", txn, result.getRecordMetadata().offset(), result.getRecordMetadata().partition() , ((Long)txn.getAccountId()).hashCode() % 3);
            } else {
                log.error("Unable to send message=[{}] due to: {}", txn, ex.getMessage());
            }
        });
    }

    @Override
    public void run(String... args) {
        for (int i = 0; i < TransactionData.data.size(); ++i) {
            Transaction txn = TransactionData.data.get(i);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            txn.setTimestamp(Instant.now().toEpochMilli() - 30 + i);
        }
        log.info("((( {}", TransactionData.data);
        List<Transaction> txnData = new ArrayList<>(TransactionData.data);
        Collections.shuffle(txnData);
        log.info("))) {}", txnData);
        for (Transaction txn : txnData) {
            try {
                Thread.sleep(50);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            sendMessage(txn);
        }
    }
}
