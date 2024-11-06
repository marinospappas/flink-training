package org.example.txnprocessor.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

@AllArgsConstructor
@NoArgsConstructor
public class AlertSerializationSchema implements KafkaRecordSerializationSchema<Alert> {

    private String topic;

    private static final ObjectMapper objectMapper =
            JsonMapper.builder()
                    .build()
                    .registerModule(new JavaTimeModule())
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Alert alert, KafkaSinkContext context, Long timestamp) {
        try {
            return new ProducerRecord<>(topic, null, alert.getTs(), null, objectMapper.writeValueAsBytes(alert));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + alert, e);
        }
    }
}
