package org.example.txnprocessor.frauddetection;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.txnprocessor.common.Alert;
import org.example.txnprocessor.common.Transaction;
import org.example.txnprocessor.filtering.TransactionFilter;
import org.example.txnprocessor.ordering.EventOrdering;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
public class FraudDetectionJob {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    public static void main(String[] args) throws Exception {
        (new FraudDetectionJob()).executeJob();
    }

    private void executeJob() throws Exception {

        // Get environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                //.setParallelism(2);

        // Source
        /*GeneratorFunction<Long, Transaction> generatorFunction = index -> {
            Transaction t = TransactionData.data.get(Math.toIntExact(index));
            t.setTimestamp(now - 20 + index);
            log.info("!!! sending transaction index {}, {} (timestamp now = {}", index, t, now);
            return t;
        };
        long numberOfRecords = TransactionData.data.size();
        DataGeneratorSource<Transaction> source =
                new DataGeneratorSource<>(generatorFunction, numberOfRecords,  RateLimiterStrategy.perSecond(1), Types.POJO(Transaction.class));*/

        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("testTopic")
                .setGroupId("flink-test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JsonDeserializationSchema<>(Transaction.class)))
                .build();

        KafkaSink<Alert> sink = KafkaSink.<Alert>builder()
                .setBootstrapServers("localhost:9092")
                //.setRecordSerializer(new AlertSerializationSchema("testAlert"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder().setValueSerializationSchema(new JsonSerializationSchema<Alert>()).setTopic("testAlert").build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // WaterMark strategy
        //WatermarkStrategy<Transaction> wmStrategy = WatermarkStrategy
        //        .<Transaction>forBoundedOutOfOrderness(Duration.of(2, ChronoUnit.SECONDS))
        //        .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        // DataStream process out of order elements
        DataStream<Transaction> orderedTransactions = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .keyBy(Transaction::getAccountId)
                .process(new EventOrdering())
                .name("ordered-transactions");

        // DataStream processing
        orderedTransactions
                .filter(new TransactionFilter())
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .sinkTo(sink)
                .name("fraud_alerts");

        // Execute
        env.execute("Fraud Detection");
    }
}