package org.example.txnprocessor.frauddetection;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
//import org.apache.flink.connector.datagen.source.DataGeneratorSource;
//import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.txnprocessor.AlertSink;
import org.example.txnprocessor.FraudDetector;
import org.example.txnprocessor.common.Alert;
import org.example.txnprocessor.common.Transaction;
import org.example.txnprocessor.common.TransactionData;
import org.example.txnprocessor.filtering.TransactionFilter;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Slf4j
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        (new FraudDetectionJob()).executeJob();
    }

    private void executeJob() throws Exception {

        // Get environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                //.setParallelism(2);

        long now = Instant.now().toEpochMilli();

        // Source
        GeneratorFunction<Long, Transaction> generatorFunction = index -> {
            Transaction t = TransactionData.data.get(Math.toIntExact(index));
            t.setTimestamp(now - 20 + index);
            log.info("!!! sending transaction index {}, {} (timestamp now = {}", index, t, now);
            return t;
        };
        long numberOfRecords = TransactionData.data.size();
        DataGeneratorSource<Transaction> source =
                new DataGeneratorSource<>(generatorFunction, numberOfRecords,  RateLimiterStrategy.perSecond(10), Types.POJO(Transaction.class));

        // WaterMark strategy
        WatermarkStrategy<Transaction> wmStrategy = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.of(2, ChronoUnit.SECONDS))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        // DataStream processing
        env
                .fromSource(source, wmStrategy, "Generator Source")
                //.fromData(TransactionData.data)
                //.assignTimestampsAndWatermarks(wmStrategy)
                .filter(new TransactionFilter())
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .addSink(new AlertSink())
                .name("fraud_alerts");

        // Execute
        env.execute("Fraud Detection");
    }
}