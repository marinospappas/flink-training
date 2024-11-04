package org.example.txnprocessor;

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
import org.example.txnprocessor.common.Alert;
import org.example.txnprocessor.common.Transaction;
import org.example.txnprocessor.common.TransactionData;

import java.time.Duration;

@Slf4j
public class FraudDetectionJobExample {

    public static void main(String[] args) throws Exception {

        // Get environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        // Watermark to ensure events are delivered in order
        WatermarkStrategy<Transaction> wmStrategy = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        // Source
        GeneratorFunction<Long, Transaction> generatorFunction = index -> {
            Transaction t = TransactionData.data.get(Math.toIntExact(index));
            log.info("!!! sending transaction index {}, {}", index, t);
            return t;
        };
        long numberOfRecords = TransactionData.data.size();
        DataGeneratorSource<Transaction> source =
                new DataGeneratorSource<>(generatorFunction, numberOfRecords,  RateLimiterStrategy.perSecond(10), Types.POJO(Transaction.class));

        // DataStream
        DataStream<Transaction> transactions = env
                .fromSource(source, wmStrategy, "Generator Source")
                .name("transactions");

        // Processing
        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        // Output
        alerts
                .addSink(new AlertSink())
                .name("send-alerts");

        // Execute
        env.execute("Fraud Detection");
    }
}