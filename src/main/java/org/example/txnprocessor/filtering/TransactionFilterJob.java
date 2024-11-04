package org.example.txnprocessor.filtering;

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
import org.example.txnprocessor.common.Transaction;
import org.example.txnprocessor.common.TransactionData;

import java.time.Duration;

@Slf4j
public class TransactionFilterJob {

    public static void main(String[] args) throws Exception {
        (new TransactionFilterJob()).executeJob();
    }

    private void executeJob() throws Exception {

        // Get environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        // Watermark strategy
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
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Generator Source")
                .name("transactions");

        // Processing
        DataStream<Transaction> filteredTxns = transactions
                .filter(new TransactionFilter())
                .name("txn-filtering");

        filteredTxns.print();

        // Execute
        env.execute("Transaction Filter");
    }
}