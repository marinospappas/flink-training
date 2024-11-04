package org.example.txnprocessor;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.txnprocessor.common.Alert;
import org.example.txnprocessor.common.Transaction;

import java.io.Serial;

@Slf4j
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Transaction> txnState;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Transaction> txnDescriptor = new ValueStateDescriptor<>("txn", Types.POJO(Transaction.class));
        txnState = getRuntimeContext().getState(txnDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        log.info(">>> thread: {}, processing element {}", Thread.currentThread().getName(), transaction);

        // Get the previous txn for the current key
        Transaction previousTransaction = txnState.value();

        // Check for fraud
        if (previousTransaction != null) {
            if (transaction.getAmount() > LARGE_AMOUNT && previousTransaction.getAmount() < SMALL_AMOUNT) {
                // Output an alert downstream
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                alert.setDetails("second txn. " + transaction + " previous txn. " + previousTransaction);

                collector.collect(alert);
            }
        }
        txnState.update(transaction);
    }
}