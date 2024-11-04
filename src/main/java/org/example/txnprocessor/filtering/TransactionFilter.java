package org.example.txnprocessor.filtering;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.example.txnprocessor.common.Transaction;

public class TransactionFilter implements FilterFunction<Transaction> {

    @Override
    public boolean filter(Transaction transaction) {
        return StringUtils.equals(transaction.getCountry(), "GB");
    }
}
