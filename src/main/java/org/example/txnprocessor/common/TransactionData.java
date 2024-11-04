package org.example.txnprocessor.common;

import java.util.List;

public class TransactionData {

    public static List<Transaction> data = List.of(
            new Transaction(12345, 1, 124.5, "GR"),
            new Transaction(99901, 2, 99.25, "GB"),
            new Transaction(88845, 3, 0.155, "GB"),
            new Transaction(12345, 4, 48.1, "DE"),
            new Transaction(99901, 5, 1024.0, "GB"),
            new Transaction(99901, 6, 0.48, "GB"),
            new Transaction(99901, 7, 120.0, "CH"),
            new Transaction(12345, 8, 678.45, "GB"),
            new Transaction(88845, 9, 502.23, "GB"),
            new Transaction(12345, 10, 56.78, "GB"),
            new Transaction(88845, 11, 0.11, "GB"),
            new Transaction(88845, 12, 0.99, "GB"),
            new Transaction(99901, 13, 642.4, "GB"),
            new Transaction(88845, 14, 625.0, "GB")
    );
}
