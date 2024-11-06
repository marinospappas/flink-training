package org.example.txnprocessor.common;

import java.time.Instant;

public class Utils {

    public static String milliToString(long timestamp) {
        return Instant.ofEpochMilli(timestamp).toString();
    }
}
