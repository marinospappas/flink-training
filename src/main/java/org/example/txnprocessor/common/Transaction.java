package org.example.txnprocessor.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.time.Instant;
import java.util.List;

import static org.example.txnprocessor.common.Utils.milliToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
    private long accountId;
    private long timestamp;
    private double amount;
    private String country;

    public String toString() {
        return "Transaction[accountId=" + accountId + ", timestamp=" + milliToString(timestamp) + ", amount=" + amount + ", country " + country + "]";
    }

    public static Transaction random() {
        List<String> countries = List.of("GB", "US", "CH", "GR", "DE");
        RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
        return new Transaction(randomDataGenerator.nextLong(1, 1000), Instant.now().toEpochMilli(),
                randomDataGenerator.nextUniform(0.01, 1000.0), countries.get(randomDataGenerator.nextInt(0, countries.size()) - 1));
    }

}
