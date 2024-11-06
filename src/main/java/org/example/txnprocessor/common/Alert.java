package org.example.txnprocessor.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public final class Alert {

    private long id;
    private long ts;
    private String details;

    public String toString() {
        return "Alert[id=" + id + " timestamp=" + ts + " details=" + details + "]";
    }
}
