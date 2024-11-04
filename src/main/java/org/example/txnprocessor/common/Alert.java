package org.example.txnprocessor.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public final class Alert {

    private long id;
    private String details;

    public String toString() {
        return "Alert{id=" + this.id + " details=" + this.details + "}";
    }
}
