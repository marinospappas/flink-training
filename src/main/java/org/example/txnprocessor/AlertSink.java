package org.example.txnprocessor;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.example.txnprocessor.common.Alert;


@PublicEvolving
@Slf4j
public class AlertSink implements SinkFunction<Alert> {

    private static final long serialVersionUID = 1L;
    public AlertSink() {
    }

    public void invoke(Alert value, SinkFunction.Context context) {
        log.info(value.toString());
    }
}
