package org.example.txnprocessor.ordering;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.txnprocessor.common.Transaction;

import java.io.IOException;
import java.io.Serial;
import java.util.*;

import static org.example.txnprocessor.common.Utils.milliToString;

@Slf4j
public class EventOrdering extends KeyedProcessFunction<Long, Transaction, Transaction> {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final long WINDOW = 4000;

    private transient ValueState<List<Transaction>> streamState;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<List<Transaction>> streamSateDescriptor = new ValueStateDescriptor<>("txn", Types.LIST(Types.POJO(Transaction.class)));
        streamState = getRuntimeContext().getState(streamSateDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Transaction> collector) throws Exception {

        log.info("<<< thread: {}, processing element {}", Thread.currentThread().getName(), transaction);

        // add the current event to the list of event
        List<Transaction> events = streamState.value();
        if (Objects.isNull(events)) {
            events = new ArrayList<>();
        }
        events.add(transaction);
        streamState.update(events);
        //log.info("   <<<<<< list of events in this window: {}", streamState.value());

        long timer = transaction.getTimestamp() + WINDOW;
        if (timer <= context.timerService().currentProcessingTime()) {
            log.warn("   %%% event arrived too late, current processing time: {}", milliToString(context.timerService().currentProcessingTime()));
        } else {
            context.timerService().registerProcessingTimeTimer(timer);
            //log.info("   <<<<<< Set timer: {}, current processing time: {}", milliToString(timer), milliToString(context.timerService().currentProcessingTime()));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Transaction> out) throws IOException {
        //log.info("   &&& timer for {} {} triggered, list of events: {}",timestamp, milliToString(timestamp), streamState.value());
        List<Transaction> events = streamState.value();
        int index = -1;
        for (int i = 0; i < events.size(); ++i) {
            Transaction event = events.get(i);
            if (event.getTimestamp() == timestamp - WINDOW) {
                index = i;
                break;
            }
        }
        if (index >= 0) {
            out.collect(events.get(index));
            log.info("   &&& published event {}", events.get(index));
            events.remove(index);
            streamState.update(events);
        }
    }
}