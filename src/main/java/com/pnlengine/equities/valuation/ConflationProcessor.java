package com.pnlengine.equities.valuation;

import com.pnlengine.equities.streaming.avro.AvroPnlEvent;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;
import java.time.Duration;

public class ConflationProcessor implements Processor<String, AvroPnlEvent, String, AvroPnlEvent> {

    private ProcessorContext<String, AvroPnlEvent> context;
    private KeyValueStore<String, String> lastEmittedStore;
    private KeyValueStore<String, AvroPnlEvent> bufferedStore;

    @Override
    public void init(ProcessorContext<String, AvroPnlEvent> context) {
        this.context = context;
        this.lastEmittedStore = context.getStateStore("last-emitted-store");
        this.bufferedStore = context.getStateStore("buffered-event-store");

        context.schedule(Duration.ofMillis(50), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
    }

    @Override
    public void process(Record<String, AvroPnlEvent> record) {
        String key = record.key();
        AvroPnlEvent event = record.value();
        if (event == null) return;

        String meta = lastEmittedStore.get(key);
        boolean shouldEmit = false;
        long now = context.currentSystemTimeMs();

        if (meta == null) {
            shouldEmit = true;
        } else {
            String[] parts = meta.split("\\|");
            BigDecimal lastPnL = new BigDecimal(parts[0]);
            long lastEmitTime = Long.parseLong(parts[1]);

            BigDecimal newPnL = event.getUnrealizedPnl();
            BigDecimal diff = newPnL.subtract(lastPnL).abs();
            
            BigDecimal threshold = lastPnL.abs().multiply(new BigDecimal("0.0005")); // 0.05%
            if (threshold.compareTo(BigDecimal.ZERO) == 0) {
                threshold = new BigDecimal("0.01"); // Absolute threshold if previous PnL was 0
            }

            if (diff.compareTo(threshold) > 0) {
                shouldEmit = true;
            } else if (now - lastEmitTime >= 250) {
                shouldEmit = true;
            }
        }

        if (shouldEmit) {
            emit(key, event, now);
        } else {
            bufferedStore.put(key, event);
        }
    }

    private void punctuate(long timestamp) {
        long now = context.currentSystemTimeMs();
        try (KeyValueIterator<String, AvroPnlEvent> iterator = bufferedStore.all()) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                String key = entry.key;
                AvroPnlEvent event = entry.value;

                String meta = lastEmittedStore.get(key);
                if (meta != null) {
                    long lastEmitTime = Long.parseLong(meta.split("\\|")[1]);
                    if (now - lastEmitTime >= 250) {
                        emit(key, event, now);
                    }
                } else {
                    emit(key, event, now);
                }
            }
        }
    }

    private void emit(String key, AvroPnlEvent event, long now) {
        context.forward(new Record<>(key, event, now));
        lastEmittedStore.put(key, event.getUnrealizedPnl().toPlainString() + "|" + now);
        bufferedStore.delete(key);
    }
}
