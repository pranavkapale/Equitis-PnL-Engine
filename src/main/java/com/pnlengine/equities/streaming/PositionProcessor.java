package com.pnlengine.equities.streaming;

import com.pnlengine.equities.domain.Execution;
import com.pnlengine.equities.domain.PositionBook;
import com.pnlengine.equities.engine.LotMatchingStrategy;
import com.pnlengine.equities.engine.PositionEngine;
import com.pnlengine.equities.streaming.avro.AvroExecution;
import com.pnlengine.equities.streaming.avro.AvroPositionBook;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.atomic.AtomicInteger;

public class PositionProcessor implements Processor<String, AvroExecution, String, AvroPositionBook> {
    private static final Logger log = LoggerFactory.getLogger(PositionProcessor.class);

    private ProcessorContext<String, AvroPositionBook> context;
    private KeyValueStore<String, AvroPositionBook> positionStore;
    private KeyValueStore<String, String> processedExecutionsStore;
    private final PositionEngine positionEngine = new PositionEngine();
    
    private final MeterRegistry meterRegistry;
    private final Timer calculationTimer;
    private final AtomicInteger activePositionBookLots;

    public PositionProcessor(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.calculationTimer = meterRegistry.timer("equitis_pnl_calculation_latency_seconds");
        this.activePositionBookLots = meterRegistry.gauge("equitis_active_position_book_lots", new AtomicInteger(0));
    }

    @Override
    public void init(ProcessorContext<String, AvroPositionBook> context) {
        this.context = context;
        this.positionStore = context.getStateStore("position-store");
        this.processedExecutionsStore = context.getStateStore("processed-executions-store");
    }

    @Override
    public void process(Record<String, AvroExecution> record) {
        String key = record.key();
        AvroExecution avroExecution = record.value();

        if (avroExecution == null) {
            log.warn("Received null execution for key {}", key);
            return;
        }

        String execId = avroExecution.getId();
        if (processedExecutionsStore.get(execId) != null) {
            log.info("Duplicate execution detected and dropped: {}", execId);
            return;
        }

        Execution execution = AvroDomainMapper.toDomain(avroExecution);
        AvroPositionBook avroState = positionStore.get(key);
        
        PositionBook currentState = avroState != null 
                ? AvroDomainMapper.toDomain(avroState) 
                : PositionBook.empty(execution.accountId(), execution.symbol());

        // Measure PnL calculation latency
        PositionBook newState = calculationTimer.record(() -> 
            positionEngine.applyExecution(currentState, execution, LotMatchingStrategy.FIFO)
        );
        
        // Track the size (number of open lots) of the currently active position book
        if (newState != null && newState.openLots() != null) {
            activePositionBookLots.set(newState.openLots().size());
        }
        
        AvroPositionBook avroNewState = AvroDomainMapper.toAvro(newState);
        
        positionStore.put(key, avroNewState);
        processedExecutionsStore.put(execId, "PROCESSED");
        
        context.forward(record.withValue(avroNewState));
    }
}
