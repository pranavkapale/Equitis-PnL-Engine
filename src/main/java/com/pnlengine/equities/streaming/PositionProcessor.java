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

public class PositionProcessor implements Processor<String, AvroExecution, String, AvroPositionBook> {
    private static final Logger log = LoggerFactory.getLogger(PositionProcessor.class);

    private ProcessorContext<String, AvroPositionBook> context;
    private KeyValueStore<String, AvroPositionBook> positionStore;
    private KeyValueStore<String, String> processedExecutionsStore;
    private final PositionEngine positionEngine = new PositionEngine();

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

        PositionBook newState = positionEngine.applyExecution(currentState, execution, LotMatchingStrategy.FIFO);
        
        AvroPositionBook avroNewState = AvroDomainMapper.toAvro(newState);
        
        positionStore.put(key, avroNewState);
        processedExecutionsStore.put(execId, "PROCESSED");
        
        context.forward(record.withValue(avroNewState));
    }
}
