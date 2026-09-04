package com.pnlengine.equities.valuation;

import com.pnlengine.equities.domain.PositionBook;
import com.pnlengine.equities.engine.PositionEngine;
import com.pnlengine.equities.streaming.avro.AvroPnlEvent;
import com.pnlengine.equities.streaming.avro.AvroPositionBook;
import com.pnlengine.equities.streaming.avro.AvroTradeLifecycle;
import com.pnlengine.equities.streaming.avro.TradeLifecycleAction;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.stream.Collectors;

public class BiTemporalCorrectionProcessor implements Processor<String, AvroTradeLifecycle, String, AvroPnlEvent> {

    private ProcessorContext<String, AvroPnlEvent> context;
    private KeyValueStore<String, AvroPositionBook> positionStore;
    private final PositionEngine engine;

    public BiTemporalCorrectionProcessor() {
        this.engine = new PositionEngine();
    }

    @Override
    public void init(ProcessorContext<String, AvroPnlEvent> context) {
        this.context = context;
        this.positionStore = context.getStateStore("position-store");
    }

    @Override
    public void process(Record<String, AvroTradeLifecycle> record) {
        AvroTradeLifecycle lifecycle = record.value();
        
        if (lifecycle.getAction() != TradeLifecycleAction.CANCEL) {
            return; // We only support CANCEL right now
        }

        String key = record.key();
        AvroPositionBook avroBook = positionStore.get(key);
        
        if (avroBook == null) {
            return; // Cannot cancel a lot for a position that doesn't exist
        }

        // Convert Avro state to domain state
        PositionBook state = new PositionBook(
                avroBook.getAccountId(),
                avroBook.getSymbol(),
                avroBook.getNetQuantity(),
                avroBook.getCostBasis(),
                avroBook.getRealizedPnl(),
                avroBook.getUnrealizedPnl(),
                avroBook.getOpenLots().stream().map(l -> 
                    new com.pnlengine.equities.domain.TaxLot(
                        l.getId(), l.getQuantity(), l.getPrice(), l.getTimestamp()
                    )
                ).collect(Collectors.toList())
        );

        try {
            // Recalculate
            PositionBook newState = engine.cancelLot(state, lifecycle.getOriginalExecutionId());

            // Convert back to Avro and store
            AvroPositionBook newAvroBook = AvroPositionBook.newBuilder()
                    .setAccountId(newState.accountId())
                    .setSymbol(newState.symbol())
                    .setNetQuantity(newState.netQuantity())
                    .setCostBasis(newState.costBasis())
                    .setRealizedPnl(newState.realizedPnl())
                    .setUnrealizedPnl(newState.unrealizedPnl())
                    .setOpenLots(newState.openLots().stream().map(l -> 
                        com.pnlengine.equities.streaming.avro.AvroTaxLot.newBuilder()
                            .setId(l.id())
                            .setQuantity(l.quantity())
                            .setPrice(l.price())
                            .setTimestamp(l.timestamp())
                            .build()
                    ).collect(Collectors.toList()))
                    .build();

            positionStore.put(key, newAvroBook);

            // Emit corrective PnL event
            AvroPnlEvent pnlEvent = AvroPnlEvent.newBuilder()
                    .setAccountId(newState.accountId())
                    .setSymbol(newState.symbol())
                    .setRealizedPnl(newState.realizedPnl())
                    .setUnrealizedPnl(newState.unrealizedPnl())
                    .setReportingCurrency("USD")
                    .setTimestamp(Instant.now())
                    .build();

            context.forward(new Record<>(key, pnlEvent, context.currentSystemTimeMs()));

        } catch (IllegalArgumentException e) {
            // Lot not found, drop the correction or send to DLQ
        }
    }
}
