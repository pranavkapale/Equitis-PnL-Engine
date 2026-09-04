package com.pnlengine.equities.valuation;

import com.pnlengine.equities.streaming.avro.AvroPnlEvent;
import com.pnlengine.equities.streaming.avro.AvroRiskEvent;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;
import java.time.Instant;

public class RiskAggregationProcessor implements Processor<String, AvroPnlEvent, String, AvroRiskEvent> {

    private ProcessorContext<String, AvroRiskEvent> context;
    private KeyValueStore<String, String> symbolPnlStore;
    private KeyValueStore<String, String> equityStore;
    
    // Configurable or static margin requirement
    private static final BigDecimal MAINTENANCE_MARGIN = new BigDecimal("100000");

    @Override
    public void init(ProcessorContext<String, AvroRiskEvent> context) {
        this.context = context;
        this.symbolPnlStore = context.getStateStore("risk-symbol-pnl-store");
        this.equityStore = context.getStateStore("risk-account-equity-store");
    }

    @Override
    public void process(Record<String, AvroPnlEvent> record) {
        AvroPnlEvent event = record.value();
        String accountId = event.getAccountId();
        String symbol = event.getSymbol();
        String symbolKey = accountId + "|" + symbol;

        BigDecimal newSymbolPnl = event.getRealizedPnl().add(event.getUnrealizedPnl());
        
        String oldSymbolPnlStr = symbolPnlStore.get(symbolKey);
        BigDecimal oldSymbolPnl = oldSymbolPnlStr == null ? BigDecimal.ZERO : new BigDecimal(oldSymbolPnlStr);

        BigDecimal delta = newSymbolPnl.subtract(oldSymbolPnl);
        
        String oldEquityStr = equityStore.get(accountId);
        BigDecimal oldEquity = oldEquityStr == null ? BigDecimal.ZERO : new BigDecimal(oldEquityStr);
        
        BigDecimal newEquity = oldEquity.add(delta);
        
        symbolPnlStore.put(symbolKey, newSymbolPnl.toPlainString());
        equityStore.put(accountId, newEquity.toPlainString());
        
        if (newEquity.compareTo(MAINTENANCE_MARGIN) < 0) {
            AvroRiskEvent riskEvent = AvroRiskEvent.newBuilder()
                    .setAccountId(accountId)
                    .setTotalEquity(newEquity)
                    .setMarginRequirement(MAINTENANCE_MARGIN)
                    .setBreachAmount(MAINTENANCE_MARGIN.subtract(newEquity))
                    .setTimestamp(Instant.now())
                    .build();
            
            context.forward(new Record<>(accountId, riskEvent, context.currentSystemTimeMs()));
        }
    }
}
