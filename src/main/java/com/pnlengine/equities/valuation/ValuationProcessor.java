package com.pnlengine.equities.valuation;

import com.pnlengine.equities.domain.PositionBook;
import com.pnlengine.equities.streaming.AvroDomainMapper;
import com.pnlengine.equities.streaming.avro.AvroMarketTick;
import com.pnlengine.equities.streaming.avro.AvroPnlEvent;
import com.pnlengine.equities.streaming.avro.AvroPositionBook;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;

public class ValuationProcessor implements Processor<String, AvroMarketTick, String, AvroPnlEvent> {

    private ProcessorContext<String, AvroPnlEvent> context;
    private KeyValueStore<String, AvroPositionBook> positionStore;

    @Override
    public void init(ProcessorContext<String, AvroPnlEvent> context) {
        this.context = context;
        this.positionStore = context.getStateStore("position-store");
    }

    @Override
    public void process(Record<String, AvroMarketTick> record) {
        String key = record.key();
        AvroMarketTick tick = record.value();
        if (tick == null) return;

        AvroPositionBook avroPosition = positionStore.get(key);
        if (avroPosition == null) {
            return; // No position to value
        }

        PositionBook domainPosition = AvroDomainMapper.toDomain(avroPosition);
        
        BigDecimal price = tick.getPrice();
        BigDecimal netQuantity = domainPosition.netQuantity();
        BigDecimal costBasis = domainPosition.costBasis();
        
        // Market Value = price * netQuantity
        // Total Cost = costBasis (per unit) * netQuantity
        // Unrealized PnL = Market Value - Total Cost
        BigDecimal marketValue = price.multiply(netQuantity);
        BigDecimal totalCost = costBasis.multiply(netQuantity);
        BigDecimal unrealizedPnl = marketValue.subtract(totalCost);

        AvroPnlEvent pnlEvent = AvroPnlEvent.newBuilder()
                .setAccountId(domainPosition.accountId())
                .setSymbol(domainPosition.symbol())
                .setRealizedPnl(domainPosition.realizedPnl())
                .setUnrealizedPnl(unrealizedPnl)
                .setReportingCurrency("USD")
                .setTimestamp(tick.getTimestamp())
                .build();

        context.forward(record.withValue(pnlEvent));
    }
}
