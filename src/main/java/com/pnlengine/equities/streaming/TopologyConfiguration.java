package com.pnlengine.equities.streaming;

import com.pnlengine.equities.streaming.avro.AvroExecution;
import com.pnlengine.equities.streaming.avro.AvroFxRate;
import com.pnlengine.equities.streaming.avro.AvroMarketTick;
import com.pnlengine.equities.streaming.avro.AvroPnlEvent;
import com.pnlengine.equities.streaming.avro.AvroPositionBook;
import com.pnlengine.equities.streaming.avro.AvroRiskEvent;
import com.pnlengine.equities.streaming.avro.AvroTradeLifecycle;
import com.pnlengine.equities.valuation.BiTemporalCorrectionProcessor;
import com.pnlengine.equities.valuation.ConflationProcessor;
import com.pnlengine.equities.valuation.RiskAggregationProcessor;
import com.pnlengine.equities.valuation.ValuationProcessor;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import io.micrometer.core.instrument.MeterRegistry;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

@Configuration
public class TopologyConfiguration {

    @Value("${spring.kafka.properties.schema.registry.url:mock://localhost:8081}")
    private String schemaRegistryUrl;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        Map<String, String> serdeConfig = Map.of("schema.registry.url", schemaRegistryUrl);

        SpecificAvroSerde<AvroExecution> executionSerde = new SpecificAvroSerde<>();
        executionSerde.configure(serdeConfig, false);

        SpecificAvroSerde<AvroPositionBook> positionBookSerde = new SpecificAvroSerde<>();
        positionBookSerde.configure(serdeConfig, false);

        SpecificAvroSerde<AvroMarketTick> tickSerde = new SpecificAvroSerde<>();
        tickSerde.configure(serdeConfig, false);

        SpecificAvroSerde<AvroFxRate> fxRateSerde = new SpecificAvroSerde<>();
        fxRateSerde.configure(serdeConfig, false);

        SpecificAvroSerde<AvroPnlEvent> pnlEventSerde = new SpecificAvroSerde<>();
        pnlEventSerde.configure(serdeConfig, false);

        SpecificAvroSerde<AvroTradeLifecycle> lifecycleSerde = new SpecificAvroSerde<>();
        lifecycleSerde.configure(serdeConfig, false);

        SpecificAvroSerde<AvroRiskEvent> riskEventSerde = new SpecificAvroSerde<>();
        riskEventSerde.configure(serdeConfig, false);

        // State Stores
        StoreBuilder<KeyValueStore<String, AvroPositionBook>> positionStoreBuilder = 
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("position-store"), Serdes.String(), positionBookSerde);

        StoreBuilder<KeyValueStore<String, String>> executionStoreBuilder = 
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("processed-executions-store"), Serdes.String(), Serdes.String());

        StoreBuilder<KeyValueStore<String, String>> lastEmittedStoreBuilder = 
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("last-emitted-store"), Serdes.String(), Serdes.String());

        StoreBuilder<KeyValueStore<String, AvroPnlEvent>> bufferedEventStoreBuilder = 
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("buffered-event-store"), Serdes.String(), pnlEventSerde);

        // Phase 4 Stores
        StoreBuilder<KeyValueStore<String, String>> riskSymbolPnlStoreBuilder = 
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("risk-symbol-pnl-store"), Serdes.String(), Serdes.String());
        StoreBuilder<KeyValueStore<String, String>> riskAccountEquityStoreBuilder = 
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("risk-account-equity-store"), Serdes.String(), Serdes.String());

        streamsBuilder.addStateStore(positionStoreBuilder);
        streamsBuilder.addStateStore(executionStoreBuilder);
        streamsBuilder.addStateStore(lastEmittedStoreBuilder);
        streamsBuilder.addStateStore(bufferedEventStoreBuilder);
        streamsBuilder.addStateStore(riskSymbolPnlStoreBuilder);
        streamsBuilder.addStateStore(riskAccountEquityStoreBuilder);

        // Phase 2: Trade Executions -> Position Processor
        KStream<String, AvroPositionBook> positionStream = streamsBuilder
                .stream("trade-executions", Consumed.with(Serdes.String(), executionSerde))
                .process(() -> new PositionProcessor(meterRegistry), "position-store", "processed-executions-store");

        // Convert trades to PnL events (using the raw book state)
        KStream<String, AvroPnlEvent> tradePnlStream = positionStream.mapValues(pos -> 
            AvroPnlEvent.newBuilder()
                .setAccountId(pos.getAccountId())
                .setSymbol(pos.getSymbol())
                .setRealizedPnl(pos.getRealizedPnl())
                .setUnrealizedPnl(pos.getUnrealizedPnl()) // This will be stale until MtM
                .setReportingCurrency("USD")
                .setTimestamp(Instant.now())
                .build()
        );

        // Phase 3: Market Ticks -> Valuation Processor (MtM)
        KStream<String, AvroPnlEvent> marketPnlStream = streamsBuilder
                .stream("market-ticks", Consumed.with(Serdes.String(), tickSerde))
                .process(() -> new ValuationProcessor(meterRegistry), "position-store");

        // Phase 4: Bi-Temporal Correction
        KStream<String, AvroPnlEvent> correctionPnlStream = streamsBuilder
                .stream("trade-lifecycle", Consumed.with(Serdes.String(), lifecycleSerde))
                .process(() -> new BiTemporalCorrectionProcessor(meterRegistry), "position-store");

        // Merge Trade PnL, Market PnL, and Correction PnL streams
        KStream<String, AvroPnlEvent> mergedPnlStream = tradePnlStream.merge(marketPnlStream).merge(correctionPnlStream);

        // Phase 3: FX GlobalKTable
        GlobalKTable<String, AvroFxRate> fxRatesTable = streamsBuilder
                .globalTable("fx-rates", Consumed.with(Serdes.String(), fxRateSerde));

        // Phase 3: FX Normalization Join
        KStream<String, AvroPnlEvent> normalizedPnlStream = mergedPnlStream.join(
            fxRatesTable,
            (key, event) -> "EUR_USD", // Lookup key for FX table (hardcoded EUR_USD per assumptions)
            (event, fxRate) -> {
                BigDecimal rate = fxRate.getRate();
                event.setRealizedPnl(event.getRealizedPnl().multiply(rate));
                event.setUnrealizedPnl(event.getUnrealizedPnl().multiply(rate));
                return event;
            }
        );

        // Phase 3: Conflation & Egress
        KStream<String, AvroPnlEvent> conflatedPnlStream = normalizedPnlStream
                .process(ConflationProcessor::new, "last-emitted-store", "buffered-event-store");
                
        conflatedPnlStream.to("pnl-events", Produced.with(Serdes.String(), pnlEventSerde));

        // Phase 4: Risk Aggregation
        conflatedPnlStream
                .selectKey((k, v) -> v.getAccountId())
                .repartition(Repartitioned.with(Serdes.String(), pnlEventSerde))
                .process(RiskAggregationProcessor::new, "risk-symbol-pnl-store", "risk-account-equity-store")
                .to("risk-events", Produced.with(Serdes.String(), riskEventSerde));
    }
}
