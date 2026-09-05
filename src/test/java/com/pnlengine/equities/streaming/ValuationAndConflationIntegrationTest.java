package com.pnlengine.equities.streaming;

import com.pnlengine.equities.streaming.avro.AvroExecution;
import com.pnlengine.equities.streaming.avro.AvroFxRate;
import com.pnlengine.equities.streaming.avro.AvroMarketTick;
import com.pnlengine.equities.streaming.avro.AvroPnlEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class ValuationAndConflationIntegrationTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, AvroExecution> executionTopic;
    private TestInputTopic<String, AvroMarketTick> tickTopic;
    private TestInputTopic<String, AvroFxRate> fxTopic;
    private TestOutputTopic<String, AvroPnlEvent> outputTopic;

    @BeforeEach
    public void setup() throws Exception {
        TopologyConfiguration config = new TopologyConfiguration();
        
        java.lang.reflect.Field urlField = TopologyConfiguration.class.getDeclaredField("schemaRegistryUrl");
        urlField.setAccessible(true);
        urlField.set(config, "mock://test-registry");

        java.lang.reflect.Field registryField = TopologyConfiguration.class.getDeclaredField("meterRegistry");
        registryField.setAccessible(true);
        registryField.set(config, new SimpleMeterRegistry());

        StreamsBuilder builder = new StreamsBuilder();
        config.buildPipeline(builder);

        Properties props = new Properties();
        props.put(org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG, "test-app-valuation");
        props.put(org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        
        testDriver = new TopologyTestDriver(builder.build(), props);

        Map<String, String> serdeConfig = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test-registry");

        SpecificAvroSerde<AvroExecution> executionSerde = new SpecificAvroSerde<>();
        executionSerde.configure(serdeConfig, false);

        SpecificAvroSerde<AvroMarketTick> tickSerde = new SpecificAvroSerde<>();
        tickSerde.configure(serdeConfig, false);

        SpecificAvroSerde<AvroFxRate> fxSerde = new SpecificAvroSerde<>();
        fxSerde.configure(serdeConfig, false);

        SpecificAvroSerde<AvroPnlEvent> pnlSerde = new SpecificAvroSerde<>();
        pnlSerde.configure(serdeConfig, false);

        executionTopic = testDriver.createInputTopic("trade-executions", new StringSerializer(), executionSerde.serializer());
        tickTopic = testDriver.createInputTopic("market-ticks", new StringSerializer(), tickSerde.serializer());
        fxTopic = testDriver.createInputTopic("fx-rates", new StringSerializer(), fxSerde.serializer());
        
        outputTopic = testDriver.createOutputTopic("pnl-events", new StringDeserializer(), pnlSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    public void testValuationAndConflation() throws InterruptedException {
        // 1. Seed FX Rate (EUR_USD = 1.10)
        AvroFxRate fxRate = AvroFxRate.newBuilder()
                .setCurrencyPair("EUR_USD")
                .setRate(new BigDecimal("1.10"))
                .setTimestamp(Instant.now())
                .build();
        fxTopic.pipeInput("EUR_USD", fxRate);

        String key = "ACC-1|AAPL";
        
        // 2. Seed Execution (Buy 10 @ 100)
        AvroExecution exec = AvroExecution.newBuilder()
                .setId("exec-1")
                .setAccountId("ACC-1")
                .setSymbol("AAPL")
                .setSide("BUY")
                .setQuantity(new BigDecimal("10"))
                .setPrice(new BigDecimal("100"))
                .setCommission(new BigDecimal("5"))
                .setTimestamp(Instant.now())
                .build();
        executionTopic.pipeInput(key, exec);

        // The initial UnrlPnL is calculated using the execution price as the market price: (10 * 100) - 1005 = -5
        // FX Normalized = -5 * 1.10 = -5.50
        AvroPnlEvent firstPnl = outputTopic.readKeyValue().value;
        assertThat(firstPnl.getUnrealizedPnl()).isEqualByComparingTo("-5.50");

        // 3. Market Tick 1 (Price = 110)
        // Market Value = 10 * 110 = 1100. UnrlPnL = 1100 - 1005 = 95.
        // FX Normalized = 95 * 1.10 = 104.5
        AvroMarketTick tick1 = AvroMarketTick.newBuilder()
                .setSymbol("AAPL")
                .setPrice(new BigDecimal("110"))
                .setTimestamp(Instant.now())
                .build();
        tickTopic.pipeInput(key, tick1);

        // The difference from 0 to 104.5 is large, it should emit immediately
        AvroPnlEvent tickPnl1 = outputTopic.readKeyValue().value;
        assertThat(tickPnl1.getUnrealizedPnl()).isEqualByComparingTo("104.5");

        // 4. Market Tick 2 (Price = 110.001) - Tiny change, should be conflated
        AvroMarketTick tick2 = AvroMarketTick.newBuilder()
                .setSymbol("AAPL")
                .setPrice(new BigDecimal("110.001"))
                .setTimestamp(Instant.now())
                .build();
        tickTopic.pipeInput(key, tick2);
        
        // Should not emit immediately
        assertThat(outputTopic.isEmpty()).isTrue();

        // Advance wall clock to trigger Punctuator (250ms elapsed)
        testDriver.advanceWallClockTime(java.time.Duration.ofMillis(300));
        
        // Output should now be emitted by the punctuator
        assertThat(outputTopic.isEmpty()).isFalse();
        AvroPnlEvent tickPnl2 = outputTopic.readKeyValue().value;
        
        // Market Value = 1100.01, UnrlPnL = 1100.01 - 1005 = 95.01
        // FX Normalized = 95.01 * 1.10 = 104.511
        assertThat(tickPnl2.getUnrealizedPnl()).isEqualByComparingTo("104.511");
    }
}
