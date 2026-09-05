package com.pnlengine.equities.streaming;

import com.pnlengine.equities.streaming.avro.AvroExecution;
import com.pnlengine.equities.streaming.avro.AvroFxRate;
import com.pnlengine.equities.streaming.avro.AvroPositionBook;
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
import org.springframework.test.util.ReflectionTestUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class TopologyIntegrationTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, AvroExecution> inputTopic;
    private TestInputTopic<String, AvroFxRate> fxTopic;
    private TestOutputTopic<String, AvroPnlEvent> outputTopic;

    @BeforeEach
    public void setup() {
        TopologyConfiguration config = new TopologyConfiguration();
        ReflectionTestUtils.setField(config, "schemaRegistryUrl", "mock://test-registry");
        ReflectionTestUtils.setField(config, "meterRegistry", new SimpleMeterRegistry());

        StreamsBuilder builder = new StreamsBuilder();
        config.buildPipeline(builder);

        Properties props = new Properties();
        props.put(org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        
        testDriver = new TopologyTestDriver(builder.build(), props);

        Map<String, String> serdeConfig = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test-registry");

        SpecificAvroSerde<AvroExecution> executionSerde = new SpecificAvroSerde<>();
        executionSerde.configure(serdeConfig, false);

        SpecificAvroSerde<AvroPnlEvent> pnlSerde = new SpecificAvroSerde<>();
        pnlSerde.configure(serdeConfig, false);

        SpecificAvroSerde<com.pnlengine.equities.streaming.avro.AvroFxRate> fxSerde = new SpecificAvroSerde<>();
        fxSerde.configure(serdeConfig, false);

        inputTopic = testDriver.createInputTopic("trade-executions", new StringSerializer(), executionSerde.serializer());
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
    public void testTopologyProcessesExecutionAndState() {
        // Seed FX
        com.pnlengine.equities.streaming.avro.AvroFxRate fxRate = com.pnlengine.equities.streaming.avro.AvroFxRate.newBuilder()
                .setCurrencyPair("EUR_USD")
                .setRate(new BigDecimal("1.00")) // 1.0 rate so PnL equals what we expect
                .setTimestamp(Instant.now())
                .build();
        fxTopic.pipeInput("EUR_USD", fxRate);

        String key = "ACC-1|AAPL";
        
        AvroExecution exec1 = AvroExecution.newBuilder()
                .setId("exec-1")
                .setAccountId("ACC-1")
                .setSymbol("AAPL")
                .setSide("BUY")
                .setQuantity(new BigDecimal("10"))
                .setPrice(new BigDecimal("100"))
                .setCommission(new BigDecimal("5"))
                .setTimestamp(Instant.now())
                .build();

        inputTopic.pipeInput(key, exec1);

        AvroPnlEvent output1 = outputTopic.readKeyValue().value;
        // In Phase 3, this outputs AvroPnlEvent which has UnrlPnL.
        // We just verify it produces an event.
        assertThat(output1.getSymbol()).isEqualTo("AAPL");

        // Duplicate execution should be dropped
        inputTopic.pipeInput(key, exec1);
        assertThat(outputTopic.isEmpty()).isTrue();

        // Second execution
        AvroExecution exec2 = AvroExecution.newBuilder()
                .setId("exec-2")
                .setAccountId("ACC-1")
                .setSymbol("AAPL")
                .setSide("SELL")
                .setQuantity(new BigDecimal("5"))
                .setPrice(new BigDecimal("110"))
                .setCommission(new BigDecimal("2"))
                .setTimestamp(Instant.now())
                .build();

        inputTopic.pipeInput(key, exec2);
        
        AvroPnlEvent output2 = outputTopic.readKeyValue().value;
        assertThat(output2.getSymbol()).isEqualTo("AAPL");
    }
}
