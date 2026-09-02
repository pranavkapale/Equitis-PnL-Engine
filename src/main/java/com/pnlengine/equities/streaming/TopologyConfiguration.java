package com.pnlengine.equities.streaming;

import com.pnlengine.equities.streaming.avro.AvroExecution;
import com.pnlengine.equities.streaming.avro.AvroPositionBook;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class TopologyConfiguration {

    @Value("${spring.kafka.properties.schema.registry.url:mock://localhost:8081}")
    private String schemaRegistryUrl;

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        SpecificAvroSerde<AvroExecution> executionSerde = new SpecificAvroSerde<>();
        executionSerde.configure(Map.of("schema.registry.url", schemaRegistryUrl), false);

        SpecificAvroSerde<AvroPositionBook> positionBookSerde = new SpecificAvroSerde<>();
        positionBookSerde.configure(Map.of("schema.registry.url", schemaRegistryUrl), false);

        KeyValueBytesStoreSupplier positionStoreSupplier = Stores.persistentKeyValueStore("position-store");
        StoreBuilder<KeyValueStore<String, AvroPositionBook>> positionStoreBuilder = 
                Stores.keyValueStoreBuilder(positionStoreSupplier, Serdes.String(), positionBookSerde);

        KeyValueBytesStoreSupplier executionStoreSupplier = Stores.persistentKeyValueStore("processed-executions-store");
        StoreBuilder<KeyValueStore<String, String>> executionStoreBuilder = 
                Stores.keyValueStoreBuilder(executionStoreSupplier, Serdes.String(), Serdes.String());

        streamsBuilder.addStateStore(positionStoreBuilder);
        streamsBuilder.addStateStore(executionStoreBuilder);

        KStream<String, AvroExecution> stream = streamsBuilder.stream("trade-executions", Consumed.with(Serdes.String(), executionSerde));

        stream.process(PositionProcessor::new, "position-store", "processed-executions-store")
              .to("pnl-events", Produced.with(Serdes.String(), positionBookSerde));
    }
}
