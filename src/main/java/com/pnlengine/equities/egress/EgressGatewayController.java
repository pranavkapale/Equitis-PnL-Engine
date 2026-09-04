package com.pnlengine.equities.egress;

import com.pnlengine.equities.streaming.avro.AvroPnlEvent;
import com.pnlengine.equities.streaming.avro.AvroRiskEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class EgressGatewayController {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;

    public EgressGatewayController(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${spring.kafka.properties.schema.registry.url:mock://localhost:8081}") String schemaRegistryUrl) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    private Map<String, Object> baseConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return props;
    }

    @GetMapping(value = "/pnl/stream/{accountId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<AvroPnlEvent>> streamPnl(@PathVariable String accountId) {
        Map<String, Object> props = baseConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "pnl-stream-group-" + UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        ReceiverOptions<String, AvroPnlEvent> options = ReceiverOptions.<String, AvroPnlEvent>create(props)
                .subscription(Collections.singletonList("pnl-events"));

        return KafkaReceiver.create(options)
                .receive()
                .filter(record -> record.value() != null && accountId.equals(record.value().getAccountId()))
                .map(record -> ServerSentEvent.<AvroPnlEvent>builder()
                        .id(String.valueOf(record.offset()))
                        .event("pnl-update")
                        .data(record.value())
                        .build());
    }

    @GetMapping(value = "/risk/stream/{accountId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<AvroRiskEvent>> streamRisk(@PathVariable String accountId) {
        Map<String, Object> props = baseConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "risk-stream-group-" + UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        ReceiverOptions<String, AvroRiskEvent> options = ReceiverOptions.<String, AvroRiskEvent>create(props)
                .subscription(Collections.singletonList("risk-events"));

        return KafkaReceiver.create(options)
                .receive()
                .filter(record -> record.value() != null && accountId.equals(record.value().getAccountId()))
                .map(record -> ServerSentEvent.<AvroRiskEvent>builder()
                        .id(String.valueOf(record.offset()))
                        .event("risk-update")
                        .data(record.value())
                        .build());
    }
}
