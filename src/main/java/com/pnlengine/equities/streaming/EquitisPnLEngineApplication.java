package com.pnlengine.equities.streaming;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
public class EquitisPnLEngineApplication {
    public static void main(String[] args) {
        SpringApplication.run(EquitisPnLEngineApplication.class, args);
    }
}
