# Equitis PnL Engine

A high-performance, deterministic financial accounting and PnL calculation engine built with pure Java 25, Kafka Streams, and Spring Boot WebFlux. 

## Architecture

The system is built across 5 phases:
1. **Domain Kernel**: A pure Java, framework-agnostic financial math engine utilizing `java.math.BigDecimal` (DECIMAL128) for deterministic cost basis, lot matching, and Realized/Unrealized PnL calculations.
2. **Kafka Streams Ingress & Materialized State**: Wraps the Domain Kernel in a distributed stream processing topology backed by RocksDB to maintain the `PositionBook` state.
3. **Market Data & Conflation**: Integrates live Mark-to-Market (MtM) prices via market ticks and conflates output emissions to reduce downstream load.
4. **Bi-Temporal Correction & Egress**: Handles canceled/busted trades retroactively, computes margin risk events, and exposes a decoupled Reactive WebFlux gateway for UI/WebSocket consumption.
5. **Production Hardening**: Tuned RocksDB settings, enabled Generational ZGC for sub-millisecond GC pauses, and integrated Micrometer/Prometheus for latency and state size observability.

## Prerequisites

- Java 25 or higher
- Apache Maven
- Docker & Docker Compose (for running local Kafka)

## Testing the Implementation

### 1. Run the Automated Test Suite
The engine includes a comprehensive suite of unit tests for the pure Java domain and `TopologyTestDriver` integration tests for the Kafka Streams topology.

```bash
./mvnw clean test
```

### 2. Run Locally with Kafka

You can run the engine end-to-end on your local machine using the provided `docker-compose.yml` to spin up a local Kafka cluster.

**Step A: Start Kafka**
```bash
docker-compose up -d
```
Wait for Zookeeper and Kafka to fully initialize (usually takes ~15-30 seconds).

**Step B: Create Kafka Topics**
Kafka Streams requires the `fx-rates` topic to exist before it can initialize its `GlobalKTable` state store. You can create the required input topics by executing the following commands via the running Kafka container:
```bash
docker-compose exec kafka kafka-topics --create --topic fx-rates --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-topics --create --topic trade-executions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-topics --create --topic market-ticks --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-topics --create --topic trade-lifecycle --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**Step C: Build and Start the Engine**
Use the provided launch script which builds the fat JAR and applies the required `-XX:+UseZGC` tuning for Java 25:
```bash
./mvnw clean package -DskipTests
./start-engine.sh
```

**Step D: Observe Metrics**
While the engine is running, you can scrape Prometheus metrics via the Spring Boot Actuator endpoint:
```
http://localhost:8080/actuator/prometheus
```
Key metrics to look for:
- `equitis_pnl_calculation_latency_seconds`: Measures the time taken to apply executions and calculate PnL.
- `equitis_active_position_book_lots`: Tracks the number of active tax lots in memory.

### 3. Simulating Data (Manual Testing)

Because the engine relies on Kafka Avro serialization configured with a `mock://` Schema Registry for local development, the easiest way to manually test it is to write a small Kafka Producer script in Java or use a Kafka GUI tool that supports mocking the Confluent Schema Registry. 

Alternatively, you can observe the pipeline's deterministic behavior directly through the `TopologyIntegrationTest.java` and `Phase4TopologyTest.java` files, which simulate data flowing into `trade-executions`, `market-ticks`, and `trade-lifecycle` topics and verify the outbound `pnl-events`.

## Stopping

To gracefully stop the local Kafka cluster and remove the containers, run:
```bash
docker-compose down
```
