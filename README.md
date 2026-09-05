# Equitis PnL Engine 📈 💼

## 📌 Overview
**Equitis PnL Engine** is a high-performance, deterministic financial accounting and PnL calculation engine built with pure Java 25, Kafka Streams, and Spring Boot WebFlux. 

By analyzing real-time trade executions and market ticks, this engine calculates deterministic cost basis, lot matching, Realized PnL, and base Unrealized PnL while maintaining robust state consistency.

**Key Features:**
* **Deterministic Accounting:** Framework-agnostic domain kernel using `java.math.BigDecimal` (DECIMAL128) for high-precision lot matching and calculations.
* **Stream Processing:** Kafka Streams topology backed by RocksDB for maintaining distributed `PositionBook` state and high-throughput ingestion.
* **Risk Aggregation:** Computes margin risk events and bi-temporal corrections for canceled/busted trades.
* **Low Latency:** Tuned RocksDB settings and Java 25 Generational ZGC for sub-millisecond GC pauses.

---

## 🏗 Architecture
The pipeline follows a 5-phase architecture:

1.  **Domain Kernel (Raw):** Pure Java financial math engine for calculating PnL without framework dependencies.
2.  **Kafka Streams Ingress (Stateful):** Wraps the Domain Kernel to ingest Avro payloads and manage the materialized `PositionBook` state via RocksDB.
3.  **Market Data & Conflation (Enriched):** Integrates live Mark-to-Market (MtM) prices and applies FX normalization using `GlobalKTable` lookups, conflating events to reduce egress volume.
4.  **Correction & Egress (Analytics):** Applies retro-active trade corrections and exposes a decoupled Reactive WebFlux gateway for downstream UI/WebSocket consumption.
5.  **Observability (Monitor):** Micrometer/Prometheus integration for tracking latency and memory footprint.

---

## 🛠 Tech Stack
* **Language:** Java 25
* **Stream Processing:** [Kafka Streams](https://kafka.apache.org/documentation/streams/)
* **Framework:** [Spring Boot 4.1.1](https://spring.io/projects/spring-boot) & Spring WebFlux
* **State Store:** [RocksDB](https://rocksdb.org/)
* **Serialization:** [Avro](https://avro.apache.org/) & Confluent Schema Registry
* **Build Tool:** [Maven](https://maven.apache.org/)
* **Infrastructure:** Docker & Docker Compose

---

## 🚀 How to Run

### Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
* **Java 25+** and **Maven** (optional, wrapper `mvnw` is provided).

### 1. Run the Automated Test Suite
The engine includes a comprehensive suite of unit tests for the pure Java domain and `TopologyTestDriver` integration tests for the Kafka Streams topology.

```bash
./mvnw clean test
```

### 2. Start Infrastructure
Start the local Kafka cluster and Zookeeper:

```bash
docker-compose up -d
```
*Wait for Zookeeper and Kafka to fully initialize (usually takes ~15-30 seconds).*

### 3. Create Kafka Topics
Kafka Streams requires the `fx-rates` topic to exist before it can initialize its `GlobalKTable` state store. You can create the required input topics by executing the following commands via the running Kafka container:

```bash
docker-compose exec kafka kafka-topics --create --topic fx-rates --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-topics --create --topic trade-executions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-topics --create --topic market-ticks --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-topics --create --topic trade-lifecycle --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 4. Build and Start the Engine
Use the provided launch script which builds the fat JAR and applies the required `-XX:+UseZGC` tuning for Java 25:

```bash
./mvnw clean package -DskipTests
./start-engine.sh
```
*(To stop the engine gracefully in your terminal, press `Ctrl + C`)*

### 5. Observe Metrics
While the engine is running, you can scrape Prometheus metrics via the Spring Boot Actuator endpoint:
```
http://localhost:8080/actuator/prometheus
```
Key metrics to look for:
- `equitis_pnl_calculation_latency_seconds`: Measures the time taken to apply executions and calculate PnL.
- `equitis_active_position_book_lots`: Tracks the number of active tax lots in memory.

### 6. Stopping
To gracefully stop the local Kafka cluster and remove the containers, run:
```bash
docker-compose down
```
