# Equitis PnL Engine - Distributed Architecture & Implementation Plan

## Project Overview
The Equitis PnL Engine is a strictly consistent, distributed financial accounting system. It leverages an event-driven topology to process high-frequency market data and trade executions, calculating real-time risk, cost basis, and PnL through a deterministic state machine.

**Core Technology Stack:**
* **Runtime:** Java 25 LTS (utilizing Generational ZGC and Virtual Threads)
* **Framework:** Spring Boot 4.1.x & Spring Kafka 4.1.x
* **Streaming Engine:** Apache Kafka 4.x & Kafka Streams
* **State Store:** RocksDB (Local Materialized State via JNI)
* **Serialization:** Avro with Confluent Schema Registry

## Global Architectural Directives
1. **Financial Precision:** All monetary, volume, and fractional share calculations MUST use `java.math.BigDecimal` (IEEE 754 decimal128 precision, `RoundingMode.HALF_EVEN`). Never use `float` or `double`.
2. **Domain Isolation:** The core accounting logic must be pure Java. Zero Spring, Kafka, or RocksDB dependencies are permitted inside the domain models or PnL calculators.
3. **Partitioning Strategy:** All trades, market ticks, and position state MUST route via a composite key: `accountId|symbol`.
4. **Exactly-Once Semantics (EOS):** The Kafka Streams topology must configure `processing.guarantee="exactly_once_v2"`.

---

## 5-Phase Implementation Roadmap

### Phase 1: Pure Java Domain Kernel & Strict TDD Harness
**Objective:** Build the deterministic financial accounting rules in pure Java, validated by exhaustive parameter-driven tests.
* Create immutable Java 25 records for `Execution`, `PositionBook`, `TaxLot`, and `PnlSnapshot`.
* Implement lot-matching algorithms (FIFO and Weighted Average Cost).
* Calculate Realized PnL (upon liquidation) and base Unrealized PnL.
* Create JUnit 5 test fixtures covering all directional state transitions (BUY→BUY, BUY→SELL, Long→Short, partial fills, cross-zero handling).

### Phase 2: Kafka Streams Topology & State Materialization
**Objective:** Wrap the proven Domain Kernel in the distributed stream processing infrastructure.
* Initialize the Spring Boot 4.1.x project and configure Avro/Schema Registry SerDes.
* Build the Kafka Streams topology defining `trade-executions` as the ingress topic.
* Configure the RocksDB `KeyValueBytesStoreSupplier` to act as the `PositionBook` state store.
* Wire the Phase 1 Domain Kernel into the Kafka Streams Processor API. 
* Implement execution deduplication using cached `executionId`s in RocksDB.

### Phase 3: Valuation, Temporal Joins & Conflation
**Objective:** Introduce live market data and currency normalization for dynamic MtM valuation.
* Ingest the `market-ticks` topic and perform a stream-table temporal join with the RocksDB position store to compute real-time Unrealized PnL.
* Consume the `fx-rates` topic as a `GlobalKTable`. Intercept PnL calculations to dynamically convert foreign-asset PnL into the account's base reporting currency.
* Build an adaptive sliding-window conflator to throttle outbound PnL emissions (e.g., emit only if price delta > 0.05% OR time elapsed > 250ms).

### Phase 4: Advanced Distributed Features & Egress
**Objective:** Implement enterprise-grade risk triggers and bi-temporal correction capabilities.
* **Bi-Temporal Engine:** Process `trade-lifecycle` cancel/amend events. Fetch the affected `PositionBook`, recalculate the historical cost basis from the busted trade's timestamp forward, and emit corrective PnL journals.
* **Risk Triggers:** Continuously monitor aggregate account equity. If Unrealized PnL breaches maintenance margin requirements, emit a `MARGIN_BREACH` payload to a high-priority `risk-events` topic.
* **Egress Gateway:** Build a decoupled Spring WebFlux module to consume the outbound `pnl-events` topic and push updates to clients via WebSockets/SSE.

### Phase 5: Production Hardening, Observability & Tuning
**Objective:** Optimize latency, resilience, and memory management for high-frequency trading constraints.
* Configure Java 25 JVM arguments explicitly for Generational ZGC (`-XX:+UseZGC -XX:+ZGenerational`).
* Implement a `RocksDBConfigSetter` to tune block cache sizes, write buffers, and memory-mapped files.
* Configure Kafka Streams standby replicas (`num.standby.replicas=1`) for instant state failover.
* Instrument the application with Micrometer to expose Prometheus metrics covering Kafka consumer lag, RocksDB JNI latency, PnL calculation time, and ZGC pause durations.