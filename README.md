Equities PnL Engine
====================

This repository contains a **sample Equities PnL Engine** designed to target responsibilities of *Equities Transactional Systems Core Technology* jobs.(Work is in progress)

The goal is to demonstrate how I would architect and implement:

- **Trade lifecycle event processing** (including corporate actions and expiries).
- **Python-based transformation and PnL calculation services**.
- **Kafka + Avro-based publishing of transactional and PnL data**.
- **Production-grade engineering practices** (tests, configuration, modular design).

## High-Level Architecture

- **`src/equities_pnl_engine/models`**: Core domain models for trades, corporate actions, and PnL.
- **`src/equities_pnl_engine/schemas`**: Avro schemas for transaction and PnL topics.
- **`src/equities_pnl_engine/pipeline`**: Transformation & PnL calculation logic.
- **`src/equities_pnl_engine/messaging`**: Kafka client abstractions for publishing and consuming events.
- **`src/equities_pnl_engine/services`**: Orchestrating services for end-to-end pipelines (consume → transform → PnL → publish).
- **`src/equities_pnl_engine/cli.py`**: Command-line entrypoints to run batch or streaming jobs.
- **`tests`**: Automated tests for core business logic.

### How it maps to the JD

- **Trade lifecycle event processing**: `models`, `pipeline/transformations.py`, and `services/event_processor.py` handle events like NEW/AMEND/CANCEL/EXPIRY and corporate actions.
- **Python-based services**: `services/event_processor.py` and `cli.py` expose batch and streaming services written in Python.
- **Kafka + Avro publishing**: `messaging/kafka.py` plus `schemas/*.avsc` demonstrate Avro-encoded Kafka topics for trades and PnL.
- **Process improvements / scalability**: clear separation of concerns with configuration in `config.py` and testable core logic in `pipeline`.
- **Modern engineering practices**: `tests/` for automated testing, `requirements.txt` for dependency management, and modular packaging suitable for CI/CD.

The project is intentionally made to target opportunities in the Equities Transactional Systems Technology domain and it's also intentionally put **light framework** as it is in developing phase and to focuses on clear separation of concerns.

**Further considerations**
- I would evolve it for scale (partitioning strategies, backpressure, schema evolution).
- I will try to match the *Principal Responsibilities* that are required to be eligible for Core Equities Transactional Systems Domain.

## Getting Started

```bash
python -m venv .venv
.venv\Scripts\activate  # On Windows

pip install -r requirements.txt
```

> Note: Kafka and Avro dependencies are optional and only required if you want to run the Kafka-based services locally.

## Example Commands

- **Run a simple batch PnL calculation on sample data**

```bash
python -m equities_pnl_engine.cli run-batch-pnl
```

- **Run a streaming trade-event processor (Kafka)**

```bash
python -m equities_pnl_engine.cli run-streaming-processor
```

The CLI commands are minimal but wired through the same pipeline components you would use in a production system.

