# CryptoStream

A real-time + batch data pipeline built with **Kafka**, **Apache Flink**, and **Apache Spark**, streaming live cryptocurrency trade data from Finnhub.

## Overview

![Pipeline architecture](.assets/crypto_pipeline_architecture.png)

## Goals

Demonstrates how three complementary tools fit together in a modern streaming architecture:

- **Kafka** as the durable, decoupled transport layer between components
- **Flink** for low-latency, stateful stream processing (windowing, anomaly detection)
- **Spark** for batch analytics over accumulated data

## Components

### 1. Producer (Python)
![Producer coverage](.assets/coverage-producer.svg)

A small script connects to Finnhub's websocket API, subscribes to a basket of crypto pairs (e.g. `BINANCE:BTCUSDT`, `BINANCE:ETHUSDT`), and publishes each trade event to the `trades` Kafka topic, keyed by symbol.

Messages are serialized using **Avro** (schema defined in `producer/schemas/trade.avsc`) with the Confluent wire format (`\x00` + 4-byte schema ID + Avro binary). On startup the producer registers the schema under subject `trades-value` and obtains its ID from the Schema Registry. BACKWARD compatibility is enforced at the registry level — a schema change that would break existing consumers is rejected.

### 2. Flink job (Scala)
![Flink coverage](.assets/coverage-flink.svg)

Consumes the `trades` topic and:
- Computes tumbling window aggregates (avg/min/max price, volume) per symbol every 10-30 seconds
- Detects price spikes (percentage change between consecutive windows above a threshold)
- Writes window aggregates to `price_aggregates` and spike alerts to `alerts`

### 3. Spark job (Scala)
![Spark coverage](.assets/coverage-spark.svg)

Run on-demand (batch) against accumulated `price_aggregates` data to compute:
- Daily volatility (standard deviation of price) per symbol
- Min/max/avg price per symbol per day
- Top movers ranking

Outputs a summary report to console/CSV.

## Tech stack

- Kafka (KRaft mode, no Zookeeper) — `apache/kafka:3.9.0`
- Confluent Schema Registry — enforces BACKWARD-compatible Avro schemas
- Apache Flink (Scala)
- Apache Spark (Scala)
- Python 3.14 (producer only)
- Docker Compose for orchestration
- SBT for Scala builds

## Project structure

```
CryptoStream/
├── docker-compose.yml
├── pyproject.toml
├── producer/
│   ├── producer.py
│   ├── schemas/
│   │   └── trade.avsc       # Avro schema, registered in Schema Registry on startup
│   └── tests/
├── flink-job/
│   ├── build.sbt
│   └── src/main/scala/...
├── spark-job/
│   ├── build.sbt
│   └── src/main/scala/...
└── README.md
```

## Getting started

### Build order

![Weekend build order](.assets/weekend_build_order.png)

### Prerequisites
- Docker and Docker Compose
- SBT (Scala build tool)
- A free [Finnhub](https://finnhub.io/) API key

### Running the pipeline

1. Start Kafka, Schema Registry, producer, and Kafbat UI:
   ```bash
   docker compose up -d --build
   ```

   - Kafka broker: `localhost:9092`
   - Schema Registry: [http://localhost:8081](http://localhost:8081)
   - Kafbat UI: [http://localhost:8080](http://localhost:8080)

   The producer starts automatically once Kafka is healthy, registers the Avro schema, and begins streaming trades into the `trades` topic.

3. Run the Flink job:
   ```bash
   cd flink-job
   sbt run
   ```

   Reads from `trades`, writes 30-second tumbling window aggregates to `price_aggregates` and spike alerts (≥2% avg price change between windows) to `alerts`. Window size and spike threshold are configurable via `WINDOW_SECONDS` and `SPIKE_THRESHOLD_PCT` env vars.

4. Run the Spark batch job (once some data has accumulated):
   ```bash
   cd spark-job
   sbt run
   ```

## Notes

- Crypto pairs are used because they trade 24/7, providing a continuous stream of real data.

## License

MIT