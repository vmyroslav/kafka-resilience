# Changelog

## 0.1.0 (2026-02-13)

Initial release of Kafka Resilience library.

### Features

* 3-topic retry pattern (retry, redirect, DLQ) with message ordering guarantees
* Distributed state coordination via compacted Kafka topic
* Configurable backoff strategies (exponential, constant, linear)
* Non-retriable error support for immediate DLQ routing
* OpenTelemetry metrics integratio
