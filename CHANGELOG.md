# Changelog

## [0.1.1](https://github.com/vmyroslav/kafka-resilience/compare/v0.1.0...v0.1.1) (2026-08-05)


### Bug Fixes

* correct release-please tag format for adapter sub-module ([f731ecb](https://github.com/vmyroslav/kafka-resilience/commit/f731ecbcaeb0ea28d5c24a2a43e7f258c1677059))

## 0.1.0 (2026-02-13)

Initial release of Kafka Resilience library.

### Features

* 3-topic retry pattern (retry, redirect, DLQ) with message ordering guarantees
* Distributed state coordination via compacted Kafka topic
* Configurable backoff strategies (exponential, constant, linear)
* Non-retriable error support for immediate DLQ routing
* OpenTelemetry metrics integratio
