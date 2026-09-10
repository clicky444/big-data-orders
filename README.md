# Kafka Order Processing Assignment

A Node.js application that sends Avro-encoded purchase orders through Apache Kafka, calculates a running average of successfully processed prices, retries temporary processing failures, and routes failed orders to a Dead Letter Queue (DLQ).

## Requirements demonstrated

| Requirement | Implementation |
| --- | --- |
| Produce order messages | `producer.js` sends 10 orders, approximately one second apart. |
| Avro serialization | `order.avsc` defines the order; `avsc` encodes and decodes message values. |
| Real-time aggregation | `consumer.js` updates the total, count, and average after successful processing. |
| Retry temporary failures | Up to 3 processing attempts, with a 1-second delay between attempts. |
| Dead Letter Queue | Invalid orders and orders that exhaust retries are published to `orders-dlq`. |
| Inspect failures | `dlq-consumer.js` displays the original order and failure metadata. |

## How it works

The producer publishes Avro binary values to the `orders` topic. The consumer decodes each value and attempts to process the order. After success, it updates the running average. A permanent validation failure skips retries; a temporary failure is retried up to the attempt limit. Failed orders are sent to `orders-dlq` and excluded from the average. A separate consumer reads the DLQ for inspection.

The order consumer also acts as a Kafka producer when publishing to the DLQ. It waits for the DLQ send to succeed before returning from the message handler. A failed DLQ send propagates as an error rather than silently skipping the order.

## Technology

- Node.js and npm
- Apache Kafka Docker image: `apache/kafka:4.3.1`
- Docker Compose v2
- KafkaJS: Kafka producer, consumer, and admin clients
- avsc: Avro encoding and decoding

Dependency versions are recorded in `package-lock.json`. Use `npm ci` to install them.

## Project files

| File | Purpose |
| --- | --- |
| `compose.yaml` | Starts Kafka and checks broker readiness. |
| `setup-topics.js` | Creates `orders` and `orders-dlq` if they do not exist. |
| `order.avsc` | Shared Avro order schema. |
| `kafka-client.js` | Shared IPv4 Kafka connection configuration and Avro type. |
| `producer.js` | Generates the demonstration orders and encodes them with Avro. |
| `consumer.js` | Processing, retries, running average, and DLQ publishing. |
| `dlq-consumer.js` | Decodes and displays failed orders. |
| `package.json` | Dependencies and npm commands. |
| `package-lock.json` | Locked dependency versions. |
| `.gitignore` | Excludes dependencies, environment files, and logs. |

## Order schema

The Avro record is named `Order`, in namespace `com.assignment.orders`.

| Field | Avro type | Meaning |
| --- | --- | --- |
| `orderId` | `string` | Unique order identifier generated with `randomUUID()`. |
| `product` | `string` | Product name, such as `Item1`. |
| `price` | `float` | Product price. |

Both applications use the same local schema file; this project does not use a Schema Registry. Message values are Avro binary data, even though console output displays readable fields.

## Prerequisites

- Node.js with npm, supporting `node:crypto.randomUUID` and `node:timers/promises`.
- Docker Desktop running Linux containers, with Docker Compose v2 supporting `--wait`.
- Git to clone the repository.
- Local port `9092` available.

Verify the environment:

```powershell
node --version
npm --version
docker info
docker compose version
git --version
```

## Setup

Clone or download this repository and open a terminal in its root folder, where `package.json` is located.

If the earlier manually created `kafka-assignment` container is running, stop it first to free port 9092:

```powershell
docker stop kafka-assignment
```

That command is only needed for an existing container with that name.

Install dependencies, start Kafka, and create the topics:

```powershell
npm ci
npm run kafka:up
npm run setup
```

Run each command after the previous one completes successfully. The Kafka startup command waits for the broker health check. Topic creation can be repeated without deleting existing messages.

## Run the live demonstration

Open three terminals in the project root.

**Terminal 1: order consumer**

```powershell
npm run consumer
```

**Terminal 2: DLQ reader**

```powershell
npm run dlq
```

Wait for both consumers to join their groups.

**Terminal 3: producer**

```powershell
npm run producer
```

The producer sends 10 orders and exits. Both consumers remain running until stopped with Ctrl+C.

## Demonstration scenarios

Failure simulation is deliberately built into the current demonstration code. It is not evidence of a real downstream outage.

| Order | Processing behavior | Expected result |
| --- | --- | --- |
| `Item3` | Simulated temporary failure on attempts 1 and 2. | Succeeds on attempt 3 and contributes to the average once during normal processing. |
| `Item5` | Producer assigns price `-50`. | Fails validation on attempt 1; sent to DLQ as `permanent`. |
| `Item7` | Simulated temporary failure on every attempt. | Sent to DLQ as `retries-exhausted` after attempt 3. |
| Other items | Valid orders without simulated failures. | Succeed on attempt 1. |

Three attempts means one initial attempt and two retries. Only temporary processing errors are retried by the application. These are separate from KafkaJS's connection and broker-request retries.

A negative number is valid under the Avro `float` schema. The negative-price rejection is an application validation rule.

For one new batch with no backlog and a newly started order consumer, expect:

- 10 orders produced.
- 8 orders successfully processed.
- 2 orders displayed by the DLQ reader.
- Processing continues through `Item10` despite earlier failures.
- The running average includes the 8 successful orders only.

Prices and IDs vary between runs. The final successful count is cumulative within one consumer process; sending another batch without restarting it increases the successful count by another 8.

## Running average

```text
runningAverage = totalPrice / successfulOrderCount
```

The consumer updates the total and count only after processing succeeds. It displays prices and averages to two decimal places. Avro `float` uses binary floating-point representation, so small representation differences are expected; this is a demonstration rather than a financial accounting implementation.

## DLQ message format

The DLQ preserves the original order key and Avro-encoded value. Kafka headers carry:

| Header | Meaning |
| --- | --- |
| `failure-reason` | Error description. |
| `failure-type` | `permanent` or `retries-exhausted`. |
| `attempts` | Number of processing attempts. |
| `source-topic` | Original topic. |
| `source-partition` | Original partition. |
| `source-offset` | Original Kafka message offset. |
| `failed-at` | Failure timestamp. |

Reading the DLQ does not delete its messages or automatically retry them.

## Commands

| Command | Action |
| --- | --- |
| `npm run kafka:up` | Run `docker compose up -d --wait`. |
| `npm run kafka:stop` | Stop the Compose-managed Kafka container. |
| `npm run setup` | Create the required topics. |
| `npm run producer` | Generate a batch of demonstration orders. |
| `npm run consumer` | Process orders. |
| `npm run dlq` | Inspect failed orders. |

Stop both consumers with Ctrl+C before stopping Kafka:

```powershell
npm run kafka:stop
```

## Restart behavior and limitations

- **The average is stored in memory.** Its total and count reset when the order consumer restarts. It represents successful processing in that process, not a durable all-time average.
- **Consumer positions are separate from the average.** Kafka stores committed offsets for `order-average-group` and `dlq-inspector-group`. On restart, a group normally resumes at its committed position. `fromBeginning: true` selects the beginning only when there is no valid committed offset; it does not force a full replay on every run.
- **No exactly-once guarantee.** Automatic offset commits are not atomic with the in-memory average or DLQ publishing. A crash or rebalance can cause replay and duplicate processing or duplicate DLQ entries. The original topic, partition, and offset identify a source message for investigation.
- **One broker and one partition per topic.** This keeps processing sequential for the assignment. It does not demonstrate broker fault tolerance or a distributed global average.
- **No persistent Docker volume is configured.** Stopping and starting the same container retains its writable layer; removing or recreating the container can lose Kafka data and consumer offsets.
- **Failures are simulated by product name.** Every `Item3` and `Item7` follows the demonstration rules. The producer deliberately assigns `Item5` a negative price.
- **Malformed Avro is not routed to the DLQ in this version.** Decoding errors and unexpected programming errors propagate. The demonstrated DLQ handles decoded orders that fail application validation or exhaust processing retries.
- **Local development configuration.** Connections use IPv4 on localhost without TLS or authentication.

Possible extensions include durable aggregation with coordinated offset storage, replay deduplication, malformed-message handling, and configurable failure simulation.

## Troubleshooting

**Port 9092 is already in use:** stop any earlier Kafka container or other service using that port before starting Compose.

**Cannot connect to Docker:** open Docker Desktop and wait for its engine to start; check `docker info`.

**Kafka does not become healthy:** inspect the service and logs:

```powershell
docker compose ps
docker compose logs --tail 100 kafka
```

**Connection refused at `::1:9092`:** ensure the applications retain the working IPv4 socket configuration with `family: 4`.

**Consumer waits without printing orders:** it may already have consumed the available messages. Keep it running and produce a new batch. Also check that another instance of the same consumer group is not already processing the partition.

**A group-coordinator error appears at startup:** startup can involve retries. A subsequent successful group join and message processing demonstrate recovery. If errors persist, inspect the Kafka logs.

