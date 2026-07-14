# Distributed Ticketing Engine

An event-driven reservation system built to handle high-concurrency seat booking and real-time search indexing without double-booking or database locking.

## Architecture & Data Flow

This project implements a CQRS (Command Query Responsibility Segregation) pattern to separate transactional writes from text-based reads.

*   **Transactional Database (Cassandra):** Handles all seat reservations. Utilizes Lightweight Transactions (LWT) and Paxos consensus to serialize concurrent booking requests, completely eliminating double-booking during flash sales.
*   **Event Broker (Kafka):** Acts as the asynchronous bridge. Every new movie created in Cassandra pushes an event to a Kafka topic, ensuring the transactional database is never slowed down by read-heavy operations.
*   **Search Engine (Elasticsearch):** Consumes the Kafka stream via a Sink Connector to build a low-latency inverted index for instantaneous text searching.
*   **Distributed Cache (Redis):** Manages temporary state. When a seat is locked, a Redis TTL key is generated. If payment is not completed within 10 minutes, Redis Keyspace Notifications trigger a Node.js listener to automatically revert the Cassandra row back to 'available'.

## Tech Stack
*   **Backend:** Node.js, Express
*   **Databases/Infrastructure:** Apache Cassandra, Elasticsearch, Redis, Apache Kafka, Zookeeper
*   **Deployment:** Docker, Docker Compose

## Features
*   **Race-Condition Protection:** Database-level CAS (Compare-And-Set) operations prevent simultaneous seat purchases.
*   **Automated State Rollback:** 10-minute temporary locks using Redis TTL and event listeners.
*   **Asynchronous Indexing:** Fire-and-forget Kafka streaming keeps write-latencies under 50ms.

## Local Setup
1. Clone the repository.
2. Run `docker-compose up -d` to spin up the cluster (Node API, Cassandra, Kafka, Redis, Elasticsearch).
3. Wait 60 seconds for Cassandra and Kafka to initialize.
4. (Include any database initialization scripts or API setup curls here).