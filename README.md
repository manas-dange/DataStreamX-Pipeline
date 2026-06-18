# DataStreamX: Local Data Pipeline Demo

This project provides a simplified, enterprise-grade data pipeline architecture designed to demonstrate how modern data systems—like those used by Apache Kafka and Snowflake—handle ingestion, real-time transformation, and analytical storage.

## Architecture Overview

The system uses Docker to provide an isolated, production-like environment on your local machine, consisting of three main parts:

1. Apache Kafka: Acts as the high-throughput message broker. It decouples the data producers (the sources) from the consumers (the processing logic), ensuring the system can handle bursts of data without crashing.
2. PostgreSQL: Serves as the analytical data warehouse. This is where your transformed, structured telemetry data is stored so it can be queried for business reports and analytics.
3. Python Pipeline: This is the "brain" of the operation. It implements the logic for producing (ingesting) raw IoT data and consuming that data stream to perform transformations and storage.

## Prerequisites

To run this demonstration, you will need:

* Docker Desktop installed and running.
* Python 3.x installed.
* Required Python libraries:
  pip install kafka-python psycopg2-binary

## Setup Instructions

1. Ensure your project directory contains both `docker-compose.yml` and `pipeline_demo.py`.
2. Start the Infrastructure:
   In your terminal, inside the project directory, run:
   docker compose up -d
   (Wait about 15–20 seconds for Kafka and PostgreSQL to fully initialize).
3. Run the Pipeline:
   Execute the Python script:
   python pipeline_demo.py

The script will automatically:

* Initialize the database table.
* Ingest mock IoT telemetry data into Kafka.
* Process the stream by converting temperatures and flagging critical status.
* Store the results in PostgreSQL.
* Display the final contents of the data warehouse in your terminal.

## What is this project about?

DataStreamX is a case study implementation of a scalable, fault-tolerant data pipeline. The purpose of this project is to demonstrate:

* Decoupling: Using Kafka to manage massive streams of data without bottlenecking the processing layer.
* Stream Processing: Applying real-time transformations and business logic (like temperature unit conversion and critical status flagging) before data reaches the final storage.
* Polyglot Persistence: Using different storage strategies to handle raw ingestion versus structured analytical data.
* Environment Parity: Using Docker containers to ensure that the code developed on a laptop behaves identically to a large-scale cloud-based enterprise deployment.

This demo serves as a Proof of Concept (PoC) for enterprise-scale data engineering.
