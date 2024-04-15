# Overview

This repo contains dags for collecting currency pairs, both the current FX exchange rate and an indication of the change compared to yesterday’s rate at 5PM New York time.

The structure of repository is taken from [Astro](https://github.com/astronomer/airflow-quickstart). 

## Implementation

The task implemented using Apache Airflow. The logic is to ingest the data into `rate_event` table, then, executuon time is 5PM in New York - update `comparable_rate` table with newest rates to compare. Lastly, insert active rates into `display_rates` table. The logic is the same for all dags. 

Implementation is not fully complete, because it's more to demonstrate my working style than to create a project for production, so there are comments for these improvements in the code.

## Dags
This repository contains 5 DAGs:

- `A-hourly-job`: This DAG ingests rates from the file hourly, transforms the data and writes rates for display into the `display_rate` table.
- `B-minute-job`: This DAG ingests rates from the file every minute, transforms the data and writes rates for display into the 'display_rate' table.
- `listen_to_the_stream`: This DAG listens to a Kafka stream and triggers a task when a new message is received.
- `refresh_rates`: This DAG refreshes rates by inserting Kafka source data, refreshing comparable rates, and reading and writing the transformed data (same as A or B jobs, but only when triggered).
- `produce_rate_event`: This DAG produces rate events to a Kafka topic.

## Data storage

For data storage Postges database is used and there are 3 tables:
- `display_rates`: contains data for LED screen.
- `rate_event`: contains all currency events.
- `comparable_rate`: contains rates for yesterday 5PM New York time.

## Run locally

1. Install the Astro CLI by following the steps in the [Astro CLI documentation](https://docs.astronomer.io/astro/cli/install-cli). Docker Desktop/Docker Engine is a prerequisite.
2. Run `astro dev start` in your cloned repository.
4. Connect to Postgres database ([instructions](https://www.commandprompt.com/education/how-to-create-a-postgresql-database-in-docker/)) and run these commands:
    * `CREATE TABLE display_rate(ID INT PRIMARY KEY NOT NULL, CCY_COUPLE TEXT NOT NULL, RATE DOUBLE PRECISION NOT NULL, CHANGE REAL NOT NULL);`
    * `CREATE TABLE rate_event(EVENT_ID BIGINT PRIMARY KEY NOT NULL, EVENT_TIME BIGINT NOT NULL, CCY_COUPLE TEXT NOT NULL, RATE DOUBLE PRECISION NOT NULL);`
    * `CREATE TABLE comparable_rate(ID BIGINT PRIMARY KEY NOT NULL, EVENT_TIME BIGINT NOT NULL, CCY_COUPLE TEXT NOT NULL, RATE DOUBLE PRECISION NOT NULL);`
3. After your Astro project has started. View the Airflow UI at `localhost:8080`.

