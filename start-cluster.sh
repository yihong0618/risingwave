#!/usr/bin/env bash

# Start postgres first for CDC.
# Start it via a docker service.
start_postgres

# Configure it for CDC
configure_postgres

# Start the full cluster.
./risedev d full-with-connector

# Create tables on rw side for postgres CDC.

# Create tables on rw side for Kafka.