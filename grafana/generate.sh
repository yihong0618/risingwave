#!/usr/bin/env bash

set -euo pipefail

generate-dashboard -o risingwave-dev-dashboard.gen.json risingwave-dev-dashboard.dashboard.py
generate-dashboard -o risingwave-user-dashboard.gen.json risingwave-user-dashboard.dashboard.py

jq -c . risingwave-dev-dashboard.gen.json > risingwave-dev-dashboard.json
jq -c . risingwave-user-dashboard.gen.json > risingwave-user-dashboard.json
cp risingwave-user-dashboard.json ../docker/dashboards/
cp risingwave-dev-dashboard.json ../docker/dashboards/