#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Build deterministic simulation e2e test runner"
ENABLE_RELEASE_PROFILE=true cargo make sslt -- --help

echo "--- Upload artifacts"
cp target/sim/ci-release/risingwave_simulation ./risingwave_simulation
buildkite-agent artifact upload risingwave_simulation
