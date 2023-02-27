#!/bin/bash

# setup script
set -e
cd "$(dirname "$0")"

# build and run tests
./scripts/build.sh mt_tests RELEASE
./cmake-build-release/src/mt_tests $@
