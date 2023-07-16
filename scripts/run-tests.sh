#!/usr/bin/env bash
set -e

TEST_PATH="tests/unit"
DATA_DIR="coverage"

# run unit tests
UNIT_DATA_FILE="${DATA_DIR}/.coverage.unit"
coverage run --data-file=$UNIT_DATA_FILE -m pytest $TEST_PATH
coverage html --data-file=$UNIT_DATA_FILE -d coverage/html/unit
coverage xml --data-file=$UNIT_DATA_FILE -o coverage/coverage.xml

# run integration tests
INTEGRATION_DATA_FILE="${DATA_DIR}/.coverage.integration"
coverage run --data-file=$INTEGRATION_DATA_FILE -m pytest tests/integration
coverage html --data-file=$INTEGRATION_DATA_FILE -d coverage/html/integration
coverage xml --data-file=$INTEGRATION_DATA_FILE -o coverage/coverage.xml

# combine coverage data
coverage combine --append $UNIT_DATA_FILE $INTEGRATION_DATA_FILE
coverage html -d coverage/html/combined
coverage xml -o coverage/coverage.xml

# show coverage report & fail if under 100% test coverage
coverage report --fail-under=100
