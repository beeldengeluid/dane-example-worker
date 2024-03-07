#!/bin/sh

echo "Starting DANE example worker"

poetry run python worker.py "$@"

echo "The worker crashed"