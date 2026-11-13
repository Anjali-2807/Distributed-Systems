#!/bin/bash
# Run automated cluster fault tolerance and consistency test suite

cd "$(dirname "$0")/.."
python3 python/cluster_test.py
