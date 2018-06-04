#!/bin/bash

set -o nounset    # give error for any unset variables
set -o errexit    # exit if a command returns non-zero status

set -x
spark-submit \
  --deploy-mode client \
  --master 'local[8]' \
  --jars ../../target/kinetica-spark-*-jar-with-dependencies.jar \
  ingest_csv.py
