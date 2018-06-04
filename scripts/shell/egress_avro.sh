#!/bin/bash

set -o nounset    # give error for any unset variables
set -o errexit    # exit if a command returns non-zero status

set -x
spark-shell \
    --jars ../../target/kinetica-spark-*-jar-with-dependencies.jar \
    --packages com.databricks:spark-avro_2.11:4.0.0 \
    < egress_avro.scala
