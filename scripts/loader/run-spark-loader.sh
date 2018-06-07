#!/bin/bash

set -o nounset    # give error for any unset variables
set -o errexit    # exit if a command returns non-zero status

if [ $# -ne 1 ]; then
    echo "Usage: $0 [PROPERTY FILE]"
    exit 1
fi

_prop_file=$1

_master='local[8]'
#_master='yarn'
#_master='spark://hive-spark:7077'

_param_master=

if [ -n "$_master" ]; then
  echo "Using master: $_master"
  _param_master="--master $_master"
fi

set -x
spark-submit \
  --class com.kinetica.spark.SparkKineticaDriver \
  $_param_master \
  --deploy-mode client \
  --packages com.databricks:spark-avro_2.11:4.0.0 \
  --driver-java-options "-Dlog4j.configuration=file:$_logger_config" \
  ../../target/kinetica-spark-*-jar-with-dependencies.jar $_prop_file
