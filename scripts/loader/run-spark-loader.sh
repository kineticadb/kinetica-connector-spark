#!/bin/bash

set -o nounset    # give error for any unset variables
set -o errexit    # exit if a command returns non-zero status

_script_dir="$(cd "$(dirname "$(readlink -m "$0")")" && pwd -P)"

if [ $# -ne 1 ]; then
    echo "Usage: $0 [PROPERTY FILE]"
    exit 1
fi

_prop_file=$1

_master='local[8]'
#_master='yarn'
#_master='spark://hive-spark:7077'

set -x
spark-submit \
  --class com.kinetica.spark.SparkKineticaDriver \
  --master $_master \
  --deploy-mode client \
  --packages com.databricks:spark-avro_2.11:4.0.0 \
  --driver-java-options "-Dlog4j.configuration=file:${_script_dir}/log4j.properties" \
  ../../target/kinetica-spark-*-jar-with-dependencies.jar $_prop_file
