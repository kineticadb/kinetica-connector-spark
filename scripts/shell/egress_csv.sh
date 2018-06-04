#!/bin/bash

set -o nounset    # give error for any unset variables
set -o errexit    # exit if a command returns non-zero status

set -x
spark-shell \
    --jars ../../target/kinetica-spark-*-jar-with-dependencies.jar \
    < egress_csv.scala
