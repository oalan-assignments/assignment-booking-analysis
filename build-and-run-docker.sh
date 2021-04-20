#!/usr/bin/env bash

set -e
set -u

START_DATE=$1
END_DATE=$2
BOOKINGS_DIR=$3

sbt -DsparkDependencyScope=provided clean assembly

docker build --rm=true -t booking-analysis .

docker run --env START_DATE="$START_DATE" \
          --env END_DATE="$END_DATE" \
          --env BOOKINGS_DIR="$BOOKINGS_DIR" \
          booking-analysis