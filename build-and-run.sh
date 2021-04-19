#!/usr/bin/env bash

set -e
set -u

echo "Building the jar"
sbt -DsparkDependencyScope=provided clean assembly
START_DATE=$1
END_DATE=$2
BOOKINGS_DIR=$3
echo "Running for"
echo "Start date: $START_DATE"
echo "End date: $END_DATE"
echo "Directory for bookings: $BOOKINGS_DIR"
spark-submit  --class booking.Analysis \
              --name "booking-analysis" \
              --master "local[4]" \
              target/scala-2.12/booking-analysis-0.1.jar \
              $START_DATE $END_DATE $BOOKINGS_DIR data/airports/airports.dat
echo "Completed!"
