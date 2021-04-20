#!/usr/bin/env bash

set -e
set -u

echo "Running for"
echo "Start date: $START_DATE"
echo "End date: $END_DATE"
echo "Directory for bookings: $BOOKINGS_DIR"
/spark/bin/spark-submit  --class booking.Analysis \
              --name "booking-analysis" \
              --master "local[4]" \
              target/scala-2.12/booking-analysis-0.1.jar \
              "$START_DATE" "$END_DATE" "$BOOKINGS_DIR" data/airports/airports.dat
echo "Completed!"
