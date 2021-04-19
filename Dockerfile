FROM bde2020/spark-base:3.1.1-hadoop3.2

RUN mkdir -p /app/data/airports
RUN mkdir -p /app/data/bookings
RUN mkdir -p /app/target/scala-2.12
COPY run-local-spark-docker.sh /app
COPY data/airports/airports.dat /app/data/airports/airports.dat
COPY data/bookings/*.json /app/data/bookings/
COPY target/scala-2.12/booking-analysis-0.1.jar /app/target/scala-2.12
WORKDIR /app
RUN chmod +x run-local-spark-docker.sh

ENTRYPOINT ["/app/run-local-spark-docker.sh"]
