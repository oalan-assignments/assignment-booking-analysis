# Commercial Booking Analysis

Most popular destinations by season and weekday for KLM flights originating from the Netherlands.

It takes start date, end date strings in `yyyy-MM-dd` format, path for bookings data and airports data. 

Results are displayed in console (using Spark's `show` function)

### Prerequisites

For able to run in self-contained form using [script](build-and-run-docker.sh)
* Docker  

Build done locally. Therefore: 
* Scala 2.12
* Sbt 1.4.7+
* Java 8+

For local runs using [script](build-and-run.sh) additionally 
* Spark 3.1.1

### How to Build 

```
sbt -DsparkDependencyScope=provided clean assembly
```

### Usage

Use either:
* [run-in-docker.sh](run-in-docker.sh) 
* [run.sh](run.sh)

which provides defaults for date and data paths

If you want to provide different dates and bookings path:
* For local [build-and-run.sh](build-and-run.sh)  
`./build-and-run.sh 2021-01-01 2021-04-20 path-to-bookings`
* For docker [build-and-run-docker.sh](build-and-run-docker.sh)  
  It is using default data folder for local spark-submit though.
  
If there is an intention to run the code in a cluster (e.g. YARN), scripts should be modified by replacing `--master "local[4]` with `--master yarn --deploy-mode cluster`

## Built With

* [Spark 3.1.1](https://spark.apache.org/releases/spark-release-3-1-1.html)
* [Scala 2.12.4](https://www.scala-lang.org/download/2.12.4.html)
* [upickle 0.9.5](https://github.com/com-lihaoyi/upickle) for json line parsing
* [os-lib 0.7.3](https://github.com/com-lihaoyi/os-lib) for path operations

## Design

### Translated Requirements

#### Functional

* Inputs:
    * URI (local | HDFS) for booking files (**dynamic**)
    * Start and end date for the analysis period (**dynamic**) 
    * Airports dataset (**static**)
* Output: A table that displays most popular destinations (countries) by showing number of passengers per season per weekday
    * Minimal schema for output: `Most-Booked(season, weekday, country, number-of-passengers)`
    * Season is enum of `(Winter, Spring, Summer, Autumn)` and is defined by Northern hemisphere meteorological dates
    * Weekday is enum of `(Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday)` and is defined by using `departureDate` 
* Business Rules/Constraints:
    * For a booking to be included in the analysis:
        * Last `bookingStatus` in `productList` should be `Confirmed`
        * `originAirport` should be in the Netherlands
        * `operatingAirline` should be KLM
    * Use `distinct` passengers per flight leg to avoid double count. There can be multiple events for the same passenger.
    * UTC time should be converted to local timezone for deducing `weekday`

#### Non-functional

* Should scale to 100s of TBs
* Should be capable of using partitioned files in a certain directory
* Should run locally or in a YARN cluster
* Solution should seek a balance between performance and maintainability (readable & simple)
* Should handle invalid input data

#### Some assumptions I made:

* After exploring the data I noticed there are bookings which contains more than one KLM flights originating from the Netherlands (up to 4 at most). I processed all of them in the report. Some of them seems like connecting flights. So, I am not sure if those flights should be included. Considering I have limited domain understanding, I did not want to make more assumptions and included all those flights.
* I used `destinationAirport` information only. I noticed in some bookings destination is just a connection and most probably many flights originating from the Netherlands has a final destination. I looked at `yieldTripDestination` and `yieldDestination` but could not find a consistent pattern in the dataset. Also I did not want to make more than one assumption. Therefore, current code use `destinationAirport` info of flight leg rather than the final destination.


### Data Investigation

#### Design

Code is organised in packages as:
* `domain`: contains `Booking` class to contain `Booking` case class and methods for
    * parses a booking (json line) partially (only necessary fields)
    * performing several checks on bookings, used for filtering data
* `input`: contains:
    * `Airports`: loads airports data (by filtering records with null fields that we intent to use) and convert it to a lookup (airport -> country and airport -> timezone) which is later broadcasted
    * `Bookings`: loads bookings data by parsing json (and filtering our invalid entries), also provides a method to filter bookings which are eligible for analysis (bookings that contain any KLM flight originating from the Netherlands in the given period)
* `analysis`: contains `Report` object applies following transformations to reach a report:
    * `flattenFlighs`: a flatMap on bookings so a booking that contains more than 1 eligible KLM flight converted into many records (`FlightWithPassengersData`)
    * `toAnalysisDataSet`: converts flatten flights to a pair of key (`Destination Country`, `Season`, `Weekday`) and data (`#of Passengers`, `#of Adults`, `#of Children`, `Weight sum`, `Age sum`, `#of Passengers with Age info (used as denominator)`). **At this stage different events that contain same flights are handled to avoid double counts of passengers** 
    * `aggregateToFinalReport`: reduces the dataset by taking sums and averages on grouped data and **sorts it in descending order by `#of Passengers`**

#### Design Choices

* Used `Spark` which suited the task since it is batch-first use case. In case, there is a desire to use streaming data `Spark`'s Streaming API could be used with little change.
* Used mostly `DataSet` api to utilise case classes and methods on them to filter/transform. It yields to a code that promotes FP (in contrast to declarative sql api), that provides type safety to some degree. Finally give room for out of the box optimizations (via Tungsten and Catalyst)
* Used `upickle` and did selective/partial parsing of booking records. It gave more control for error handling in contrast to using Spark's json reading capability (which has also some invalid record handling but harder to control). Also this approach (due to partial parsing) yielded to a narrow/simpler case class (`Booking`)
* Airports are loaded into a lookup (`map`) and broadcasted to workers since the resulting data is pretty small and static (will not grow much further)
* Used [docker base image](https://github.com/big-data-europe/docker-spark) to provide a self-contained environment to run without dealing with dependencies (particularly spark), however sbt compilation is very slow in the container, so that has to be done locally unfortunately.



## Future Improvements

If I could make more time, I would implement:
* more data validations
* a few more Integration Tests (e.g. for `Report.run`)
* nicer way to present report (one small iteration could be generating html and expose from docker container)
* separate unit tests and integration tests
* sbt build in the container 
* spark streaming
* finally would like to check data to detect inconsistencies (some avg age values look weird)

Things to consider, if this code will be evolved into a production system:
* Job could be broken into two steps (filtering and producing the report) by using Airflow/Luigi to avoid a monolithic job. That way failed steps could be retried and also it would be easier to maintain the job
* After filtering data intermediate result could be written in `parquet` format (considering columnar format suits querying better) or if there is an intention to perform adhoc queries and make reports, ingesting into a database would be a good idea.
* Monitoring/Metrics/Alerts for the job, input validations. I implemented some invalid data handling in json parsing. Using similar pattern (using Try/Success/Failure) and logging (or persisting) invalid records could be implemented.
* Providing an initial load routine (for retrospective data and in case of undetected failures)
* Retention of data (particularly PII) and cleaning up has to be discussed and designed