# Commercial Booking Analysis

### Prerequisites

### How to Build (?)

### Usage

## Built With

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
    * Weekday is enum of `(Mon, Tue, Wed, Thu, Fri, Sat, Sun)` and is defined by using `departureDate` 
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

### Data Investigation

TODO:
* Design choices (with pros & cons)
* Data flow

## Future Improvements