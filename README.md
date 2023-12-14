# Flight Data Analysis Project

## Overview
The Flight Data Analysis project is a Scala-based application designed for processing and analyzing flight data. It utilizes Apache Spark, offering robust capabilities for handling large datasets efficiently.

## Structure
The project is composed of several Scala files, each serving a distinct purpose:

### FlightDataAnalysis.scala
**Purpose:** Acts as the main entry point of the application. It initializes the Spark session and configures the application using predefined settings.

### FlightDataUtils.scala
**Purpose:** Contains utility functions for data processing and analysis. It includes methods for data reading, transformation, and generating analytical insights.

### AppConfig.scala
**Purpose:** Defines a case class `AppConfig` to manage application configuration. It includes input/output paths, task identifiers, and other configurable parameters.

### build.sbt
**Configuration File:** Specifies the build configuration, including Scala version, library dependencies, and other build settings.

## Functionality
This application is capable of executing a variety of analyses on flight and passenger data
1. calculating the total number of flights per month, 
2. identifying frequent flyers,
3. determining the longest travel sequence without visiting a specific country,
4. pairing the passengers who have been on more than the specified number of flights together, and
5. pairing the passengers who have been on more than the specified number of flights together within a specified time range.


## How to Run

### Using SBT
1. Ensure Scala and Spark are installed.
2. Clone/download the project repository.
3. Navigate to the project directory.
4. Run the application using `sbt run`.
5. Results are written to the output directory in CSV format.

### Using IntelliJ IDEA
1. Open IntelliJ IDEA and select "Import Project."
2. Navigate to and select the project's root directory.
3. Choose "sbt" as the build tool.
4. Confirm the import of the project's settings.
5. After the import, navigate to `FlightDataAnalysis.scala` and run it using IntelliJ's run configuration.

## Configuration
Modify `AppConfig.scala` to adjust input/output paths, tasks to run, and other settings. The Scala version used is `2.12.10`.

## Dependencies
- Apache Spark 2.4.8
- ScalaTest 3.2.9

## Scalability and Performance
The application is optimized for a distributed environment. However, note the `.coalesce(1)` statement in `FlightDataUtils.scala`, which may limit scalability for very large datasets. Consider removing or tagging this line to improve performance in such scenarios.

## Testing
Unit tests are uncompleted and will be a future work for the next release.

## Notes
- The application is set up for local execution. For deployment in a production environment, configure Spark to run in a cluster mode.
- Ensure input data matches the format specified in `FlightsData` and `PassengersData` case classes.
- For improved scalability, especially with large datasets, review and adjust the `.coalesce(1)` statement in `FlightDataUtils.scala`.

---