import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.sql.Date

// Define case classes
case class FlightsData(
                        passengerId: Int,
                        flightId: Int,
                        from: String,
                        to: String,
                        date: Date
                      )

case class PassengersData(
                           passengerId: Int,
                           firstname: String,
                           lastname: String
                         )

// Object FlightDataUtils: Contains utility functions for processing flight data analysis tasks
object FlightDataUtils {
  // Function to read input data in CSV format
  def readData(spark: SparkSession, path: String): Dataset[_] = {

    // Read CSV file into DataFrame with headers and inferred schema
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .csv(path)

    // Check if the DataFrame has specific columns
    val hasDateColumn = df.columns.contains("date")
    val hasPassengerIdColumn = df.columns.contains("passengerId")
    val hasFlightIdColumn = df.columns.contains("flightId")

    // Apply transformations if columns exist
    var transformedDf = df
    if (hasDateColumn) {
      transformedDf = transformedDf.withColumn("date", col("date").cast("date"))
    }
    if (hasPassengerIdColumn) {
      transformedDf = transformedDf.withColumn("passengerId", col("passengerId").cast("int"))
    }
    if (hasFlightIdColumn) {
      transformedDf = transformedDf.withColumn("flightId", col("flightId").cast("int"))
    }

    transformedDf
  }

  // Function to write an output answer in CSV format
  def writeOutput(output: DataFrame, N: Int, path: String): Unit = {
    output
      .coalesce(1) // combines all the partitions into a single partition - NOT recommended for very large datasets
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(path + s"Q$N Answer")
  }

  // Q1 SOLUTION
  // Function to calculate the total number of flights for each month (in a specific year)
  def calculateFlightsByMonth(spark: SparkSession, flightsData: Dataset[FlightsData]): DataFrame = {
    import spark.implicits._

    flightsData
      .dropDuplicates("Date", "FlightID") // Assumption: flight IDs are distinct (i.e. no two different flights will have the same ID number).
      .withColumn("Month", month($"date"))
      .groupBy("Month")
      .agg(count("*").as("Number of Flights"))
      .orderBy("Month")
  }

  // Q2 SOLUTION
  // Function to calculate the names of the top N most frequent flyers
  def calculateFrequentFlyers(spark: SparkSession, flightsData: Dataset[FlightsData], passengersData: Dataset[PassengersData], topN: Int): DataFrame = {
    import spark.implicits._

    flightsData
      .groupBy("PassengerID")
      .agg(count("*").as("Number of Flights"))
      .join(passengersData, Seq("PassengerID"))
      .orderBy($"Number of Flights".desc, $"PassengerID".asc)
      .select($"PassengerID".as("Passenger ID"), $"Number of Flights", $"FirstName".as("First name"), $"LastName".as("Last name"))
      .limit(topN)
  }

  // Q3 SOLUTION
  // Function to calculate the longest run without being in a specified country for a single passenger
  def calculateLongestRunUdf(withoutCountry: String) = udf((countrySeq: Seq[String]) => {
    countrySeq
      .mkString(",")
      .split(s",$withoutCountry,")
      .map(_.split(",").distinct.length)
      .max
  })

  // Function to compile the travel sequence for all passengers
  def compileTravelSequenceAllPassengers(spark: SparkSession, flightsData: Dataset[FlightsData]): DataFrame = {
    import spark.implicits._

    val windowSpec = Window.partitionBy("PassengerID").orderBy("Date")

    // Assumption: If a passenger's journey appears across multiple rows for the same date,
    // the travel sequence is determined based on the order of the data rows.
    // This approach may not accurately reflect the actual travel sequence in cases where
    // specific flight times are not available.
    flightsData
      .withColumn("nextFrom", lead("from", 1).over(windowSpec))
      .withColumn("isLastRow", lead("from", 1).over(windowSpec).isNull)
      .withColumn("Countries", when($"isLastRow" || $"to" =!= $"nextFrom", array($"from", $"to")).otherwise(array($"from")))
      .groupBy("PassengerID")
      .agg(flatten(collect_list("Countries")).as("CountrySequence"))
  }

  // Function to calculate the longest run without being in a specified country for all passengers
  def calculateLongestRunAllPassengers(spark: SparkSession, flightsData: Dataset[FlightsData], withoutCountry: String): DataFrame = {
    import spark.implicits._

    compileTravelSequenceAllPassengers(spark, flightsData)
      .withColumn("Longest Run", calculateLongestRunUdf(withoutCountry)($"CountrySequence"))
      .select($"PassengerID".as("Passenger ID"), $"Longest Run")
      .orderBy($"Longest Run".desc, $"PassengerID".asc)
  }

  // Q4 SOLUTION
  // Function to create the passenger pairs on the same flight
  def createPassengerPairs(spark: SparkSession, flightsData: Dataset[FlightsData]): DataFrame = {
    import spark.implicits._

    flightsData
      .as("df1")
      .join(flightsData.as("df2"), $"df1.FlightID" === $"df2.FlightID" && $"df1.PassengerID" < $"df2.PassengerID")
      .select($"df1.PassengerID".as("Passenger1ID"), $"df2.PassengerID".as("Passenger2ID"))
  }

  // Function to calculate the passengers who have been on more than N flights together (Note: Check noisy data - if the same passenger on same flight on same date)
  def calculateFlightsTogether(spark: SparkSession, flightsData: Dataset[FlightsData], minFlights: Int): DataFrame = {
    import spark.implicits._

    createPassengerPairs(spark, flightsData)
      .groupBy("Passenger1ID", "Passenger2ID")
      .count()
      .filter($"count" > minFlights)
      .orderBy($"count".desc, $"Passenger1ID".asc, $"Passenger2ID".asc)
      .withColumnRenamed("count", "Number of flights together")
  }

  // Q5 (Bonus question) SOLUTION
  // Function to filter flights in a specified date range
  def filteredFlightDataInRange(spark: SparkSession, flightsData: Dataset[FlightsData], fromDate: String, toDate: String): DataFrame = {
    import spark.implicits._

    flightsData
      .withColumn("Date", to_date($"Date", "yyyy-MM-dd"))
      .filter($"Date" >= lit(fromDate) && $"Date" <= lit(toDate))
  }

  // Function to create the passenger pairs on the same flight in a specified date range
  def createPassengerPairsInRange(spark: SparkSession, flightsData: Dataset[FlightsData], fromDate: String, toDate: String): DataFrame = {
    import spark.implicits._

    filteredFlightDataInRange(spark, flightsData, fromDate, toDate)
      .as("df1")
      .join(filteredFlightDataInRange(spark, flightsData, fromDate, toDate).as("df2"), $"df1.FlightID" === $"df2.FlightID" && $"df1.PassengerID" < $"df2.PassengerID")
      .select($"df1.PassengerID".as("Passenger1ID"), $"df2.PassengerID".as("Passenger2ID"))
  }

  // Function to calculate the passengers who have been on more than N flights together in a specified date range (from,to)
  def calculateflightsTogetherInRange(spark: SparkSession, flightsData: Dataset[FlightsData], fromDate: String, toDate: String, minFlights: Int): DataFrame = {
    import spark.implicits._

    createPassengerPairsInRange(spark, flightsData, fromDate, toDate)
      .groupBy("Passenger1ID", "Passenger2ID")
      .count()
      .filter($"count" > minFlights)
      .withColumnRenamed("count", "Number of flights together")
      .withColumn("From", lit(fromDate))
      .withColumn("To", lit(toDate))
      .orderBy($"Number of flights together".desc)
  }

  // Function to process data based on selected questions
  def processData(spark: SparkSession, inputPath: String, outputPath: String, questions: Set[Int]): Unit = {
    import spark.implicits._

    // Initialize AppConfig with default values
    val config = AppConfig.default

    // Read the flights and passengers data
    val flightsData = readData(spark, inputPath + "flightData.csv").as[FlightsData]
    val passengersData = readData(spark, inputPath + "passengers.csv").as[PassengersData]

    // Process and write output for each selected question
    if (questions.contains(1)) {
      // Perform Q1 SOLUTION
      val flightsByMonth = calculateFlightsByMonth(spark, flightsData)

      // Write the result of Q1 SOLUTION
      writeOutput(flightsByMonth, 1, outputPath)
    }

    if (questions.contains(2)) {
      // Perform Q2 SOLUTION
      val frequentFlyers = calculateFrequentFlyers(spark, flightsData, passengersData, config.topN)

      // Write the result of Q2 SOLUTION
      writeOutput(frequentFlyers, 2, outputPath)
    }

    if (questions.contains(3)) {
      // Perform Q3 SOLUTION
      val longestRunReport = calculateLongestRunAllPassengers(spark, flightsData, config.withoutCountry)

      // Write the result of Q3 SOLUTION
      writeOutput(longestRunReport, 3, outputPath)
    }

    if (questions.contains(4)) {
      // Perform Q4 SOLUTION
      val flightsTogether = calculateFlightsTogether(spark, flightsData, config.Q4MinFlights)

      // Write the result of Q4 SOLUTION
      writeOutput(flightsTogether, 4, outputPath)
    }

    if (questions.contains(5)) {
      // Perform Q5 SOLUTION
      val flightsTogetherInRange = calculateflightsTogetherInRange(spark, flightsData, config.fromDate, config.toDate, config.Q5MinFlights)

      // Write the result of Q5 SOLUTION
      writeOutput(flightsTogetherInRange, 5, outputPath)
    }
  }
}
