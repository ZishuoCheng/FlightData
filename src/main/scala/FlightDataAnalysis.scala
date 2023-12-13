import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.sql.Date

// Define case classes
case class FlightsData(
                        passengerId: String,
                        flightId: String,
                        from: String,
                        to: String,
                        date: Date
                      )

case class PassengersData(
                           passengerId: String,
                           firstname: String,
                           lastname: String
                         )

// Main method
object FlightDataAnalysis extends App {

  // Spark session
  val spark = SparkSession.builder()
    .appName("Flight Data Analysis")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Function to read flight data in CSV format
  def readFlightsData(path: String): Dataset[FlightsData] = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .withColumn("date", col("date").cast("date")) // Convert timestamp to date
      .as[FlightsData]
  }

  // Function to read passenger data in CSV format
  def readPassengersData(path: String): Dataset[PassengersData] = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .as[PassengersData]
  }

  // Function to write an output answer in CSV format
  def writeOutput(output: DataFrame, N: Int, path: String): Unit = {
    output
//      .coalesce(1) // combines all the partitions into a single partition - NOT recommended for very large datasets
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(path + s"Q$N Answer")
  }

  // Q1 SOLUTION
  // Function to calculate the total number of flights for each month (in a specific year)
  def calculateFlightsByMonth(flightsData: Dataset[FlightsData]): DataFrame = {
    flightsData
      .dropDuplicates("Date", "FlightID") // Assumption: a flight appears only once on the same date.
      .withColumn("Month", month(col("date")))
      .groupBy("Month")
      .agg(count("*").as("Number of Flights"))
      .orderBy("Month")
  }

  // Q2 SOLUTION
  // Function to calculate the names of the top N most frequent flyers
  def calculateFrequentFlyers(flightsData: Dataset[FlightsData], passengersData: Dataset[PassengersData], topN: Int): DataFrame = {
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
    val delimiter = s",$withoutCountry,"
    val runs = countrySeq.mkString(",").split(delimiter).map(_.split(",").length)
    if (runs.isEmpty) 0 else runs.max
  })

  // Function to compile the travel sequence for all passengers
  def compileTravelSequenceAllPassengers(flightsData: Dataset[FlightsData]): DataFrame = {
    val windowSpec = Window.partitionBy("PassengerID").orderBy("Date")
    flightsData
      .withColumn("nextFrom", lead("from", 1).over(windowSpec))
      .withColumn("isLastRow", lead("from", 1).over(windowSpec).isNull)
      .withColumn("Countries", when($"isLastRow" || $"to" =!= $"nextFrom", array($"from", $"to")).otherwise(array($"from")))
      .groupBy("PassengerID")
      .agg(flatten(collect_list("Countries")).as("CountrySequence"))
  }

  // Function to calculate the longest run without being in a specified country for all passengers
  def calculateLongestRunAllPassengers(flightsData: Dataset[FlightsData], withoutCountry: String): DataFrame = {
    compileTravelSequenceAllPassengers(flightsData)
      .withColumn("Longest Run", calculateLongestRunUdf(withoutCountry)($"CountrySequence"))
      .select($"PassengerID".as("Passenger ID"), $"Longest Run")
      .orderBy($"Longest Run".desc)
  }

  // Q4 SOLUTION
  // Function to create the passenger pairs on the same flight
  def createPassengerPairs(flightsData: Dataset[FlightsData]): DataFrame = flightsData
    .as("df1")
    .join(flightsData.as("df2"), $"df1.FlightID" === $"df2.FlightID" && $"df1.PassengerID" < $"df2.PassengerID")
    .select($"df1.PassengerID".as("Passenger1ID"), $"df2.PassengerID".as("Passenger2ID"))

  // Function to calculate the passengers who have been on more than N flights together (Note: Check noisy data - if the same passenger on same flight on same date)
  def calculateFlightsTogether(flightsData: Dataset[FlightsData], minFlights: Int): DataFrame = createPassengerPairs(flightsData)
    .groupBy("Passenger1ID", "Passenger2ID")
    .count()
    .filter($"count" > minFlights)
    .orderBy($"count".desc, $"Passenger1ID".asc, $"Passenger2ID".asc)
    .withColumnRenamed("count", "Number of flights together")

  // Q5 (Bonus question) SOLUTION
  // Function to filter flights in a specified date range
  def filteredFlightDataInRange(flightsData: Dataset[FlightsData], fromDate: String, toDate: String): DataFrame = flightsData
    .withColumn("Date", to_date($"Date", "yyyy-MM-dd"))
    .filter($"Date" >= lit(fromDate) && $"Date" <= lit(toDate))

  // Function to create the passenger pairs on the same flight in a specified date range
  def createPassengerPairsInRange(flightsData: Dataset[FlightsData], fromDate: String, toDate: String): DataFrame = filteredFlightDataInRange(flightsData, fromDate, toDate)
    .as("df1")
    .join(filteredFlightDataInRange(flightsData, fromDate, toDate).as("df2"), $"df1.FlightID" === $"df2.FlightID" && $"df1.PassengerID" < $"df2.PassengerID")
    .select($"df1.PassengerID".as("Passenger1ID"), $"df2.PassengerID".as("Passenger2ID"))

  // Function to calculate the passengers who have been on more than N flights together in a specified date range (from,to)
  def calculateflightsTogetherInRange(flightsData: Dataset[FlightsData], fromDate: String, toDate: String, minFlights: Int): DataFrame = createPassengerPairsInRange(flightsData, fromDate, toDate)
    .groupBy("Passenger1ID", "Passenger2ID")
    .count()
    .filter($"count" > minFlights)
    .withColumnRenamed("count", "Number of flights together")
    .withColumn("From", lit(fromDate))
    .withColumn("To", lit(toDate))
    .orderBy($"Number of flights together".desc)

  // Specify the path of inputs
  val inputPath = "src/main/resources/inputs/"

  // Specify the path of outputs
  val outputPath = "src/main/resources/outputs/"

  // Read the flights data
  val flightsData = readFlightsData(inputPath + "flightData.csv")

  // Read the passengers data
  val passengersData = readPassengersData(inputPath + "passengers.csv")

  // Perform Q1 SOLUTION - find total number of flights for each month (in 2017)
  val flightsByMonth = calculateFlightsByMonth(flightsData)

  // Write the result of Q1 SOLUTION
  writeOutput(flightsByMonth, 1, outputPath)

  // Perform Q2 SOLUTION - find the names of the 100 most frequent flyers
  val topN = 100 // Define the specified number of most frequent flyers
  val frequentFlyers = calculateFrequentFlyers(flightsData, passengersData, topN)

  // Write the result of Q2 SOLUTION
  writeOutput(frequentFlyers, 2, outputPath)

  // Perform Q3 SOLUTION - find the greatest number of countries a passenger has been in without being in the UK
  val withoutCountry = "UK" // Define the country without being in
  val longestRunReport = calculateLongestRunAllPassengers(flightsData, withoutCountry)

  // Write the result of Q3 SOLUTION
  writeOutput(longestRunReport, 3, outputPath)

  // Perform Q4 SOLUTION - find the passengers who have been on more than 3 flights together
  val Q4MinFlights = 3 // Define the minimum number of flights in Q4
  val flightsTogether = calculateFlightsTogether(flightsData, Q4MinFlights)

  // Write the result of Q4 SOLUTION
  writeOutput(flightsTogether, 4, outputPath)

  // Perform Q5 SOLUTION - find the passengers who have been on more than N flights together within the range (from,to)
  val fromDate = "2017-01-01" // Define a specified starting date
  val toDate = "2017-12-31" // Define a specified ending date
  val Q5MinFlights = 3 // Define the minimum number of flights in Q5
  val flightsTogetherInRange = calculateflightsTogetherInRange(flightsData, fromDate, toDate, Q5MinFlights)

  // Write the result of Q5 SOLUTION
  writeOutput(flightsTogetherInRange, 5, outputPath)

  // Terminate Spark session
  spark.stop()
}
