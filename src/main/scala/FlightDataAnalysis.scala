import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window



object FlightDataAnalysis extends App {

  // Spark session
  val spark = SparkSession.builder()
    .appName("Flight Data Analysis")
    .master("local[*]")
    .getOrCreate()

  // Reading the flight data
  val flightData = spark.read
    .option("header", "true") // Assuming the CSV has a header
    .option("inferSchema", "true") // To infer data types
    .csv("src/main/resources/flightData.csv")

  // Reading the passenger data
  val passengersData = spark.read
    .option("header", "true") // Assuming the CSV has a header
    .option("inferSchema", "true") // To infer data types
    .csv("src/main/resources/passengers.csv")
  import spark.implicits._
  // Q1 SOLUTION - total number of flights for each month (in 2017)
  val flightsByMonth = flightData
    .dropDuplicates("Date", "FlightID") // Assumption: a flight appears only once on the same date.
    .withColumn("Month", month(col("date")))
    .groupBy("Month")
    .agg(count("*").as("Number of Flights"))
    .orderBy("Month")

  // Show the results
  flightsByMonth.show()

  // Q2 SOLUTION - the names of the 100 most frequent flyers (Note: change 100 to N)
  val frequentFlyers = flightData
    .groupBy("PassengerID")
    .agg(count("*").as("Number of Flights"))
    .join(passengersData, Seq("PassengerID"))
    .orderBy(desc("Number of Flights"), asc("PassengerID"))
    .select(col("PassengerID").as("Passenger ID"), col("Number of Flights"), col("FirstName").as("First name"), col("LastName").as("Last name"))
    .limit(100)

  // Show the results
  frequentFlyers.show()

  // Q3 SOLUTION - the greatest number of countries a passenger has been in without being in the UK
  // Define a user-defined function to calculate the longest run without being in UK  (Note: change UK to country)
  val longestRunUdf = udf((countrySeq: Seq[String]) => {
    val runs = countrySeq.mkString(",").split(",UK,").map(_.split(",").length)
    if (runs.isEmpty) 0 else runs.max
  })

  // Prepare data and calculate the longest run for each passenger
  val windowSpec = Window.partitionBy("PassengerID").orderBy("Date")
  val countrySeqDf = flightData
    .withColumn("nextFrom", lead("from", 1).over(windowSpec))
    .withColumn("isLastRow", lead("from", 1).over(windowSpec).isNull)
    .withColumn("Countries", when(col("isLastRow") || col("to") =!= col("nextFrom"), array(col("from"), col("to"))).otherwise(array(col("from"))))
    .groupBy("PassengerID")
    .agg(flatten(collect_list("Countries")).alias("CountrySequence"))

//  countrySeqDf.filter(col("PassengerID") === 4827).show(countrySeqDf.count.toInt, false)

  // Calculate the longest run without being in UK for each passenger
  val longestRunDf = countrySeqDf
    .withColumn("Longest Run", longestRunUdf(col("CountrySequence")))

  // Select the final output
  val outputDf = longestRunDf
    .select(col("PassengerID").as("Passenger ID"), col("Longest Run"))
    .orderBy(desc("Longest Run"))

  // Show the results
  outputDf.show()

  // Q4 SOLUTION - the passengers who have been on more than 3 flights together
  // Create the passenger pairs
  val passengerPairs = flightData
    .as("df1")
    .join(flightData.as("df2"), $"df1.FlightID" === $"df2.FlightID" && $"df1.PassengerID" < $"df2.PassengerID")
    .select($"df1.PassengerID".alias("Passenger1ID"), $"df2.PassengerID".alias("Passenger2ID"))

  // Calculate the passengers who have been on more than 3 flights together (Note: Check noisy data - if the same passenger on same flight on same date)
  val flightsTogether = passengerPairs
    .groupBy("Passenger1ID", "Passenger2ID")
    .count()
    .filter($"count" > 3)
    .orderBy($"count".desc, $"Passenger1ID".asc, $"Passenger2ID".asc)
    .withColumnRenamed("count", "Number of flights together")

  // Show the results
  flightsTogether.show()

  // Bonus Question SOLUTION - the passengers who have been on more than N flights together within the range (from,to)
  // Define the date range and minimum number of flights
  val fromDate = "2017-01-01" // Replace with actual date
  val toDate = "2017-12-30"   // Replace with actual date
  val minFlights = 3          // Replace N with the actual number

  val filteredFlightData = flightData
    .withColumn("Date", to_date($"Date", "yyyy-MM-dd"))
    .filter($"Date" >= lit(fromDate) && $"Date" <= lit(toDate))


  val passengerPairs = filteredFlightData
    .as("df1")
    .join(filteredFlightData.as("df2"), $"df1.FlightID" === $"df2.FlightID" && $"df1.PassengerID" < $"df2.PassengerID")
    .select($"df1.PassengerID".alias("Passenger1ID"), $"df2.PassengerID".alias("Passenger2ID"))

  val flightsTogether = passengerPairs
    .groupBy("Passenger1ID", "Passenger2ID")
    .count()
    .filter($"count" > minFlights)
    .withColumnRenamed("count", "Number of flights together")
    .withColumn("From", lit(fromDate))
    .withColumn("To", lit(toDate))
    .orderBy($"Number of flights together".desc)

  flightsTogether.show()
  spark.stop()
}
