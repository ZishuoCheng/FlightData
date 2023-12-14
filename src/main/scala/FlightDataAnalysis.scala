import org.apache.spark.sql.SparkSession

// Main object FlightDataAnalysis: Entry point of the Flight Data Analysis application
object FlightDataAnalysis extends App {

  // Spark session
  val spark = SparkSession.builder()
    .appName("Flight Data Analysis")
    .master("local[*]")
    .getOrCreate()

  // Initialize AppConfig with default values
  val config = AppConfig.default

  // Call the processData function with selected questions
  FlightDataUtils.processData(spark, config.inputPath, config.outputPath, config.tasks)

  // Terminate Spark session
  spark.stop()
}

