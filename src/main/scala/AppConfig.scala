// Define a case class to hold application configuration
case class AppConfig(
                      inputPath: String,
                      outputPath: String,
                      tasks: Set[Int],
                      topN: Int,
                      withoutCountry: String,
                      Q4MinFlights: Int,
                      fromDate: String,
                      toDate: String,
                      Q5MinFlights: Int
                    )

// Object AppConfig: Contains the companion object for AppConfig with a method to create a default configuration
object AppConfig {
  // Provide a method to create an instance of AppConfig with default values
  def default: AppConfig = AppConfig(
    // Specify the path of inputs
    inputPath = "src/main/resources/inputs/",

    // Specify the path of outputs
    outputPath = "src/main/resources/outputs/",

    // Specify the task number to implement
    tasks = Set(1, 2, 3, 4, 5),

    // Define the specified number of most frequent flyers in Q2
    topN = 100,

    // Define the country (in lower case) without being in in Q3
    withoutCountry = "uk",

    // Define the minimum number of flights in Q4
    Q4MinFlights = 3,

    // Define a specified starting date, ending date, the minimum number of flights in Q5
    fromDate = "2017-01-01",
    toDate = "2017-12-31",
    Q5MinFlights = 3
  )
}
