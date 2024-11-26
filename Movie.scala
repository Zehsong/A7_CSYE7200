import org.apache.spark.sql.{DataFrame, SparkSession}

object Movie extends App {
  val spark = SparkSession
    .builder
    .appName("MovieRating")
    .master("local[*]")
    .getOrCreate()

  val filePath = "C:/Users/momog/Downloads/movie_metadata.csv"
  val df = readCSV(spark, filePath)

  val processedDF = processDataFrame(df)
  processedDF.show()

  def readCSV(spark: SparkSession, filePath: String): DataFrame = {
    spark.read.option("header", "true").csv(filePath)
  }

  def processDataFrame(df: DataFrame): DataFrame = {
    df.describe("imdb_score")
  }
}
