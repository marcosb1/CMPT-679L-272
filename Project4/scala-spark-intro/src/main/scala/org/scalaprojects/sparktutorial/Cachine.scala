import org.apache.spark._
import org.apache.spark.sql.SparkSession

object Caching {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Caching").getOrCreate()
    import spark.implicits._

    val textFile = spark.read.textFile("README.md")
    val linesWithSpark = textFile.filter(line => line.contains("Spark"))

    println(linesWithSpark.cache())
    println(linesWithSpark.count())
    println(linesWithSpark.count())
  }
}
