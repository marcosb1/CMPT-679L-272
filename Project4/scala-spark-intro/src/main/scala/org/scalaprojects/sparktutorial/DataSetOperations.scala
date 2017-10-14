import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.lang.Math
import java.util.Map

object DataSetOperations {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("DataSetOperations").getOrCreate()
    import spark.implicits._
    val textFile = spark.read.textFile("README.md")

    textFile.map(line => line.split(" ").size).reduce((a,b) => if (a > b) a else b)
    textFile.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))

    val wordCounts = textFile.flatMap(line=> line.split(" ")).groupByKey(identity).count()

    println("Word Counts Collection...")
    println(wordCounts.collect())
  }
}
