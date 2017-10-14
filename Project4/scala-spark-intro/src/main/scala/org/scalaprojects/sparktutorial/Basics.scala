import org.apache.spark._
import org.apache.spark.sql.SparkSession

object Basics {

    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("Basics").getOrCreate()

	val textFile = spark.read.textFile("README.md")

	println("Text File Count...")
        println(textFile.count())
        println("Text File First...")
        println(textFile.first())

        val linesWithSpark = textFile.filter(line => line.contains("Spark"))
        
	println("Lines with Spark...")
        println(linesWithSpark)
        println(linesWithSpark.count())
    }
}
