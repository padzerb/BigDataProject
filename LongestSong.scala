import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.util.Try
import java.io.StringReader
import org.apache.commons.csv.{CSVFormat, CSVParser}
import CSVUtil._

object LongestSong {
  def main(args: Array[String]): Unit = {
    val start: Long = System.nanoTime()
    //  Initialize Spark
    val conf = new SparkConf()
      .setAppName("LongestSongFinder")
      .setMaster("local[*]") // Use all CPU cores
    val sc = new SparkContext(conf)

    val inputFile = "src/main/resources/ds2Scala.csv"
    val linesRDD: RDD[String] = sc.textFile(inputFile)

    //  Merge multi-line records
    val mergedRDD: RDD[String] = linesRDD.coalesce(1).mapPartitions(mergeMultilineCSV)

    //  Function to parse CSV lines safely
    def parseCSVLine(line: String): Option[(String, Int)] = {
      Try {
        val reader = new StringReader(line)
        val csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withQuote('"'))
        val row = csvParser.iterator().next()
        reader.close()
        csvParser.close()

        if (row.size() >= 7) {
          val title = row.get(0).trim
          val lyrics = row.get(6).trim
          val numLines = lyrics.split("\\R").length // Handles \n, \r\n, etc.
//          val numLines = lyrics.split(" ").length
          Some(title, numLines)
        } else None
      }.getOrElse(None)
    }

    //  Parse the merged RDD
    val parsedRDD: RDD[(String, Int)] = mergedRDD.flatMap(parseCSVLine)

    //  Find the longest song
    val longestSong: (String, Int) = parsedRDD.reduce((a, b) => if (a._2 > b._2) a else b)

    val end: Long = System.nanoTime()
    val duration: Double = (end - start) / 1e9 // Convert to seconds

    //  Print the result
    println("\nLongest Song in Dataset ")
    println(s"${longestSong._1}: ${longestSong._2} lines")
    println(s"\nTotal Execution Time: $duration seconds")

    //  Stop Spark
    sc.stop()

//    Longest Song in Dataset by line breaks \\R
//      Patient Protection and Affordable Care Act ObamaCare: 29251 lines
//      Total Execution Time: 142.511299042 seconds
//
//    Longest Song in Dataset by spaces
//      Patient Protection and Affordable Care Act ObamaCare: 365904 spaces
//      Total Execution Time: 141.570158917 seconds

  }
}
