import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.util.Try
import java.io.StringReader
import org.apache.commons.csv.{CSVFormat, CSVParser}
import CSVUtil._


object MostCommonArtist {
  def main(args: Array[String]): Unit = {
    val start: Long = System.nanoTime()

    //  Initialize Spark
    val conf = new SparkConf()
      .setAppName("MostCommonArtistFinder")
      .setMaster("local[*]") // Use all CPU cores
    val sc = new SparkContext(conf)

    //  Read CSV file
    val inputFile = "src/main/resources/ds2Scala.csv"
    val linesRDD: RDD[String] = sc.textFile(inputFile)

    //  Merge multi-line records
    val mergedRDD: RDD[String] = linesRDD.coalesce(1).mapPartitions(mergeMultilineCSV)

    //  Function to parse CSV safely
    def parseCSVLine(line: String): Option[String] = {
      Try {
        val reader = new StringReader(line)
        val csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withQuote('"'))
        val row = csvParser.iterator().next()
        reader.close()
        csvParser.close()

        if (row.size() >= 3) {
          val artist = row.get(2).trim
          if (artist.nonEmpty) Some(artist) else None
        } else None
      }.getOrElse(None)
    }

    //  Parse the merged RDD to extract artist names
    val artistRDD: RDD[String] = mergedRDD.flatMap(parseCSVLine)

    //  Count occurrences of each artist
    val artistCountRDD: RDD[(String, Int)] = artistRDD
      .map(artist => (artist, 1))
      .reduceByKey(_ + _) // Sum occurrences

    //  Find the most common artist
    val mostCommonArtist: (String, Int) = artistCountRDD.reduce((a, b) => if (a._2 > b._2) a else b)

    val end: Long = System.nanoTime()
    val duration: Double = (end - start) / 1e9 // Convert to seconds

    //  Print the result
    println("\nMost Common Artist")
    println(s"${mostCommonArtist._1}: ${mostCommonArtist._2} songs")
    println(s"\nTotal Execution Time: $duration seconds")

    //  Stop Spark
    sc.stop()

//    Most Common Artist
//    Genius Romanizations: 16573 songs
//
//      Total Execution Time: 115.807831167 seconds
  }
}
