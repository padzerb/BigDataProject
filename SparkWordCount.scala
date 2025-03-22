import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.util.Try
import java.io.StringReader
import org.apache.commons.csv.{CSVFormat, CSVParser}
import scala.util.matching.Regex
import scala.collection.mutable
import CSVUtil._

object GenreWordCount {
  def main(args: Array[String]): Unit = {
    val start: Long = System.nanoTime()

    //  Initialize Spark
    val conf = new SparkConf()
      .setAppName("GenreWordCount")
      .setMaster("local[*]") // Use all CPU cores
    val sc = new SparkContext(conf)

    //  Read CSV file
    val inputFile = "src/main/resources/ds2Scala.csv"
    val linesRDD: RDD[String] = sc.textFile(inputFile)

    //  Define genre dictionary
    val genreDictionary: Set[String] = Set("rock", "pop", "country", "misc")

    //  Function to merge multi-line CSV records

    //  Merge multi-line records
    val mergedRDD: RDD[String] = linesRDD.mapPartitions(mergeMultilineCSV)

    //  Function to parse CSV safely
    def parseCSVLine(line: String): Option[(String, String)] = {
      Try {
        val reader = new StringReader(line)
        val csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withQuote('"'))
        val row = csvParser.iterator().next()
        reader.close()
        csvParser.close()

        if (row.size() >= 7) {
          val genre = row.get(1).trim.toLowerCase
          val lyrics = row.get(6).trim.toLowerCase

          //  Validate genre using the dictionary
          if (genreDictionary.contains(genre)) Some(genre, lyrics) else None
        } else None
      }.getOrElse(None)
    }

    //  Parse the merged RDD
    val genreLyricsRDD: RDD[(String, String)] = mergedRDD.flatMap(parseCSVLine)

    //  Regex pattern for 5-letter words
    val fiveLetterWordPattern: Regex = "\\b[a-zA-Z]{5}\\b".r

    //  Function to find the most common 5-letter word in a song
    def mostCommonWord(lyrics: String): Option[String] = {
      val wordCounts = mutable.Map[String, Int]()

      for (word <- fiveLetterWordPattern.findAllIn(lyrics)) {
        wordCounts(word) = wordCounts.getOrElse(word, 0) + 1
      }

      //  Remove "verse" from the word list before choosing the most common word
      val filteredWords = wordCounts.filterNot { case (word, _) => word == "verse" }

      if (filteredWords.nonEmpty) Some(filteredWords.maxBy(_._2)._1) else None
    }

    //  Extract most common 5-letter word per song & group by genre
    val genreWordPairsRDD: RDD[(String, String)] = genreLyricsRDD
      .flatMap { case (genre, lyrics) => mostCommonWord(lyrics).map(word => (genre, word)) }

    //  Count occurrences of each word per genre
    val genreWordCountRDD: RDD[(String, (String, Int))] = genreWordPairsRDD
      .map { case (genre, word) => ((genre, word), 1) }
      .reduceByKey(_ + _) // Count occurrences
      .map { case ((genre, word), count) => (genre, (word, count)) }

    //  Find the most common 5-letter word in each genre
    val mostCommonWordByGenreRDD: RDD[(String, String)] = genreWordCountRDD
      .reduceByKey((a, b) => if (a._2 > b._2) a else b) // Keep highest count
      .mapValues(_._1) // Extract only the word

    //  Collect & print results
    val results = mostCommonWordByGenreRDD.collect()
    println("\nMost Common 5-Letter Word in Each Genre (Excluding 'verse')")
    results.foreach { case (genre, word) =>
      println(s"$genre: $word")
    }

    val end: Long = System.nanoTime()
    val duration: Double = (end - start) / 1e9 // Convert to seconds

    println(s"\nTotal Execution Time: $duration seconds")

    //  Stop Spark
    sc.stop()

//    Most Common 5-Letter Word in Each Genre (Excluding 'verse')
//    country: there
//    rock: never
//    pop: never
//    misc: their
//
//    Total Execution Time: 139.168461167 seconds
  }
}
