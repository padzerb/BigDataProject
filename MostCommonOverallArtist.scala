import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.util.Try
import java.io.StringReader
import org.apache.commons.csv.{CSVFormat, CSVParser}
import CSVUtil._

case class ArtistData(main: Option[String], features: Seq[String])//to avoid Task not serializable error

object MostCommonOverallArtist {

  def parseCSVLine(line: String): Option[ArtistData] = {
    Try {
      val reader = new StringReader(line)
      val parser = new CSVParser(reader, CSVFormat.DEFAULT.withQuote('"'))
      val row = parser.iterator().next()
      reader.close()
      parser.close()

      if (row.size() >= 6) {
        val mainArtist = Option(row.get(2)).map(_.trim).filter(_.nonEmpty)
        val featuresRaw = Option(row.get(5)).map(_.trim)

        val features = featuresRaw
          .filter(f => f.startsWith("{") && f.endsWith("}"))
          .map(f => f.substring(1, f.length - 1)) // Remove { and }
          .getOrElse("")
          .split(",")
          .map(_.trim.stripPrefix("\"").stripSuffix("\""))
          .filter(_.nonEmpty)
          .toSeq

        Some(ArtistData(mainArtist, features))
      } else None
    }.getOrElse(None)
  }

  def main(args: Array[String]): Unit = {
    val start: Long = System.nanoTime()

    val conf = new SparkConf()
      .setAppName("MostCommonOverallArtist")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputFile = "src/main/resources/ds2Scala.csv"
    val linesRDD: RDD[String] = sc.textFile(inputFile)
    val mergedRDD: RDD[String] = linesRDD.coalesce(1).mapPartitions(mergeMultilineCSV)

    val parsedRDD: RDD[ArtistData] = mergedRDD.flatMap(parseCSVLine)

    val artistTypeCounts: RDD[((String, String), Int)] = parsedRDD.flatMap { data =>
      val main = data.main.map(artist => ((artist, "main"), 1))
      val features = data.features.map(artist => ((artist, "feature"), 1))
      val overall = (data.main.toSeq ++ data.features).map(artist => ((artist, "overall"), 1))
      main.toSeq ++ features ++ overall
    }.reduceByKey(_ + _)


    val topMain = artistTypeCounts
      .filter { case ((_, artistType), _) => artistType == "main" }
      .max()(Ordering.by(_._2))

    val topFeature = artistTypeCounts
      .filter { case ((_, artistType), _) => artistType == "feature" }
      .max()(Ordering.by(_._2))

    val topOverall = artistTypeCounts
      .filter { case ((_, artistType), _) => artistType == "overall" }
      .max()(Ordering.by(_._2))


    val end = System.nanoTime()
    val duration = (end - start) / 1e9

    println(f"\nTotal Execution Time: $duration%.2f seconds")
    println(s"Most Common Main Artist: ${topMain._1} (${topMain._2} songs)")
    println(s"Most Common Featured Artist: ${topFeature._1} (${topFeature._2} features)")
    println(s"Most Common Artist Overall: ${topOverall._1} (${topOverall._2} appearances)")

    sc.stop()
//    Total Execution Time: 134.12 seconds
//      Most Common Main Artist: (Genius Romanizations,main) (16573 songs)
//    Most Common Featured Artist: (Genius Brasil Traduções,feature) (8844 features)
//    Most Common Artist Overall: (Genius Romanizations,overall) (16573 appearances)
  }
}
