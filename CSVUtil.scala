object CSVUtil {
  def mergeMultilineCSV(partition: Iterator[String]): Iterator[String] = {
    val buffer = new StringBuilder
    var insideQuotes = false

    partition.flatMap { line =>
      val quoteCount = line.count(_ == '"')
      if (quoteCount % 2 == 1) insideQuotes = !insideQuotes
      buffer.append(line).append("\n")
      if (!insideQuotes) {
        val record = buffer.toString().trim
        buffer.clear()
        Some(record)
      } else None
    }
  }
}
