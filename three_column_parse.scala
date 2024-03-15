import org.clulab.timenorm.scfg._, scala.util.Success, scala.util.Failure,
  scala.util.Try
import java.io._

object ThreeColCSVNorm {

  val parser = TemporalExpressionParser.en

  def process(inDir: String) = {
    val outDir = "/home/etg/three_column_timenorm_processed/"
    for (inFile <- getListOfFiles(new File(inDir), List("csv"))) {
      val outFileName = outDir + inFile.getName()
      val outFile = new File(outFileName)
      processFile(inFile, outFile)
    }
  }

  def getDCT(filename: String, consoleWriter: PrintWriter): TimeSpan = {
    val aprilFools = TimeSpan.of(2017, 4, 1)
    filename.split("_").map(_.trim) match {
      case Array(pt, noteType, textDate, index) =>
        val Array(month, day, year) = textDate.split("-").map(_.trim)
        val parsedSpan = TimeSpan.of(year.toInt, month.toInt, day.toInt)
        val tmlVal = parsedSpan.timeMLValue
        // consoleWriter.println(s"Parsed span $tmlVal for node $filename")
        return parsedSpan
      //   case elems =>
      //     val Array( month, day, year ) = elems.slice( 2, 5 )
      //     return TimeSpan.of( year.toInt, month.toInt, day.toInt )
      // }
      case _ =>
        // println(
        //   s"DCT parse error for $filename , using April Fool's 2017"
        // )
        return aprilFools
    }
  }

  def processFile(inFile: File, outFile: File) = {

    val consoleWriter = new PrintWriter(new File("parse.log"))

    val quotes = "\"\'".toSet

    def nonseparatedDCT(rawDCT: String): Boolean = {
      // val dct = rawDCT.filterNot(quotes).trim
      return rawDCT.length == 8 // && rawDCT.forall(Character.isDigit)
    }

    def separatedDCT(rawDCT: String): Boolean = {
      // val dct = rawDCT.filterNot(quotes).trim
      return rawDCT.length == 10 && rawDCT.split("-").length == 3
        // .forall(_.forall(Character.isDigit))
    }
    val aprilFools = TimeSpan.of(2017, 4, 1)
    val inFileBuffer = io.Source.fromFile(inFile)
    val outFileWriter = new BufferedWriter(new FileWriter(outFile))

    val inFileLines = inFileBuffer.getLines.toList
    // val header = inFileLines(0)
    val header = "node_id,DocTime,timeML"
    val inFileContent = inFileLines.drop(1) // skip header

    outFileWriter.write(header + ",timeML\n")

    for (line <- inFileContent) {
      val elems = line.split(",") // .map( _.trim )
      // println(s"$line $elems")
      // consoleWriter.println(line)

      val filename = elems(0).trim
      // val sentence = elems
      //   .drop(2)
      //   .dropRight(1)
      //   .mkString(",") // don't get filename and DCT or prediction at the end

      val rawDCT = elems(1).filterNot(quotes).trim

      val documentCreationTime = rawDCT match {
        case rawDCT: String if nonseparatedDCT(rawDCT) => {
          // if (filename contains "patient114") {
          // println("nonseparated DCT")
          // println(rawDCT)
          // println(filename)
          // }
          val year = rawDCT.slice(0, 4)
          val month = rawDCT.slice(4, 6)
          val date = rawDCT.slice(6, rawDCT.length)
          val result = TimeSpan.of(year.toInt, month.toInt, date.toInt)
          // println(result.timeMLValue)
          result
        }
        case rawDCT: String if separatedDCT(rawDCT) => {
          // if (filename contains "patient114") {
          // println("separated DCT")
          // println(rawDCT)
          // println(filename)
          // }
          val dctElems = rawDCT.split("-")
          val year = dctElems(2)
          val month = dctElems(0)
          val date = dctElems(1)
          val result = TimeSpan.of(year.toInt, month.toInt, date.toInt)
          // println(result.timeMLValue)
          result

        }
        case _ => {
          println(
            s"MALFORMED DCT $rawDCT at $filename, parsing filename for DCT"
          )
          val result = getDCT(filename, consoleWriter)
          println(result.timeMLValue)
          result
        }
      }

      val timex = elems.slice(2, elems.length).mkString(",")
      val timeML =
        getTimeML(filename, documentCreationTime, timex, consoleWriter)

      val outStr =
        Array(filename.toString, documentCreationTime.timeMLValue, timeML)
          .mkString(",")

      outFileWriter.write(outStr + "\n")
    }

    inFileBuffer.close
    outFileWriter.close()
    consoleWriter.close()
  }

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  def getTimeML(
      filename: String,
      DCT: TimeSpan,
      timex: String,
      consoleWriter: PrintWriter
  ): String = {
    val dateElems = timex.split("/")

    if (
      dateElems.length == 3 && dateElems.forall(_.forall(Character.isDigit))
    ) {
      val month = dateElems(0).toInt

      val date = dateElems(1).toInt

      val raw_year = dateElems(2)
      val year = raw_year match {
        // 11 -> 2011
        case raw_year: String if (raw_year.length == 2) =>
          raw_year.toInt + 2000
        case _ =>
          raw_year.toInt
      }

      val parsedDate = TimeSpan.of(year, month, date)
      val finalDate = parsedDate.timeMLValue
      // consoleWriter.println(s"Manually parsed date $finalDate")
      return finalDate
    }

    // cases like 12-Apr
    val dateMonthAbbrevElements = timex.split("-")
    val finalTimex = if (
      dateMonthAbbrevElements.length == 2 && dateMonthAbbrevElements(0).forall(Character.isDigit) && dateMonthAbbrevElements(1).length == 3
    ) {
      dateMonthAbbrevElements(1).toString + "-" + dateMonthAbbrevElements(0).toString // term rewriting ftw
    } else {
      timex
    }
    // to Apr-12 which Timenorm can parse for instead for some reason

    parser.parse(finalTimex, DCT) match {
      case Success(temporal) =>
        val finalValue = temporal.timeMLValue
        // consoleWriter.println(s"Timenorm parsed $finalValue")
        return finalValue
      case Failure(f) =>
        consoleWriter.println(
          s"failed to parse timex: \' $timex ' at node $filename"
        )
        return ""
    }
  }
}
