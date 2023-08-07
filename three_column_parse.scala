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
        consoleWriter.println(
          s"DCT parse error for $filename , using April Fool's 2017"
        )
        return aprilFools
    }
  }

  def processFile(inFile: File, outFile: File) = {

    val consoleWriter = new PrintWriter(new File("parse.log"))

    def nonseparatedDCT(dct: String): Boolean = {
      return dct.length == 8 && dct.forall(Character.isDigit)
    }

    def separatedDCT(dct: String): Boolean = {
      return dct.length == 10 && dct
        .split("-")
        .forall(_.forall(Character.isDigit))
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
      // consoleWriter.println(line)

      val filename = elems(0).trim
      // val sentence = elems
      //   .drop(2)
      //   .dropRight(1)
      //   .mkString(",") // don't get filename and DCT or prediction at the end

      val rawDCT = elems(1)

      val documentCreationTime = rawDCT match {
        case rawDCT: String if nonseparatedDCT(rawDCT) => {
          val year = rawDCT.slice(0, 4)
          val month = rawDCT.slice(4, 6)
          val date = rawDCT.slice(6, rawDCT.length)
          TimeSpan.of(year.toInt, month.toInt, date.toInt)
        }
        case rawDCT: String if separatedDCT(rawDCT) => {
          val dctElems = rawDCT.split("-")
          val year = dctElems(2)
          val month = dctElems(0)
          val date = dctElems(1)

          TimeSpan.of(year.toInt, month.toInt, date.toInt)
        }
        case _ => {
          consoleWriter.println(
            s"MALFORMED DCT $rawDCT at $filename, parsing filename for DCT"
          )
          getDCT(filename, consoleWriter)
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

    parser.parse(timex, DCT) match {
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
