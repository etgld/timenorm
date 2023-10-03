import org.clulab.timenorm.scfg._, scala.util.Success, scala.util.Failure
import java.io._

object parseAganst {
  val parser = TemporalExpressionParser.en
  def parseAgainst(
      timex: String,
      year: Int,
      month: Int,
      date: Int
  ): String =
    parser.parse(timex, TimeSpan.of(year, month, date)) match {

      case Success(temporal) =>
        val finalValue = temporal.timeMLValue
        // consoleWriter.println(s"Timenorm parsed $finalValue")
        return finalValue
      case Failure(f) =>
        return s"failed to parse  $timex"

    }

  // def main(args: Array[String]): Unit =
  //   for (arg <- args) {
  //     println(parseAgainst(arg))
  //   }
}
