import org.clulab.timenorm.scfg._, scala.util.Success, scala.util.Failure
import java.io._

object CSVNorm {
  var patientFourDCTs = scala.collection.mutable.Map[ String, TimeSpan ]()

  val parser = TemporalExpressionParser.en

  def populateMap() = {
    patientFourDCTs += (
      ("patient04_report034_RAD"  -> TimeSpan.of( 2012, 4,  2  )),
      ("patient04_report035_NOTE" -> TimeSpan.of( 2012, 4,  23 )),
      ("patient04_report036_NOTE" -> TimeSpan.of( 2012, 5,  14 )),
      ("patient04_report039_NOTE" -> TimeSpan.of( 2012, 6,  25 )),
      ("patient04_report040_NOTE" -> TimeSpan.of( 2012, 7,  16 )),
      ("patient04_report041_NOTE" -> TimeSpan.of( 2012, 8,  9  )),
      ("patient04_report046_NOTE" -> TimeSpan.of( 2012, 10, 15 )),
      ("patient04_report051_SP"   -> TimeSpan.of( 2012, 12, 17 )),
      ("patient04_report052_NOTE" -> TimeSpan.of( 2012, 12, 17 )),
      ("patient04_report053_NOTE" -> TimeSpan.of( 2013, 2,  11 )),
      ("patient04_report055_NOTE" -> TimeSpan.of( 2013, 3,  20 ))
    )
  }


  def process( inDir : String ) = {
    populateMap()
    val outDir = "/home/etg/CSV_timenorm_processed/"
    for ( inFile <- getListOfFiles( new File( inDir ), List( "csv" ) ) ){
      val outFileName = outDir + inFile.getName()
      val outFile = new File( outFileName )
      processFile( inFile, outFile )
    }
  }

  def getTextDCT( rawDCT: String ): TimeSpan = {
    rawDCT.split( "-" ).map( _.trim ) match {
      case Array( month, date, year ) =>
        return TimeSpan.of( year.toInt, month.toInt, date.toInt )
      case other =>
        val year = rawDCT.slice( 0, 4 )
        val month = rawDCT.slice( 4, 6 )
        val date = rawDCT.slice( 6, rawDCT.length )
        return TimeSpan.of( year.toInt, month.toInt, date.toInt )
    }
  }

  def getDCT( filename: String ): TimeSpan = {
    val aprilFools = TimeSpan.of( 2017, 4, 1 )
    if ( filename.startsWith( "patient04" ) ){
      patientFourDCTs.get( filename ) match {
        case Some( timeSpan ) => return timeSpan
        case other =>
          println(s"DCT query error for $filename")
          return aprilFools
      }
    } else if ( filename.startsWith( "ID107" ) ){
      filename.split( "_" ).map( _.trim ) match {
        case Array( pt, noteType, textDate, index ) =>
          val Array( month, day, year ) = textDate.split( "-" ).map( _.trim )
          return TimeSpan.of( year.toInt, month.toInt, day.toInt )
        case elems =>
          val Array( month, day, year ) = elems.slice( 2, 5 )
          return TimeSpan.of( year.toInt, month.toInt, day.toInt )
      }

    } else {
      println(s"DCT query error for $filename")
      return aprilFools
    }
  }

  def processFile( inFile: File, outFile: File ) = {
    val inFileBuffer = io.Source.fromFile( inFile )
    val outFileWriter = new BufferedWriter( new FileWriter( outFile ) )

    val inFileLines = inFileBuffer.getLines.toList
    val header = inFileLines( 0 )
    val inFileContent = inFileLines.drop( 1 ) // skip header

    outFileWriter.write( header + ",timeML\n" )

    for ( line <- inFileContent ){
      val elems = line.split( "," ) //.map( _.trim )

      val filename = elems( 0 ).trim
      val sentence = elems.drop( 2 ).dropRight( 1 ).mkString( "," ) // don't get filename and DCT or prediction at the end

      val _rawDCT = elems( 1 )
      val documentCreationTime = getTextDCT( _rawDCT )
      val timex = getTimex( sentence )
      val timeML = getTimeML( filename, documentCreationTime, timex )

      val outStr = Array( filename.toString , documentCreationTime.timeMLValue, sentence.toString,  timeML.toString ).mkString( "," )

      outFileWriter.write( outStr + "\n" )
    }

    inFileBuffer.close
    outFileWriter.close()
  }

  def getTimex( sentence: String ): String = {
    return sentence
      .split( "\\s" )
      .map( _.trim() )
      .dropWhile( _ != "<t>" )
      .drop( 1 ) // to avoid grabbing the '<t>'
      .takeWhile( _ != "</t>" )
      .mkString( " " )
  }

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter( _.isFile ).toList.filter { file =>
        extensions.exists( file.getName.endsWith( _ ) )
    }
  }

  def getTimeML( filename: String, DCT: TimeSpan, timex: String ): String = {
    parser.parse( timex, DCT ) match {
      case Success( temporal ) =>
        return temporal.timeMLValue
      case Failure( f ) =>
        println(s"failed to parse timex: <t> $timex </t>   at filename:   $filename")
        return ""
    }
  }
}
