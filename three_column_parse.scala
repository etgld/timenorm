import org.clulab.timenorm.scfg._, scala.util.Success, scala.util.Failure, scala.util.Try
import java.io._

object ThreeColCSVNorm {

  val parser = TemporalExpressionParser.en



  def process( inDir : String ) = {
    val outDir = "/home/etg/three_column_timenorm_processed/"
    for ( inFile <- getListOfFiles( new File( inDir ), List( "csv" ) ) ){
      val outFileName = outDir + inFile.getName()
      val outFile = new File( outFileName )
      processFile( inFile, outFile )
    }
  }

  def processFile( inFile: File, outFile: File ) = {

    val aprilFools = TimeSpan.of( 2017, 4, 1 )
    val inFileBuffer = io.Source.fromFile( inFile )
    val outFileWriter = new BufferedWriter( new FileWriter( outFile ) )

    val inFileLines = inFileBuffer.getLines.toList
    val header = inFileLines( 0 )
    val inFileContent = inFileLines.drop( 1 ) // skip header

    outFileWriter.write( header + ",timeML\n" )

    for ( line <- inFileContent ){
      val elems = line.split( "," ) //.map( _.trim )
      // println(line)

      val filename = elems( 0 ).trim
      val sentence = elems.drop( 2 ).dropRight( 1 ).mkString( "," ) // don't get filename and DCT or prediction at the end

      val rawDCT = elems( 1 )

      val year = rawDCT.slice( 0, 4 )
      val month = rawDCT.slice( 4, 6 )
      val date = rawDCT.slice( 6, rawDCT.length )

      val documentCreationTime = Try({TimeSpan.of( year.toInt, month.toInt, date.toInt )})
        .recoverWith({case (e: Exception) => println(s"BAD DCT $rawDCT at $filename"); Failure(e);})
        .getOrElse(aprilFools)

      val timex = elems.slice( 2, elems.length ).mkString( "," )
      val timeML = getTimeML( filename, documentCreationTime, timex )

      val outStr = Array( filename.toString , documentCreationTime.timeMLValue, timeML ).mkString( "," )

      outFileWriter.write( outStr + "\n" )
    }

    inFileBuffer.close
    outFileWriter.close()
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
