package spark.scala.LogParser

object LoggerParser {

  def main(args: Array[String]): Unit = {
    fileReader()
  }

  def fileReader(): Unit = {

    val inputFilePath = scala.io.Source.fromFile("/Users/gxb8991/Documents/eclipse-workspace/sp-cass/src/main/resources/spark/scala/LogParser/test.txt").getLines()
    var outputFinalText = new StringBuilder
    var outputText = new StringBuilder
    val count = 0
    for (line <- inputFilePath) {

      val lineArray = line.split('|')

      if ("gourav" == lineArray(0)) {
        // TODO : Leaves one empty line in the beginning of the file or can be dropped while inserting into hive
        outputFinalText ++= outputText.slice(0, outputText.length() - 1) + "\n"
        outputText.clear()
        outputText ++= lineArray(1) + ","
      } else {
        outputText ++= lineArray(1) + ","
      }

    }
    /* Read the last record of the file*/
    outputFinalText ++= outputText.slice(0, outputText.length() - 1) + "\n"
    println("final output is -->" + outputFinalText.toString())
  }

}