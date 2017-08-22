package spark.scala.LogParser

object LoggerParserAsRDD {
  
  def main(args: Array[String]): Unit = {
    fileReader()
  }

  def fileReader(): Unit = {
    val session = spark.connection.local.SparkContext.sessionSpark()
    val sc = session.sparkContext
    val inputFilePath = session.read.text("/Users/gxb8991/Documents/eclipse-workspace/sp-cass/src/main/resources/spark/scala/LogParser/test.txt")

    var outputFinalText = new StringBuilder
    var outputText = new StringBuilder
    val count = 0
    for (line <- inputFilePath) {

      val lineArray = line.getString(0).split('|')

      if ("gourav" == lineArray(0)) {
        // TODO : Leaves one empty line in the beginning of the file or can be dropped while inserting into hive
        outputFinalText ++= outputText.slice(0, outputText.length() - 1) + "\n"
        println(outputFinalText)
        outputText.clear()
        outputText ++= lineArray(1) + ","
      } else {
        outputText ++= lineArray(1) + ","
      }

    }
    /* Read the last record of the file*/
    outputFinalText ++= outputText.slice(0, outputText.length() - 1) + "\n"
    println(outputFinalText)
       // println("final output is -->" + outputFinalText.show())
  }
}