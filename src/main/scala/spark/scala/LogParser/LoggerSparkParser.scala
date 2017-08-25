package spark.scala.LogParser

import org.apache.spark.rdd._

object LoggerSparkParser {

  def main(args: Array[String]): Unit = {
    //fileReader()
    val ss = fileReaderAsRDD()
  //    ss.foreach { x => println(x) }
    //  println(ss.for)
  }

  def fileReader(): Unit = {
    val session = spark.connection.local.SparkContext.sessionSpark()
    val sc = session.sparkContext
    // Read as dataFrame
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

  def fileReaderAsRDD(): Array[StringBuilder] = {
    val session = spark.connection.local.SparkContext.sessionSpark()
    val sc = session.sparkContext

    // Read as dataFrame
    val inputFilePath = sc.textFile("/Users/gxb8991/Documents/eclipse-workspace/sp-cass/src/main/resources/spark/scala/LogParser/test.txt")

    var outputText = new StringBuilder("")
    
    
//    val tt = inputFilePath.map(x => x.split('|')).map { y =>
//      if ("gourav" == y(0)) {
//        var outputFinalText = new StringBuilder("")
//
//        outputFinalText.append(outputText.slice(0, outputText.length() - 1))
//        outputText.clear()
//        outputText ++= y(1) + ","
//       
//       outputFinalText
//      } else {
//        var pp = new StringBuilder("")
//       
//        outputText ++= y(1) + ","
//        
//        pp.append(outputText)
//        println(pp)
//        null
//      }
//
//    }
        val tt = inputFilePath.map(x => x.split('|')).map { y =>
      if ("gourav" == y(0)) {
        var outputFinalText = new StringBuilder("")

        outputFinalText.append(outputText.slice(0, outputText.length() - 1))
        outputText.clear()
        outputText ++= y(1) + ","
       
       outputFinalText
      } else {
        var pp = new StringBuilder("")
       
        outputText ++= y(1) + ","
        
        pp.append(outputText)
        println(pp)
        null
      }

    }
    .filter { z => z != null }.collect()

      
    tt
  }

}
