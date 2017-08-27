package spark.scala.LogParser

import org.apache.spark.rdd._
import org.apache.spark.sql.Encoders

object ClaimParser {

  def main(args: Array[String]): Unit = {
   // createArr()
    //fileReader()
    val ss = fileReaderAsRDD1()
    //    ss.foreach { x => println(x) }
    //  println(ss.for)
  }

  def fileReaderAsRDD1(): Unit = {
    val session = spark.connection.local.SparkContext.sessionSpark()
    val sc = session.sparkContext

    // Read as dataFrame
    val inputFilePath = sc.textFile("/Users/gxb8991/Documents/eclipse-workspace/sp-cass/src/main/resources/spark/scala/LogParser/claimLog.txt")
    //687687687678 ruleExecuted, claim_id~1,vvv~hgjhg,bbb~1234

    val filterClaimRDD = inputFilePath.filter { x => x.contains(" rule executed,") }

    val claimRaw = filterClaimRDD.map { claimString => claimString.split(",") }
      .map { claimString => claimString.drop(1) }

    val claimValues = claimRaw.map { x =>
      x.map {
        y => y.split("~")

      }.map { z => z(1).concat(",") }

    }
    
   val cc =  claimValues.map { x =>  resursiveConcat(x)}
    
   cc.saveAsTextFile("/Users/gxb8991/Documents/sap1")
   
 //   cc.foreach { x => x.foreach { y => println(y) } }

  
  }

  def resursiveConcat( listOfString : Array[String]): String ={
    var claimText = new StringBuilder("")
    
    for(textValue <- listOfString){
      claimText.append(textValue)
    }
    var claimAsString  = claimText.toString()
    claimAsString.slice(0, claimAsString.length() - 1)  
  }
  
}
