package scala.test

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.sql.Dataset

object DataBrickExp2 {
  
  val session = spark.connection.local.SparkContext.sessionSpark()
  val sc = session.sparkContext
  val sqlContext = session.sqlContext
  import sqlContext.implicits._

  def main(args: Array[String]): Unit = {

    val x = IndexedSeq(1, 2, 3)

    println(x)

   val eventDS =  createAndPrintSchemaTest()
   

    
  }
 
  case class DeviceData(id: Int, device: String)
  def createAndPrintSchemaTest(): Dataset[DeviceData] = {
   
  Seq((0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
      (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""))
      .toDF("id", "device").as[DeviceData]
    
  }

  def getSchema(): Unit = {
    val schema = new StructType()
      .add("battery_level", LongType)
      .add("c02_level", LongType)
      .add("cca3", StringType)
      .add("cn", StringType)
      .add("device_id", LongType)
      .add("device_type", StringType)
      .add("signal", LongType)
      .add("ip", StringType)
      .add("temp", LongType)
      .add("timestamp", TimestampType)
  }

}