package scala.test

import scala.collection.mutable.LinearSeq
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.Vector
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.HashMap
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.zookeeper.server.SessionTracker.Session
import org.apache.spark.sql.catalyst.expressions.Explode
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ExpExplode {

  case class RawPanda(id: Long, zip: String, pt: String, happy: Boolean, attributes: Array[Double])
  case class Temp(id: Long, zip: String, pt: String, happy: Boolean, attributes: Array[Double])
  case class PandaPlace(name: String, pandas: Array[RawPanda])
  def main(args: Array[String]): Unit = {

    val session = spark.connection.local.SparkContext.sessionSpark()
    val sc = session.sparkContext
    val x = IndexedSeq(1, 2, 3)

    println(x)

    createAndPrintSchema(session)

  }

  def createAndPrintSchema(session: org.apache.spark.sql.SparkSession) = {
    val demo = RawPanda(1, "60080", "giant", true, Array(1.0, 9.0))
    val demo2 = RawPanda(1, "60080", "giant", true, Array(1.0, 2.0))
    val demo3 = RawPanda(3, "60080", "small", true, Array(1.0, 3.0))
    val pandaPlace = PandaPlace("Atlanta", Array(demo, demo2, demo3))
    
   
    
//    val df = session.createDataFrame(Seq(pandaPlace))
//    df.printSchema()
//    //val z = df.select(df("pandas")).dropDuplicates("pt");
//    val x = df.explode(df("pandas")) {
//      case Row(pandas: Seq[Row]) =>
//        pandas.map {
//          case (Row(id: Long, zip: String, pt: String, happy: Boolean, attributes: Seq[Double])) => Temp(id, zip, pt, happy, attributes.toArray)
//        }
//    }
//
//    x.select("name", "id", "zip", "pt", "happy", "attributes").show()
//    val tt = x.rdd
//
//    (x.agg(approx_count_distinct("attributes"))).show()
//
//    //val v = maxPandasSizePerZip(x)
//    x.registerTempTable("pandas")
//    x.write.saveAsTable("pandas_wt")
  }

  def maxPandasSizePerZip(pandas: DataFrame): Unit = {

    pandas.groupBy(pandas("zip"), pandas("pt"))
  }

}