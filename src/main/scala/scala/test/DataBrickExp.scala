package scala.test

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._


object DataBrickExp {
  def main(args: Array[String]): Unit = {

    val session = spark.connection.local.SparkContext.sessionSpark()
    val sc = session.sparkContext
    val x = IndexedSeq(1, 2, 3)

    println(x)

    createAndPrintSchemaTest(session)

  }

  def createAndPrintSchemaTest(session: org.apache.spark.sql.SparkSession): Unit = {
    
    val schemeT = new StructType()
                      .add("name", StringType)
                      .add("place", StringType)
                      .add("zip", IntegerType)
                      .add("Records", ArrayType( new StructType().add("col1", StringType)
                                                                 .add("col2", StringType)
                                                                  .add("col2", StringType))
                          )
                      .add("Attribute", ArrayType(IntegerType))
                      .add("Address", ArrayType(new StructType().add("add1", StringType)
                                                                .add("add2", StringType))
                          )
                              
  }

}