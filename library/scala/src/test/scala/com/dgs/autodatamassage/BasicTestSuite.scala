package com.dgs.autodatamassage

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.{FeatureSpec, Matchers}

import scala.collection.JavaConverters._

class BasicTestSuite extends FeatureSpec with Matchers{
  val spark = SparkSession.builder.master("local[*]").appName("BasicTestSuite").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  feature("Basic Test suite") {
    scenario("Basic spark test") {

      val schema = new StructType(Array(
        StructField("strField1", StringType),
        StructField("idFiel", ByteType, true),
        StructField("floatField", FloatType),
        StructField("vectorField", VectorType),
        StructField("doubleField", DoubleType),
        StructField("booleanField", BooleanType),

      ))

      val row0 = Row("A", 0.toByte, 1.0f, Vectors.dense(0.0,1.1,0.2,0.4), 3.4, false)
      val row1 = Row("B", 0.toByte, 2.0f, Vectors.dense(1.0,1.1,0.2,0.4), 2.4, true)
      val row2 = Row("C", 0.toByte, 3.0f, Vectors.dense(2.0,1.1,0.2,0.4), 1.4, false)

      val dataset = spark.sqlContext.createDataFrame(List[Row](row0,row1,row2 ).asJava, schema)


      val si = new StringIndexer() .setInputCol("strField1")
        .setOutputCol("categoryIndex")

      val indexed = si.fit(dataset).transform(dataset)
      indexed.show()

    }
  }


}
