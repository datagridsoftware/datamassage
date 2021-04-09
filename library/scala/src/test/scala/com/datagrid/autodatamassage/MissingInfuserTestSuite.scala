package com.datagrid.autodatamassage

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FeatureSpec, Matchers}
import com.datagrid.datamassage.feature._
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{BooleanType, ByteType, DoubleType, FloatType, StringType, StructField, StructType}

import scala.collection.JavaConverters._
import java.sql.Timestamp
import scala.reflect.io.File

class MissingInfuserTestSuite extends FeatureSpec with Matchers with BeforeAndAfterAll with StrictLogging {
  val spark = SparkSession.builder.master("local[*]").appName("BasicTestSuite").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  override def afterAll() = {
    File("./save").deleteRecursively()
  }

  feature("MissingInfuser Test suite") {
    val schema = new StructType(Array(
      StructField("strField1", StringType),
      StructField("idFiel", ByteType, true),
      StructField("floatField", FloatType),
      StructField("vectorField", VectorType),
      StructField("doubleField", DoubleType),
      StructField("booleanField", BooleanType),

    ))

    scenario("MissingInfuser spark test basic") {

      val row0 = Row(null, 0.toByte, 1.0f, Vectors.dense(0.0, 1.1, 0.2, 0.4), 3.4, false)
      val row1 = Row("B", null, 2.0f, Vectors.dense(1.0, 1.1, 0.2, 0.4), 2.4, true)
      val row2 = Row("C", 0.toByte, null, Vectors.dense(2.0, 1.1, 0.2, 0.4), null, false)

      val dataset = spark.sqlContext.createDataFrame(List[Row](row0, row1, row2).asJava, schema)


      val ni = new MissingInfuser().setInputCols(Array("strField1", "idFiel", "floatField")).setNullNumericParam(5.5).setNullBooleanParam(true).setNullStringParam("_SOME_VALUE").setNullTimestampParam(new Timestamp(0l))
      val output = ni.transform(dataset)
      output.show()
      val elems = output.head(3)
      assert(elems.length == 3)
      assert(elems(0).getString(0) == "_SOME_VALUE")
      assert(elems(1).getByte(1) == 5)
      assert(elems(2).getFloat(2) == 5.5f)

    }


    scenario("MissingInfuser save/load  test") {
      val row0 = Row(null, 0.toByte, 1.0f, Vectors.dense(0.0, 1.1, 0.2, 0.4), 3.4, false)
      val row1 = Row("B", null, 2.0f, Vectors.dense(1.0, 1.1, 0.2, 0.4), 2.4, true)
      val row2 = Row("C", 0.toByte, null, Vectors.dense(2.0, 1.1, 0.2, 0.4), null, false)

      val dataset = spark.sqlContext.createDataFrame(List[Row](row0, row1, row2).asJava, schema)


      val ni = new MissingInfuser().setInputCols(Array("strField1", "idFiel", "floatField")).setNullNumericParam(5.5).setNullBooleanParam(true).setNullStringParam("_SOME_VALUE").setNullTimestampParam(new Timestamp(0l))
      ni.write.overwrite().save("./save/missinginfuser_test")
      val mi2 = MissingInfuser.load("./save/missinginfuser_test")
      val output2 = mi2.transform(dataset)
      output2.show()
      val elems = output2.head(3)
      assert(elems.length == 3)
      assert(elems(0).getString(0) == "_SOME_VALUE")
      assert(elems(1).getByte(1) == 5)
      assert(elems(2).getFloat(2) == 5.5f)

    }

    scenario("MissingInfuser no inputFields save/load test") {
      val row0 = Row(null, 0.toByte, 1.0f, Vectors.dense(0.0, 1.1, 0.2, 0.4), 3.4, false)
      val row1 = Row("B", null, 2.0f, Vectors.dense(1.0, 1.1, 0.2, 0.4), 2.4, true)
      val row2 = Row("C", 0.toByte, null, Vectors.dense(2.0, 1.1, 0.2, 0.4), null, false)

      val dataset = spark.sqlContext.createDataFrame(List[Row](row0, row1, row2).asJava, schema)
      val ni = new MissingInfuser().setNullNumericParam(5.5).setNullBooleanParam(true).setNullStringParam("_SOME_VALUE").setNullTimestampParam(new Timestamp(0l))
      ni.write.overwrite().save("./save/missinginfuser_no_inputs_test")
      val mi2 = MissingInfuser.load("./save/missinginfuser_no_inputs_test")
      val output2 = mi2.transform(dataset)
      output2.show()
      val elems = output2.head(3)
      assert(elems.length == 3)
      assert(elems(0).getString(0) == "_SOME_VALUE")
      assert(elems(1).getByte(1) == 5)
      assert(elems(2).getFloat(2) == 5.5f)
      assert(elems(2).getDouble(4) == 5.5d)
    }
  }

}
