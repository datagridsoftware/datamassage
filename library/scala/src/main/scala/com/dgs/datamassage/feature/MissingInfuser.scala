package com.dgs.datamassage.feature

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.json4s.{DefaultFormats, JNothing, JNull}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import java.sql.Timestamp


/**
 * A feature transformer that fills null / missing values with specified default values.
 *
 * This requires one pass over the entire dataset.
 * Note: The output DF of this transformer is replacing missing values for input columns and does not generate new fields for output
 */
class MissingInfuser(override val uid: String)
  extends Transformer with HasInputCols with NullStringParam with NullBooleanParam with NullNumericParam with NullTimestampParam
    with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("missingInfuser"))

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  private def redefinedRdd(df: DataFrame, schema: StructType, defaultNullVals: Array[(Boolean, Int, Any)]): RDD[Row] = {
    df.rdd.map { row =>
      val elems = defaultNullVals.map { case (isValidFld, idx, value) =>
        if (isValidFld)
          if (!row.isNullAt(idx))
            row.get(idx)
          else
            value
        else
          row.get(idx)
      }
      Row(elems: _*)
    }
  }


  override def transform(dataset: Dataset[_]): DataFrame = {
    val data = dataset.toDF()
    val schema = dataset.schema

    if (!isDefined(inputCols)) {
      setInputCols(schema.fieldNames)
    }

    val inputColdWithNullVals = schema.fields.zipWithIndex.map {
      case (fld, idx) => {
        if ($(inputCols).contains(fld.name))
          fld.dataType match {
            case StringType => (true, idx, $(nullStringParam))
            case BooleanType => (true, idx, $(nullBooleanParam))
            case ByteType => (true, idx, $(nullNumericParam).toByte)
            case ShortType => (true, idx, $(nullNumericParam).toShort)
            case IntegerType => (true, idx, $(nullNumericParam).toInt)
            case LongType => (true, idx, $(nullNumericParam).toLong)
            case FloatType => (true, idx, $(nullNumericParam).toFloat)
            case DoubleType => (true, idx, $(nullNumericParam))
            case f if (f.isInstanceOf[DecimalType]) => (true, idx, BigDecimal($(nullNumericParam)))
            case _ => (false, idx, 0)
          }
        else
          (false, idx, 0)
      }
    }
    data.sqlContext.createDataFrame(redefinedRdd(data, schema, inputColdWithNullVals), schema)
  }

  override def copy(extra: ParamMap): MissingInfuser = {
    val model = new MissingInfuser()
    copyValues(model, extra)
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def write: MLWriter = new MLWriter() {
    protected def saveImpl(path: String) {
      import org.json4s.JsonDSL._
      MissingInfuser.super.write.save(path)
      val customMetadataPath = new Path(path, "custom").toString
      val meta =
        if (!isDefined(inputCols))
          ("uid" -> uid) ~
            ("nullStringParam" -> $(nullStringParam)) ~
            ("nullBooleanParam" -> $(nullBooleanParam)) ~
            ("nullNumericParam" -> $(nullNumericParam)) ~
            ("nullTimestampParam" -> $(nullTimestampParam).getTime)
        else
          ("uid" -> uid) ~
            ("inputCols" -> $(inputCols).toSeq) ~
            ("nullStringParam" -> $(nullStringParam)) ~
            ("nullBooleanParam" -> $(nullBooleanParam)) ~
            ("nullNumericParam" -> $(nullNumericParam)) ~
            ("nullTimestampParam" -> $(nullTimestampParam).getTime)
      val json = compact(render(meta))
      sc.parallelize(Seq(json), 1).saveAsTextFile(customMetadataPath)
    }
  }

  /**
   * Check if all input parameters are correctly set :
   * All input fields must exist in input data frame schema
   * All input fields must have numeric, boolean, string or timestamp type
   *
   * @param schema
   */

  protected def validateAndTransformSchema(
                                            schema: StructType,
                                            skipNonExistsCol: Boolean = false): StructType = {

    if (!this.isDefined(inputCols)) {
      setInputCols(schema.fieldNames)
    }
    val inputColNames = $(inputCols)

    require(inputColNames.distinct.length == inputColNames.length,
      s"Input columns should not be duplicate.")

    $(inputCols).map {
      col => {
        require(schema.fieldNames.contains(col), s"Input DF must contain column $col")
        val dt = schema(col).dataType
        require(dt.isInstanceOf[NumericType] ||
          dt == DataTypes.BooleanType ||
          dt == DataTypes.StringType ||
          dt == DataTypes.DateType ||
          dt == DataTypes.TimestampType,
          s"input field $col has an unsupported type ${schema(col).dataType}. Accepted types are numeric, boolean, string and date / timestamp types")
      }
    }
    schema
  }
}


object MissingInfuser extends DefaultParamsReadable[MissingInfuser] {
  override def read: MLReader[MissingInfuser] = new EmptyValuesInfuserReader

  private[MissingInfuser] class EmptyValuesInfuserReader extends MLReader[MissingInfuser] {
    implicit val format = DefaultFormats

    override def load(path: String): MissingInfuser = {
      val metadataPath = new Path(path, "custom").toString
      val metadata = parse(sc.textFile(metadataPath, 1).first())
      val uid = (metadata \ "uid").extract[String]
      val rawInputCol = (metadata \ "inputCols")

      val nullStringParam = (metadata \ "nullStringParam").extract[String]
      val nullBooleanParam = (metadata \ "nullBooleanParam").extract[Boolean]
      val nullNumericParam = (metadata \ "nullNumericParam").extract[Double]
      val nullTimestampLongParam = (metadata \ "nullTimestampParam").extract[Long]
      if (rawInputCol == JNothing || rawInputCol ==JNull)
        new MissingInfuser(uid)
          .setNullStringParam(nullStringParam)
          .setNullBooleanParam(nullBooleanParam)
          .setNullNumericParam(nullNumericParam)
          .setNullTimestampParam(new Timestamp(nullTimestampLongParam))
      else {
        val inputCols = rawInputCol.extract[List[String]]
        new MissingInfuser(uid)
          .setInputCols(inputCols.toArray)
          .setNullStringParam(nullStringParam)
          .setNullBooleanParam(nullBooleanParam)
          .setNullNumericParam(nullNumericParam)
          .setNullTimestampParam(new Timestamp(nullTimestampLongParam))
      }
    }
  }
}

