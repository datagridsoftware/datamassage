package com.datagrid.datamassage.feature

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.param.{IntParam, Param, ParamValidators, Params, StringArrayParam}
import org.json4s.{DefaultFormats, JDouble, JLong}
import org.json4s.JsonAST.{JBool, JObject}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import java.sql.Timestamp

trait HasCategoricalNumericalInputCols extends Params {

  /**
   * Param for categorical numeric input column names.
   *
   * @group param
   */
  val catNumericalInputCols: StringArrayParam = new StringArrayParam(this, "catNumericalInputCols", "categorical numeric input column names")

  /** @group getParam */
  def getCatNumericInputCols: Array[String] = $(catNumericalInputCols)
}

trait HasMaxCategories extends Params{
  /**
   * Threshold for the number of values a categorical feature can take.
   * If a feature is found to have {@literal >} maxCategories values, then it is declared
   * continuous. Must be greater than or equal to 2.
   *
   * (default = 20)
   * @group param
   */
  val maxCategories = new IntParam(this, "maxCategories",
    "Threshold for the number of values a categorical feature can take (>= 2)." +
      " If a feature is found to have > maxCategories values, then it is declared continuous.",
    ParamValidators.gtEq(2))

  def getMaxCategories: Int = $(maxCategories)

  setDefault(maxCategories -> 20)
}

trait NullStringParam extends Params {

  /**
   * Param for default value used in case of null value encounter on string fields.
   *
   * @group param
   */
  val nullStringParam: Param[String] = new Param[String](this, "nullStringParam", "default value for null value encounter on string field")


  /**
   *
   * @param value String
   * @return this.type
   */
  def setNullStringParam(value: String): this.type = set(nullStringParam, value)

  /** @group getParam */
  def getNullStringParam: String = $(nullStringParam)


  setDefault(nullStringParam, "__NULL__")
}

trait NullBooleanParam extends Params {

  /**
   * Param for default value used in case of null value encounter on boolean fields.
   *
   * @group param
   */
  val nullBooleanParam: Param[Boolean] = new Param[Boolean](this, "nullBooleanParam", "default value for null value encounter on boolean fields") {
    /** Encodes a param value into JSON, which can be decoded by `jsonDecode()`. */
    override def jsonEncode(value: Boolean): String = compact(render(JBool(value)))

    /** Decodes a param value from JSON. */
    override def jsonDecode(json: String): Boolean = {
      implicit val formats = DefaultFormats
      val jObject = parse(json).extract[JObject]
      val boolParamValue = (jObject \ "nullBooleanParam").extract[Boolean]
      boolParamValue
    }
  }

  /**
   *
   * @param value Boolean
   * @return this.type
   */
  def setNullBooleanParam(value: Boolean): this.type = set(nullBooleanParam, value)

  /** @group getParam */
  def getNullBooleanParam: Boolean = $(nullBooleanParam)

  setDefault(nullBooleanParam, false)
}


trait NullNumericParam extends Params {

  /**
   * Param for default value used in case of null value encounter on numeric fields.
   *
   * @group param
   */
  val nullNumericParam: Param[Double] = new Param[Double](this, "nullNumericParam", "default value for null value encounter on numeric fields") {
    /** Encodes a param value into JSON, which can be decoded by `jsonDecode()`. */
    override def jsonEncode(value: Double): String = compact(render(JDouble(value)))

    /** Decodes a param value from JSON. */
    override def jsonDecode(json: String): Double = {
      implicit val formats = DefaultFormats
      val jObject = parse(json).extract[JObject]
      val doubleParamValue = (jObject \ "nullNumericParam").extract[Double]
      doubleParamValue
    }
  }

  /** @group getParam */
  def getNullNumericParam: Double = $(nullNumericParam)

  /**
   *
   * @param value Double
   * @return this.type
   */
  def setNullNumericParam(value: Double): this.type = set(nullNumericParam, value)

  setDefault(nullNumericParam, 0.0d)
}

trait NullTimestampParam extends Params {

  /**
   * Param for default value used in case of null value encounter on date / time /timestamp fields.
   *
   * @group param
   */
  val nullTimestampParam: Param[Timestamp] = new Param[Timestamp](this, "nullTimestampParam", "default value for null value encounter on date / time / timestamp fields") {
    /** Encodes a param value into JSON, which can be decoded by `jsonDecode()`. */
    override def jsonEncode(value: Timestamp): String = compact(render(JLong(value.getTime)))

    /** Decodes a param value from JSON. */
    override def jsonDecode(json: String): Timestamp = {
      implicit val formats = DefaultFormats
      val jObject = parse(json).extract[JObject]
      val longParamValue = (jObject \ "nullTimestampParam").extract[Long]
      new Timestamp(longParamValue)
    }
  }


  /**
   *
   * @param value Double
   * @return this.type
   */
  def setNullTimestampParam(value: Timestamp): this.type = set(nullTimestampParam, value)

  /** @group getParam */
  def getNullTimestampParam: Timestamp = $(nullTimestampParam)

  setDefault(nullTimestampParam, new Timestamp(0))
}