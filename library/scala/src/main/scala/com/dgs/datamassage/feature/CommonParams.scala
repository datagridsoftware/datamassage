package com.dgs.datamassage.feature

import org.apache.spark.ml.param.{Param, ParamValidators, Params, StringArrayParam}

import java.util.Date

trait HasCatNumericalInputCols extends Params {

  /**
   * Param for categorical numeric input column names.
   * @group param
   */
  final val catNumericalInputCols: StringArrayParam = new StringArrayParam(this, "catNumericalInputCols", "categorical numeric input column names")

  /** @group getParam */
  final def getCatNumericInputCols: Array[String] = $(catNumericalInputCols)
}

trait NullStringParam extends Params {

  /**
   * Param for default value used in case of null value encounter on string fields.
   * @group param
   */
  val nullStringParam: Param[String] = new Param[String](this, "nullStringParam", "default value for null value encounter on string field")

  /** @group getParam */
  final def getNullStringParam: String = $(nullStringParam)

  setDefault(nullStringParam, "__NULL__")
}

trait NullBooleanParam extends Params {

  /**
   * Param for default value used in case of null value encounter on boolean fields.
   * @group param
   */
  val nullBooleanParam: Param[Boolean] = new Param[Boolean](this, "nullBooleanParam", "default value for null value encounter on boolean fields")

  /** @group getParam */
  final def getNullBooleanParam: Boolean = $(nullBooleanParam)

  setDefault(nullBooleanParam, false)
}


trait NullNumericParam extends Params {

  /**
   * Param for default value used in case of null value encounter on numeric fields.
   * @group param
   */
  val nullNumericParam: Param[Double] = new Param[Double](this, "nullNumericParam", "default value for null value encounter on numeric fields")

  /** @group getParam */
  final def getNullNumericParam: Double = $(nullNumericParam)

  setDefault(nullNumericParam, 0.0d)
}

trait NullDateParam extends Params {

  /**
   * Param for default value used in case of null value encounter on date / time /timestamp fields.
   * @group param
   */
  val nullDateParam: Param[Date] = new Param[Date](this, "nullDateParam", "default value for null value encounter on date / time / timestamp fields")

  /** @group getParam */
  final def getNullDateParam: Date = $(nullDateParam)
  setDefault(nullDateParam, new Date(0))
}