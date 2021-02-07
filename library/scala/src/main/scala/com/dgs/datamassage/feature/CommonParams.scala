package com.dgs.datamassage.feature

import org.apache.spark.ml.param.{Params, StringArrayParam}

trait HasCatNumericalInputCols extends Params {

  /**
   * Param for categorical numeric input column names.
   * @group param
   */
  final val catNumericalInputCols: StringArrayParam = new StringArrayParam(this, "catNumericalInputCols", "categorical numeric input column names")

  /** @group getParam */
  final def getCatNumericInputCols: Array[String] = $(catNumericalInputCols)
}