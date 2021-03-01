package com.dgs.datamassage.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}


/**
 * A feature transformer that fills null / missing values with specified default values.
 *
 * This requires one pass over the entire dataset.
 */
class NullsInflater(override val uid: String)
  extends Transformer with HasInputCols with NullStringParam with NullBooleanParam with NullNumericParam with NullDateParam
    with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("emptiesInflater"))

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def copy(extra: ParamMap): Transformer = ???

  override def transformSchema(schema: StructType): StructType = {
    ???
  }
}
