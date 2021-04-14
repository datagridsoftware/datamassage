package com.datagrid.datamassage.feature

import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}




trait AutoDataMassageBase extends Params with HasHandleInvalid with HasInputCol with HasInputCols
  with HasOutputCol with HasOutputCols with HasMaxCategories{

  /**
   * Param for how to order labels of categorical columns. The first label after ordering is assigned
   * an index of 0.
   * Options are:
   *   - 'frequencyDesc': descending order by label frequency (most frequent label assigned 0)
   *   - 'frequencyAsc': ascending order by label frequency (least frequent label assigned 0)
   *   - 'alphabetDesc': descending alphabetical order
   *   - 'alphabetAsc': ascending alphabetical order
   * Default is 'frequencyDesc'.
   *
   * @group param
   */
  final val categoricalFieldsOrderType: Param[String] = new Param(this, "stringOrderType",
    "How to order labels of string column. " +
      "The first label after ordering is assigned an index of 0. " +
      s"Supported options: ${AutoDataMassage.supportedStringOrderType.mkString(", ")}.",
    ParamValidators.inArray(AutoDataMassage.supportedStringOrderType))

}

object AutoDataMassage extends DefaultParamsReadable[AutoDataMassage] {
  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val KEEP_INVALID: String = "keep"
  private[feature] val supportedHandleInvalids: Array[String] =
    Array(SKIP_INVALID, ERROR_INVALID, KEEP_INVALID)
  private[feature] val frequencyDesc: String = "frequencyDesc"
  private[feature] val frequencyAsc: String = "frequencyAsc"
  private[feature] val alphabetDesc: String = "alphabetDesc"
  private[feature] val alphabetAsc: String = "alphabetAsc"
  private[feature] val supportedStringOrderType: Array[String] =
    Array(frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc)

  override def load(path: String): AutoDataMassage = super.load(path)

}

class AutoDataMassage(override val uid: String) extends Estimator[AutoDataMassageModel] with AutoDataMassageBase with DefaultParamsWritable {
  override def fit(dataset: Dataset[_]): AutoDataMassageModel = ???

  override def copy(extra: ParamMap): Estimator[AutoDataMassageModel] = ???

  override def transformSchema(schema: StructType): StructType = ???
}

object AutoDataMassageModel extends MLReadable[AutoDataMassageModel] {
  class AutoDataMassageModelWriter(instance: AutoDataMassageModel) extends MLWriter {

    private case class Data(labelsArray: Array[Array[String]])

    override protected def saveImpl(path: String): Unit = {
      ???
    }
  }

  private class AutoDataMassageModelReader extends MLReader[AutoDataMassageModel] {

    private val className = classOf[AutoDataMassageModel].getName

    override def load(path: String): AutoDataMassageModel = ???
  }

  override def read: MLReader[AutoDataMassageModel] = ???

  override def load(path: String): AutoDataMassageModel = super.load(path)


}

class AutoDataMassageModel(override val uid: String)
  extends Model[AutoDataMassageModel] with AutoDataMassageBase with MLWritable {

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  override def copy(extra: ParamMap): AutoDataMassageModel = ???

  override def write: MLWriter = ???

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def transformSchema(schema: StructType): StructType = ???
}
