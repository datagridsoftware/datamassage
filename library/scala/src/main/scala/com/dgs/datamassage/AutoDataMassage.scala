package com.dgs.datamassage

import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.{MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}


trait AutoDataMassageBase extends Params with HasHandleInvalid with HasInputCol
  with HasOutputCol with HasInputCols with HasOutputCols {

}


class AutoDataMassage(override val uid: String)extends Estimator[AutoDataMassageModel] {
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

class AutoDataMassageModel ( override val uid: String)
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
