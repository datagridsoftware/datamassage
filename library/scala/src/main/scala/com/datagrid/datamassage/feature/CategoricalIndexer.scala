package com.datagrid.datamassage.feature

import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}


trait CategoricalIndexerBase extends Params with HasHandleInvalid with HasInputCol with HasInputCols with HasCategoricalNumericalInputCols
  with HasOutputCol with HasOutputCols {
}

object CategoricalIndexer extends DefaultParamsReadable[CategoricalIndexer] {
  override def load(path: String): CategoricalIndexer = super.load(path)
}

class CategoricalIndexer(override val uid: String) extends Estimator[CategoricalIndexerModel] with CategoricalIndexerBase {

  def this() = this(Identifiable.randomUID("catIdx"))

  override def fit(dataset: Dataset[_]): CategoricalIndexerModel = ???

  override def copy(extra: ParamMap): Estimator[CategoricalIndexerModel] = ???

  override def transformSchema(schema: StructType): StructType = ???

}


object CategoricalIndexerModel extends MLReadable[CategoricalIndexerModel] {

  class CategoricalIndexerModelWriter(instance: CategoricalIndexerModel) extends MLWriter {

    private case class Data(labelsArray: Array[Array[String]])

    override protected def saveImpl(path: String): Unit = {
      ???
    }
  }

  private class CategoricalIndexerModelReader extends MLReader[CategoricalIndexerModel] {

    private val className = classOf[CategoricalIndexerModel].getName

    override def load(path: String): CategoricalIndexerModel = ???
  }

  override def read: MLReader[CategoricalIndexerModel] = ???

  override def load(path: String): CategoricalIndexerModel = super.load(path)
}

class CategoricalIndexerModel(override val uid: String) extends Model[CategoricalIndexerModel] with MLWritable {
  override def copy(extra: ParamMap): CategoricalIndexerModel = ???

  override def transformSchema(schema: StructType): StructType = ???

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def write: MLWriter = ???
}
