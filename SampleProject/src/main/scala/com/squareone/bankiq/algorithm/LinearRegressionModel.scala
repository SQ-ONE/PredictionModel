package com.squareone.bankiq.algorithm

import com.squareone.bankiq.data.preparation.PreparedData
import com.squareone.bankiq.{PredictedResult, Query}
import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.spark.SparkContext
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.ParamGridBuilder

class LinearRegressionModel extends P2LAlgorithm[PreparedData, org.apache.spark.ml.regression.LinearRegressionModel, Query, PredictedResult] {
  override def train(sc: SparkContext, pd: PreparedData): org.apache.spark.ml.regression.LinearRegressionModel = {

    val df = pd.df
    val lr = new LinearRegression()
      .setMaxIter(3)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    val model = lr.fit(df)
    model
  }

  override def predict(model: org.apache.spark.ml.regression.LinearRegressionModel, query: Query): PredictedResult = {
    PredictedResult(2.9)
  }
}
