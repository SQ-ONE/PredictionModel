package com.squareone.bankiq

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.DataFrame

object LinearRegressionModel {
  def apply(trainingData: DataFrame,testData: DataFrame) = {
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    val lrModel = lr.fit(trainingData)

    lrModel.transform(testData)
      .select("features", "label", "prediction")
  }

}
