package com.squareone.bankiq

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.regression.RandomForestRegressor

object RandomForrestModel {
  def apply(trainingData: DataFrame,testData: DataFrame): DataFrame = {
    val rfr = new RandomForestRegressor()
      .setMaxDepth(20)
      .setNumTrees(20)
      .setMaxBins(20)
      .setSeed(12345)
      .setImpurity("variance")

    val rfrModel = rfr.fit(trainingData)

    rfrModel.transform(testData)
      .select("features", "label", "prediction")
  }
}
