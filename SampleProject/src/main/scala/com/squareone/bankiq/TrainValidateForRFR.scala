package com.squareone.bankiq

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.DataFrame

object TrainValidateForRFR {
  def apply(trainingData: DataFrame,testData: DataFrame): DataFrame = {
    val rfr = new RandomForestRegressor()
      .setSeed(12345)
      .setImpurity("variance")

    val paramGrid = new ParamGridBuilder()
      .addGrid(rfr.maxDepth,Array(30,25,20,15,10))
      .addGrid(rfr.maxBins,Array(100,30,10))
      .addGrid(rfr.numTrees,Array(30,25,20,15,10))
      .build()

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(rfr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setParallelism(2)

    val rfrModel = rfr.fit(trainingData)

    rfrModel.transform(testData)
      .select("features", "label", "prediction")
  }

}
