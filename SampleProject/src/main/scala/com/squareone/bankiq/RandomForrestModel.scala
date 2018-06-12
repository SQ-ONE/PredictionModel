package com.squareone.bankiq

import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}

object RandomForrestModel {
  private val config = ConfigFactory.load("application.conf")
  val path = config.getConfig("filePaths").getString("savedModelFilePath")
  def apply(trainingData: DataFrame,testData: DataFrame): DataFrame = {
    val rfr = new RandomForestRegressor()
      .setMaxDepth(20)
      .setNumTrees(20)
      .setMaxBins(20)
      .setSeed(12345)
      .setImpurity("variance")

    val rfrModel = rfr.fit(trainingData)

    rfrModel.write.overwrite().save(path)

    rfrModel.transform(testData)
      .select("features", "label", "prediction")
  }
}
