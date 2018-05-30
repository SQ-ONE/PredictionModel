package com.squareone.bankiq

import org.apache.spark.ml.feature.{MaxAbsScaler, MinMaxScaler, Normalizer, StandardScaler}
import org.apache.spark.sql.DataFrame

object NormalizeFuctions {
  implicit class normalize(data: DataFrame) {
    def returnNormData(p: Double): DataFrame = {
      val normalizer = new Normalizer()
        .setInputCol("features")
        .setOutputCol("normFeatures")
        .setP(p)

      normalizer.transform(data).drop("features").withColumnRenamed("normFeatures","features")
    }
    def scaledData(std: Boolean,mean: Boolean): DataFrame ={
      val scaler = new StandardScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")
        .setWithStd(std)
        .setWithMean(mean)

      val scalerModel = scaler.fit(data)
      scalerModel.transform(data).drop("features").withColumnRenamed("scaledFeatures","features")
    }
    def minMaxScaledData ={
      val scaler = new MinMaxScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")

      val scalerModel = scaler.fit(data)
      scalerModel.transform(data).drop("features").withColumnRenamed("scaledFeatures","features")
    }
    def maxAbsScaler = {
      val scaler = new MaxAbsScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")

      val scalerModel = scaler.fit(data)
      scalerModel.transform(data).drop("features").withColumnRenamed("scaledFeatures","features")
    }
  }
}
