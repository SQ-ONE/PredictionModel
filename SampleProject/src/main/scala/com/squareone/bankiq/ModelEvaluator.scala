package com.squareone.bankiq

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}

object ModelEvaluator {
  implicit class evaluate(data: DataFrame ){
    val evaluatorRegession = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("label")

      def getRMSE = {
        evaluatorRegession.evaluate(data)
      }
      def getMSE = {
        evaluatorRegession.setMetricName("mse").evaluate(data)
      }
      def getR2 = {
        evaluatorRegession.setMetricName("r2").evaluate(data)
      }
      def getMAE = {
        evaluatorRegession.setMetricName("mae").evaluate(data)
      }
  }

}
