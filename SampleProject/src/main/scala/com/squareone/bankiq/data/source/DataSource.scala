package com.squareone.bankiq.data.source


import com.squareone.bankiq.{ActualResult, Query}
import org.apache.predictionio.controller.{EmptyEvaluationInfo, PDataSource}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}


case class TrainingData(df: DataFrame)


class DataSource extends PDataSource[TrainingData, EmptyEvaluationInfo, Query, ActualResult] {
  override def readTraining(sc: SparkContext): TrainingData = {

    val df = SparkSession.builder().getOrCreate().read
      .option("header", "true")
      .csv("/home/utkarsh/Documents/BankIQ/GitHub/PredictionModel/SampleProject/src/main/resources/sample.csv")
    new TrainingData(df)
  }
}
