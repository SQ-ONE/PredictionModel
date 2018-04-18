package com.squareone.bankiq

import org.apache.spark.sql.SparkSession

trait SparkServices

class SparkService extends SparkServices {
}


object SparkService {

  def getSparkSession(): SparkSession = {

    val session = SparkSession.getActiveSession
    session match {
      case None => createSparkSession()
      case Some(_) => session.get
    }

  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder().appName("PredictionModel").master("local[*]").getOrCreate()
  }
}
