package com.squareone.bankiq

import com.squareone.bankiq.utility.SparkService
import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.regression.RandomForestRegressionModel

object main extends App{

  private val config = ConfigFactory.load( "application.conf" )
  val path = config.getConfig("filePaths").getString("savedModelFilePath")
  val spark = SparkService.createSparkSession()
  val sqlContext = spark.sqlContext
  import spark.implicits._

  try {
    RandomForestRegressionModel.load(path)
  }
  catch {
    case _: Exception => TrainModel
  }

  //testing
  ReturnOutput.runModel5(Seq(Invoice("12345","motor_car",1000.00,"2018-01-02","2018-04-02",120,"Shridhar",10.50,"South")).toDS())
}
