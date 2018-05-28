package com.squareone.bankiq.utility

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

trait SparkServices

class SparkService extends SparkServices {
}


object SparkService {

  private val config = ConfigFactory.load()

  def getSparkSession(): SparkSession = {

    lazy val session = SparkSession.getActiveSession
    session.getOrElse(createSparkSession())
    }

  def createSparkSession(): SparkSession = {
    lazy val sessionCreation = SparkSession
      .builder()
      .master( config.getConfig( "spark" ).getString("masterURL") )
      .appName(config.getConfig("application").getString( "applicationName" ) )
      .getOrCreate()
    sessionCreation
  }
}
