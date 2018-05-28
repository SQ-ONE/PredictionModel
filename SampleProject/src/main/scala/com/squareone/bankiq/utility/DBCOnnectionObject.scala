package com.squareone.bankiq.utility

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

object DBConnectionObject {

  private val config = ConfigFactory.load()

  def getCassandraSparkConnection()  = {

    val conf = new SparkConf(true)
      .set("", config.getConfig( "dataBase" ).toString)

    val sc = new SparkContext(config.getString("dataBase.db.cassandra.master"),config.getString("application.applicationName"),conf)
    sc
  }
}
