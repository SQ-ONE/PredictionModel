package com.squareone.bankiq

import com.squareone.bankiq.utility.SparkService
import com.typesafe.config.ConfigFactory

import com.squareone.bankiq.DataPreparation._
import com.squareone.bankiq.DataCleaning._
import com.squareone.bankiq.DataWrangling._
import com.squareone.bankiq.utility.SparkService
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.cassandra._

object TrainModel {
  private val config = ConfigFactory.load( "application.conf" )
  val spark = SparkService.createSparkSession()
  val sqlContext = spark.sqlContext
  import spark.implicits._

  val tableForData2016 = config.getConfig("dbTables").getString("db.cassandra.data2016")
  val tableForData2017 = config.getConfig("dbTables").getString("db.cassandra.data2017")
  val tableForData2018 = config.getConfig("dbTables").getString("db.cassandra.data2018")
  val keyspace = config.getConfig("dbTables").getString("db.cassandra.keySpace")

  val file2016 = spark.read.cassandraFormat(tableForData2016, keyspace).load().drop("period","year","month").as[MIS_Initial]
  val file2017 = spark.read.cassandraFormat(tableForData2017, keyspace).load().drop("period","year","month").as[MIS_Initial]
  val file2018 = spark.read.cassandraFormat(tableForData2018, keyspace).load().drop("period","year","month").as[MIS_Initial]

  val file = file2016.union(file2017)

  val relevantData = file.toDF().deleteRowsWithNull("collection_date").removeRepitition("invoice_no","invoice_amount")

  val cleanData: DataFrame = relevantData.removeAllSpaces
    .removeHyphen("balance_os", "collection_incentive_on_amount_received", "net_amount_received")
    .removePercent("rate")
    .removeParenthesis("gross_collection", "usance_till_collection_days", "disc_chrges_for_discouting_tenure",
      "early_collection_days", "collection_incentive_on_amount_received", "net_amount_received")
    .removeComma("invoice_amount","gross_collection", "usance_till_collection_days", "disc_chrges_for_discouting_tenure",
      "early_collection_days", "collection_incentive_on_amount_received", "net_amount_received")

  val wrangledData: DataFrame = cleanData.parseColumnAsDouble(0.00,"invoice_amount","balance_os","usance_till_collection_days",
    "discounting_tenure","rate","disc_chrges_for_discouting_tenure","early_collection_days","collection_incentive_on_amount_received",
    "net_amount_received","gross_collection").parseColumnAsDate("collection_date","discounting_date","due_date","invoice_date")()
    .cache()

  val dataInMIS = wrangledData.as[MIS]

  val preparedData: DataFrame =  dataInMIS.toDF()
    .convertLowerCase("region")
    .convertRegionToNumeric("region")

  //------------------Model #1 - Analysis Without Dates---------------------------------------
  /*  val dataForModel1 = preparedData.drop("dealer_name","sr_no","rec")
    Model1(dataForModel1)*/


  //------------------Model #2 - Random Forrest-----------------------------------------
  /*  val preparedDataModified = preparedData.convertCatergoryToFrequency("product")
      .convertLowerCase("rm_ase_asm")
      .convertCatergoryToFrequency("rm_ase_asm")
    val dataForModel2 = preparedDataModified.drop("dealer_name","sr_no","rec")
    Model2(dataForModel2)*/

  //------------------Model #3 - Analysis Without Dates---------------------------------------
  /*  val dataForModel3 = preparedData.drop("dealer_name","sr_no","rec")
    Model3(dataForModel3)*/

  //------------------Model #4 = Analysis With Dates----------------
  /* val dataForModel4 = preparedData.drop("dealer_name","sr_no","rec")
   Model4(dataForModel4)*/

  //------------------Model #5 = Analysis With Dates ------------
  val dataForModel5 = preparedData.drop("dealer_name","sr_no","rec")
  Model5(dataForModel5)
}
