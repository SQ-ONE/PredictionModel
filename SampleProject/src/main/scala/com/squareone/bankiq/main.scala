package com.squareone.bankiq

import com.squareone.bankiq.DataPreparation._
import com.squareone.bankiq.DataCleaning._
import com.squareone.bankiq.DataWrangling._
import com.squareone.bankiq.FeatureComputation._
import com.squareone.bankiq.NormalizeFuctions._
import com.squareone.bankiq.ModelEvaluator._
import com.squareone.bankiq.utility.SparkService
import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.expressions.Window
import scala.reflect.runtime.{universe => ru}
import ru._


object main extends App{
  private val config = ConfigFactory.load( "application.conf" )
  val spark = SparkService.createSparkSession()
  val sqlContext = spark.sqlContext
  import spark.implicits._

  val tableForData = config.getConfig("dbTables").getString("db.cassandra.data")
  val keyspace = config.getConfig("dbTables").getString("db.cassandra.keySpace")

  val file = spark.read.cassandraFormat(tableForData, keyspace).load().drop("period","year","month").as[MIS_Initial]

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
    "net_amount_received","gross_collection").cache()

  val dataInMIS = wrangledData.as[MIS]

/*  val abs = udf{(value: Double ) => if(value > 0.00)value else (-1.00 * value)}
    println(dataInMIS.toDF().select("early_collection_days").withColumn("abs_early_collection_days",abs(dataInMIS("early_collection_days")))
    .drop("early_collection_days").rdd.map(_(0).asInstanceOf[Double]).reduce(_+_))
  println(dataInMIS.toDF().count)*/

  val preparedData: DataFrame =  dataInMIS.toDF()
    .convertLowerCase("region")
    .convertRegionToNumeric("region")

  preparedData.show()

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
  val dataForModel3 = preparedData.drop("dealer_name","sr_no","rec")
  Model3(dataForModel3)

  //------------------Model #4 = Analysis With Dates----------------

}
