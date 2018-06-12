package com.squareone.bankiq

import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import com.squareone.bankiq.FeatureComputation._
import com.squareone.bankiq.utility._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.cassandra._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import com.squareone.bankiq.DataWrangling._

object DealerParameters {
  private val config = ConfigFactory.load( "application.conf" )
  val spark = SparkService.getSparkSession()
  val sc = spark.sparkContext
  import spark.implicits._

  val dealer = config.getConfig("dbTables").getString("db.cassandra.dealer")
  val keyspace = config.getConfig("dbTables").getString("db.cassandra.keySpace")
  val renameColumns: Seq[String] = Seq("payer","dealer_count","dealer_cum_invoice_amount","dealer_cum_usance_till_collection_days","dealer_cum_early_collection_days"
    ,"dealer_cum_collection_incentive_on_amount_received","dealer_cum_ratio_early_collection_days_discounting_tenure","dealer_cum_delayed_days")

  def getDealerParameters: Dataset[Dealer] = {
    try(spark.read.cassandraFormat(dealer, keyspace).load().as[Dealer]) catch { case e: Exception => spark.createDataset(sc.emptyRDD[Dealer])}
  }
  implicit class ComputeDealer(data: Dataset[MIS]) {
    def currentDealerParameters(data: Dataset[MIS]) = {

      val fileParameters = data.toDF().calRatio("early_collection_days", "discounting_tenure")
        .calCondition("early_collection_days", x => if (x > 0) x else 0).groupBy("payer").agg(count("invoice_no")
        , sum("invoice_amount"), sum("usance_till_collection_days"), sum("early_collection_days"), sum("collection_incentive_on_amount_received")
        , sum("ratio_early_collection_days_discounting_tenure"), sum("condition_early_collection_days"))
      fileParameters.limitDecimal(fileParameters.columns.filter(_ != "payer"): _*).toDF(renameColumns: _*).as[Dealer]
    }
    def updateDealerParameters(data: Dataset[MIS]) = {
      val existingParams = getDealerParameters
      val currentFileParams = currentDealerParameters(data)
      val newParams = existingParams.union(currentFileParams).groupBy("payer").agg(sum("dealer_count"), sum("dealer_cum_invoice_amount")
        , sum("dealer_cum_usance_till_collection_days"), sum("dealer_cum_early_collection_days")
        ,sum("dealer_cum_collection_incentive_on_amount_received"), sum("dealer_cum_ratio_early_collection_days_discounting_tenure")
        ,sum("dealer_cum_delayed_days")).toDF(renameColumns: _*).as[Dealer]
      newParams.write.cassandraFormat(dealer, keyspace)
    }
  }
  implicit class DealerFeatures(data: DataFrame) {
    def addDealerFeaturestoInvoice: DataFrame = {
      val existingParams = getDealerParameters
      data.join(existingParams, Seq("payer"),"left_outer")
    }
  }
}

