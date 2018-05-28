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
  val renameColumns: Seq[String] = Seq("payer","count","cum_invoice_amount","cum_usance_till_collection_days","cum_early_collection_days"
    ,"cum_collection_incentive_on_amount_received","cum_ratio_early_collection_days_discounting_tenure","cum_delayed_days")

  def getDealerParameters: Dataset[Dealer] = {
    val file: Dataset[Dealer] = Option(spark.read.cassandraFormat(dealer, keyspace).load()) match {
      case Some(x) => x.as[Dealer]
      case None => spark.createDataset(sc.emptyRDD[Dealer])
    }
    file
  }

  def currentDealerParameters(data: Dataset[MIS]) = {

    val fileParameters = data.toDF().calRatio("early_collection_days","discounting_tenure")
      .calCondition("early_collection_days",x => if(x>0)x else 0).groupBy("payer").agg(count("invoice_no")
      ,sum("invoice_amount"),sum("usance_till_collection_days"),sum("early_collection_days"),sum("collection_incentive_on_amount_received")
      ,sum("ratio_early_collection_days_discounting_tenure"),sum("condition_early_collection_days"))
    fileParameters.limitDecimal(fileParameters.columns.filter(_ != "payer"): _*).toDF(renameColumns: _*).as[Dealer]
  }

  def updateDealerParameters(data: Dataset[MIS]) = {
    val existingParams = getDealerParameters
    val currentFileParams = currentDealerParameters(data)
    val newParams = existingParams.union(currentFileParams).groupBy("payer").agg(sum("count"),sum("sumInvoiceAmount"),sum("sumUsance"),sum("sumEarlyColectionDays"),
      sum("sumCollectionIncentive"),sum("sumRatioEarlyCollectionDayVsPeriod")).toDF(renameColumns: _*).as[Dealer]
    newParams.write.cassandraFormat(dealer,keyspace)
  }

  def addFeaturestoInvoice(data: Dataset[Invoice]) = {
    val existingParams = getDealerParameters
    data.join(existingParams,"payer")
  }
}

