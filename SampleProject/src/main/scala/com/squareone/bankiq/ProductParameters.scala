package com.squareone.bankiq

import com.squareone.bankiq.utility.SparkService
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import com.squareone.bankiq.utility._
import com.squareone.bankiq.FeatureComputation._
import com.squareone.bankiq.DataWrangling._

object ProductParameters {
  private val config = ConfigFactory.load( "application.conf" )
  val spark = SparkService.getSparkSession()
  val sc = spark.sparkContext
  import spark.implicits._

  val product = config.getConfig("dbTables").getString("db.cassandra.product")
  val keyspace = config.getConfig("dbTables").getString("db.cassandra.keySpace")
  val renameColumns: Seq[String] = Seq("payer","product_count","product_cum_invoice_amount","product_cum_usance_till_collection_days","product_cum_early_collection_days"
    ,"product_cum_collection_incentive_on_amount_received","product_cum_ratio_early_collection_days_discounting_tenure","product_cum_delayed_days")

  def getProductParameters: Dataset[Product] = {
    try(spark.read.cassandraFormat(product, keyspace).load().as[Product]) catch { case e: Exception => spark.createDataset(sc.emptyRDD[Product])}
  }

  def currentProductParameters(data: Dataset[MIS]): Dataset[Product] = {

    val fileParameters = data.toDF().calRatio("early_collection_days","discounting_tenure")
      .calCondition("early_collection_days",x => if(x>0)x else 0).groupBy("product").agg(count("invoice_no")
      ,sum("invoice_amount"),sum("usance_till_collection_days"),sum("early_collection_days"),sum("collection_incentive_on_amount_received")
      ,sum("ratio_early_collection_days_discounting_tenure"),sum("condition_early_collection_days"))
    fileParameters.limitDecimal(fileParameters.columns.filter(_ != "product"): _*).toDF(renameColumns: _*).as[Product]
  }

  def updateProductParameters(data: Dataset[MIS]) = {
    val existingParams = getProductParameters
    val currentFileParams = currentProductParameters(data)
    val newParams = existingParams.union(currentFileParams).groupBy("product").agg(sum("count"),sum("sumInvoiceAmount"),sum("sumUsance"),sum("sumEarlyColectionDays"),
      sum("sumCollectionIncentive"),sum("sumRatioEarlyCollectionDayVsPeriod")).toDF(renameColumns: _*).as[Product]
    newParams.write.cassandraFormat(product,keyspace)
  }

  def addFeaturestoInvoice(data: Dataset[Invoice]) = {
    val existingParams = getProductParameters
    data.join(existingParams,"product")
  }
}
