package com.squareone.bankiq

import com.squareone.bankiq.utility.SparkService
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.cassandra._
import com.squareone.bankiq.DataWrangling._
import com.squareone.bankiq.FeatureComputation._

object MonthParameters {
  private val config = ConfigFactory.load("application.conf")
  val spark = SparkService.getSparkSession()
  val sc = spark.sparkContext

  import spark.implicits._

  val month = config.getConfig("dbTables").getString("db.cassandra.month")
  val keyspace = config.getConfig("dbTables").getString("db.cassandra.keySpace")
  val renameColumns: Seq[String] = Seq("payer", "month_count", "month_cum_invoice_amount", "month_cum_usance_till_collection_days", "month_cum_early_collection_days"
    , "month_cum_collection_incentive_on_amount_received", "month_cum_ratio_early_collection_days_discounting_tenure", "month_cum_delayed_days")

  def getMonthParameters: Dataset[Month] = {
    try(spark.read.cassandraFormat(month, keyspace).load().as[Month]) catch { case e: Exception => spark.createDataset(sc.emptyRDD[Month])}
  }

  def currentMonthParameters(data: Dataset[MIS]) = {
    val fileParameters = data.toDF().calRatio("early_collection_days", "discounting_tenure")
      .calCondition("early_collection_days", x => if (x > 0) x else 0).groupBy("month").agg(count("invoice_no")
      , sum("invoice_amount"), sum("usance_till_collection_days"), sum("early_collection_days"), sum("collection_incentive_on_amount_received")
      , sum("ratio_early_collection_days_discounting_tenure"), sum("condition_early_collection_days"))
    fileParameters.limitDecimal(fileParameters.columns.filter(_ != "month"): _*).toDF(renameColumns: _*).as[Month]
  }

  def updateMonthParameters(data: Dataset[MIS]) = {
    val existingParams = getMonthParameters
    val currentFileParams = currentMonthParameters(data)
    val newParams = existingParams.union(currentFileParams).groupBy("payer").agg(sum("count"), sum("sumInvoiceAmount"), sum("sumUsance"), sum("sumEarlyColectionDays"),
      sum("sumCollectionIncentive"), sum("sumRatioEarlyCollectionDayVsPeriod")).toDF(renameColumns: _*).as[Month]
    newParams.write.cassandraFormat(month, keyspace)
  }

  def addFeaturestoInvoice(data: Dataset[Invoice]) = {
    val existingParams = getMonthParameters
    data.join(existingParams, "payer")
  }
}
