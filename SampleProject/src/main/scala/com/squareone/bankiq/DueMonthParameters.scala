package com.squareone.bankiq

import com.squareone.bankiq.utility.SparkService
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.cassandra._
import com.squareone.bankiq.DataWrangling._
import com.squareone.bankiq.FeatureComputation._

object DueMonthParameters {
  private val config = ConfigFactory.load("application.conf")
  val spark = SparkService.getSparkSession()
  val sc = spark.sparkContext

  import spark.implicits._

  val month = config.getConfig("dbTables").getString("db.cassandra.dueMonth")
  val keyspace = config.getConfig("dbTables").getString("db.cassandra.keySpace")
  val renameColumns: Seq[String] = Seq("month_due_date", "due_month_count", "due_month_cum_invoice_amount", "due_month_cum_usance_till_collection_days", "due_month_cum_early_collection_days"
    , "due_month_cum_collection_incentive_on_amount_received", "due_month_cum_ratio_early_collection_days_discounting_tenure", "due_month_cum_delayed_days")

  def getDueMonthParameters: Dataset[DueMonth] = {
    try{spark.read.cassandraFormat(month, keyspace).load().as[DueMonth]} catch { case e: Exception => spark.createDataset(sc.emptyRDD[DueMonth])}
  }
  implicit  class ComputeDueMonth(data: Dataset[MIS]) {
    def currentDueMonthParameters(data: Dataset[MIS]): Dataset[DueMonth] = {
      val fileParameters = data.toDF().calRatio("early_collection_days", "discounting_tenure")
        .calCondition("early_collection_days", x => if (x > 0) x else 0).getMonthFromDate("due_date").groupBy("month_due_date").agg(count("invoice_no")
        , sum("invoice_amount"), sum("usance_till_collection_days"), sum("early_collection_days"), sum("collection_incentive_on_amount_received")
        , sum("ratio_early_collection_days_discounting_tenure"), sum("condition_early_collection_days"))
      fileParameters.limitDecimal(fileParameters.columns.filter(_ != "month_due_date"): _*).toDF(renameColumns: _*).as[DueMonth]
    }

    def updateDueMonthParameters(data: Dataset[MIS]): Unit = {
      val existingParams = getDueMonthParameters
      val currentFileParams = currentDueMonthParameters(data)
      val newParams = existingParams.union(currentFileParams).groupBy("month_due_date").agg(sum("due_month_count")
        ,sum("due_month_cum_invoice_amount"), sum("due_month_cum_usance_till_collection_days"), sum("due_month_cum_early_collection_days")
        ,sum("due_month_cum_collection_incentive_on_amount_received"), sum("due_month_cum_ratio_early_collection_days_discounting_tenure")
        ,sum("due_month_cum_delayed_days")).toDF(renameColumns: _*).as[DueMonth]
      newParams.write.cassandraFormat(month, keyspace)
    }
  }

  implicit class DueMonthFeatures(data: DataFrame) {
    def addDueMonthFeaturestoInvoice = {
      val existingParams = getDueMonthParameters
      data.join(existingParams, Seq("month_due_date"),"left_outer")
    }
  }
}
