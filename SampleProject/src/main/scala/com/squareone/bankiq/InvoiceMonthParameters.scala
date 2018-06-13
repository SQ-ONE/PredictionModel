package com.squareone.bankiq

import com.squareone.bankiq.utility.SparkService
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{count, sum}
import com.squareone.bankiq.FeatureComputation._
import org.apache.spark.sql.cassandra._
import com.squareone.bankiq.DataWrangling._

object InvoiceMonthParameters {
  private val config = ConfigFactory.load("application.conf")
  val spark = SparkService.getSparkSession()
  val sc = spark.sparkContext

  import spark.implicits._

  val month = config.getConfig("dbTables").getString("db.cassandra.invoiceMonth")
  val keyspace = config.getConfig("dbTables").getString("db.cassandra.keySpace")
  val renameColumns: Seq[String] = Seq("month_invoice_date", "invoice_month_count", "invoice_month_cum_invoice_amount"
    , "invoice_month_cum_usance_till_invoice_days", "invoice_month_cum_early_collection_days"
    , "invoice_month_cum_collection_incentive_on_amount_received", "invoice_month_cum_ratio_early_collection_days_discounting_tenure"
    , "invoice_month_cum_delayed_days")

  def getInvoiceMonthParameters: Dataset[InvoiceMonth] = {
    try {
      spark.read.cassandraFormat(month, keyspace).load().as[InvoiceMonth]
    } catch {
      case e: Exception => spark.createDataset(sc.emptyRDD[InvoiceMonth])
    }
  }

  implicit class ComputeInvoiceMonth(data: Dataset[MIS]) {
    def currentInvoiceMonthParameters(data: Dataset[MIS]): Dataset[InvoiceMonth] = {
      val fileParameters = data.toDF().calRatio("early_collection_days", "discounting_tenure")
        .calCondition("early_collection_days", x => if (x > 0) x else 0).getMonthFromDate("invoice_date").groupBy("month_invoice_date").agg(count("invoice_no")
        , sum ("invoice_amount"), sum ("usance_till_collection_days"), sum ("early_collection_days"), sum ("collection_incentive_on_amount_received")
        , sum ("ratio_early_collection_days_discounting_tenure"), sum ("condition_early_collection_days"))
        fileParameters.limitDecimal (fileParameters.columns.filter (_ != "month_invoice_date"): _*).toDF (renameColumns: _*).as[InvoiceMonth]
      }
    def updateInvoiceMonthParameters(data: Dataset[MIS]): Unit = {
        val existingParams = getInvoiceMonthParameters
        val currentFileParams = currentInvoiceMonthParameters(data)
        val newParams = existingParams.union (currentFileParams).groupBy("month_invoice_date").agg (sum ("invoice_month_count")
        , sum ("invoice_month_cum_invoice_amount"), sum ("invoice_month_cum_usance_till_invoice_days")
        , sum ("invoice_month_cum_early_invoice_days"), sum ("invoice_month_cum_invoice_incentive_on_amount_received")
        , sum ("invoice_month_cum_ratio_early_invoice_days_discounting_tenure"), sum ("invoice_month_cum_delayed_days") )
        .toDF (renameColumns: _*).as[InvoiceMonth]
        newParams.write.cassandraFormat (month, keyspace)
      }
    }
  implicit class InvoiceMonthFeatures(data: DataFrame){
    def addInvoiceMonthFeaturestoInvoice: DataFrame = {
      val existingParams = getInvoiceMonthParameters
      data.join(existingParams, Seq("month_invoice_date"),"left_outer")
    }
  }
}


