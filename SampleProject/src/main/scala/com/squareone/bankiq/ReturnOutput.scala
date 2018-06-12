package com.squareone.bankiq

import org.apache.spark.sql.Dataset
import com.squareone.bankiq.DealerParameters._
import com.squareone.bankiq.ManagerParameters._
import com.squareone.bankiq.ProductParameters._
import com.squareone.bankiq.DueMonthParameters._
import com.squareone.bankiq.InvoiceMonthParameters._
import com.squareone.bankiq.FeatureComputation._
import com.squareone.bankiq.DataPreparation._
import com.squareone.bankiq.DataWrangling._
import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}

object ReturnOutput {
  private val config = ConfigFactory.load("application.conf")
  val path = config.getConfig("filePaths").getString("savedModelFilePath")

  def runModel5(data: Dataset[Invoice]) = {
    val featuredData = data.toDF().getMonthFromDate("due_date","invoice_date").convertRegionToNumeric("region")
      .addDealerFeaturestoInvoice.addManagerFeaturestoInvoice.addProductFeaturestoInvoice
      .addDueMonthFeaturestoInvoice.addInvoiceMonthFeaturestoInvoice

    val inputData = featuredData.select("discounting_tenure","invoice_amount","rate","region","month_due_date","month_invoice_date","dealer_count",
      "dealer_cum_invoice_amount","dealer_cum_usance_till_collection_days","dealer_cum_early_collection_days",
      "dealer_cum_collection_incentive_on_amount_received","product_count","product_cum_invoice_amount"
      ,"product_cum_usance_till_collection_days","product_cum_early_collection_days","product_cum_collection_incentive_on_amount_received"
      ,"manager_count","manager_cum_invoice_amount","manager_cum_usance_till_collection_days","manager_cum_early_collection_days"
      ,"manager_cum_collection_incentive_on_amount_received","due_month_count","due_month_cum_invoice_amount"
      ,"due_month_cum_usance_till_collection_days","due_month_cum_early_collection_days","due_month_cum_collection_incentive_on_amount_received"
      ,"invoice_month_count","invoice_month_cum_invoice_amount","invoice_month_cum_usance_till_invoice_days","invoice_month_cum_early_invoice_days"
      ,"invoice_month_cum_invoice_incentive_on_amount_received").parseAllColumnsAsDouble(0.00)

    val assembler = new VectorAssembler()
      .setInputCols(inputData.columns)
      .setOutputCol("features")

    val vectorizedData = assembler.transform(inputData)

    try{val model = RandomForestRegressionModel.load(path)
    val result = model.transform(vectorizedData)
    result.show
    } catch {case e: Exception => println(s"Model cannot be loaded $e")}
  }
}
