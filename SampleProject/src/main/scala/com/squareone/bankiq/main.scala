package com.squareone.bankiq

import com.squareone.bankiq.DataPreparation._
import com.squareone.bankiq.DataCleaning._
import com.squareone.bankiq.DataWrangling._
import com.squareone.bankiq.FeatureComputation._
import com.squareone.bankiq.NormalizeFuctions._
import com.squareone.bankiq.ModelEvaluator._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.sql.{DataFrame, Row}


object main extends App{
  val spark = SparkService.createSparkSession()
  val sqlContext = spark.sqlContext

  val file = sqlContext.read.option("header","true").csv("/home/shridhar/Desktop/SampleProject/src/main/resources/sample.csv").cache()

  file.show(5)

  val cleanData: DataFrame = file.removeSpaceFromHeader
    .removeAllSpaces
    .removeHyphen("Balance_O/s", "Collection_Incentive_on_Amount_Received", "Net_Amount_Received")
    .removePercent("Rate")
    .removeParenthesis("Gross_Collection", "Usance_till_Collection_Days", "Disc_Chrges_for_Discouting_Tenure",
    "Early_Collection_Days", "Collection_Incentive_on_Amount_Received", "Net_Amount_Received")
    .removeComma("Invoice_Amount","Gross_Collection", "Usance_till_Collection_Days", "Disc_Chrges_for_Discouting_Tenure",
      "Early_Collection_Days", "Collection_Incentive_on_Amount_Received", "Net_Amount_Received")

  cleanData.show(5)

  val preparedData: DataFrame =  cleanData.drop("Sr_No","Dealer_Name","Rec")
    .deleteRowsWithNull("Collection_Date")
    .convertLowerCase("Region")
    .convertRegionToNumeric("Region")
    .convertCatergoryToFrequency("Product")
    .convertLowerCase("RM/ASE/ASM")
    .convertCatergoryToFrequency("RM/ASE/ASM")

  preparedData.show(5)

  val wrangledData: DataFrame = preparedData.parseColumnAsDouble(0.00,"RM/ASE/ASM","Product","Invoice_Amount","Region","Gross_Collection",
    "Balance_O/s","Usance_till_Collection_Days","Discounting_Tenure","Rate","Disc_Chrges_for_Discouting_Tenure","Early_Collection_Days",
    "Collection_Incentive_on_Amount_Received","Net_Amount_Received").cache()
    //.parseColumnAsDate("ddMMMyy","Invoice_Date")
    //.parseColumnAsDate("dd-MMM-yy","Discounting_Date","Collection_Date","Due_Date")

  wrangledData.show(5)

  //-----Association Rule Learning (FP matching)---------
  FPMatching(wrangledData.select("Product","Early_Collection_Days").limit(100))


  //------------------Model #1 - Analysis Without Dates---------------------------------------
  //Model1(wrangledData)


  //------------------Model #2 - Random Forrest-----------------------------------------
  //Model2(wrangledData)

}
