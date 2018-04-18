package com.squareone.bankiq

import com.squareone.bankiq.DataPreparation._
import com.squareone.bankiq.DataCleaning._
import com.squareone.bankiq.DataWrangling._
import com.squareone.bankiq.FeatureComputation._
import org.apache.spark.sql.DataFrame

object main extends App{
  val spark = SparkService.createSparkSession()
  val sqlContext = spark.sqlContext

  val file = sqlContext.read.option("header","true").csv("/home/shridhar/Desktop/SampleProject/src/main/resources/sample.csv").cache()

  val cleanData = file.removeSpaceFromHeader
    .removeAllSpaces
    .removeHyphen("Balance_O/s", "Collection_Incentive_on_Amount_Received", "Net_Amount_Received")
    .removePercent("Rate")
    .removeParenthesis("Gross_Collection", "Usance_till_Collection_Days", "Disc_Chrges_for_Discouting_Tenure",
    "Early_Collection_Days", "Collection_Incentive_on_Amount_Received", "Net_Amount_Received")
    .removeComma("Invoice_Amount","Gross_Collection", "Usance_till_Collection_Days", "Disc_Chrges_for_Discouting_Tenure",
      "Early_Collection_Days", "Collection_Incentive_on_Amount_Received", "Net_Amount_Received").cache()

  val preparedDate =  cleanData.dropColumns("Sr_No","RM/ASE/ASM","Rec")
    .deleteRowsWithNull("Collection_Date")
    .convertLowerCase("Region")
    .convertRegionToNumeric("Region")
    .convertCatergoryToFrequency("Product")
    .convertLowerCase("Dealer_Name")
    .convertCatergoryToFrequency("Dealer_Name").cache()

  val wrangledData = preparedDate.parseColumnAsDouble(0.00,"Dealer_Name","Product","Invoice_Amount","Region","Gross_Collection",
    "Balance_O/s","Usance_till_Collection_Days","Discounting_Tenure","Rate","Disc_Chrges_for_Discouting_Tenure","Early_Collection_Days",
    "Collection_Incentive_on_Amount_Received","Net_Amount_Received").cache()
    //.parseColumnAsDate("ddMMMyy","Invoice_Date")
    //.parseColumnAsDate("dd-MMM-yy","Discounting_Date","Collection_Date","Due_Date")

  //Model #1 - Analysis Without Dates

  val dataWithoutDate = wrangledData.drop("Invoice_Date","Discounting_Date","Collection_Date","Due_Date").cache()

  //Feature Engineering

  val step1 :DataFrame = dataWithoutDate.cumSumWithGroupBy("Invoice_No","Payer","Invoice_Amount","Early_Collection_Days","Net_Amount_Received",
    "Collection_Incentive_on_Amount_Received")
    .countWithGroupBy("Invoice_No","Payer","Invoice_Amount")
    .cumWeightedAverage("Invoice_No","Payer","Early_Collection_Days","Invoice_Amount")
    .cumAverageWithGroupBy("Invoice_No","Payer","Early_Collection_Days","Collection_Incentive_on_Amount_Received").cache()

  val step2 = step1.sumBasedOnCondition("Invoice_No","Payer","Early_Collection_Days", x => if(x < 0) 1.00 else 0.00)
  step2.show()
}
