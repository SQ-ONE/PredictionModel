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

  val preparedData: DataFrame =  cleanData.drop("Sr_No","RM/ASE/ASM","Rec")
    .deleteRowsWithNull("Collection_Date")
    .convertLowerCase("Region")
    .convertRegionToNumeric("Region")
    .convertCatergoryToFrequency("Product")
    .convertLowerCase("Dealer_Name")
    .convertCatergoryToFrequency("Dealer_Name")

  preparedData.show(5)

  val wrangledData: DataFrame = preparedData.parseColumnAsDouble(0.00,"Dealer_Name","Product","Invoice_Amount","Region","Gross_Collection",
    "Balance_O/s","Usance_till_Collection_Days","Discounting_Tenure","Rate","Disc_Chrges_for_Discouting_Tenure","Early_Collection_Days",
    "Collection_Incentive_on_Amount_Received","Net_Amount_Received").cache()
    //.parseColumnAsDate("ddMMMyy","Invoice_Date")
    //.parseColumnAsDate("dd-MMM-yy","Discounting_Date","Collection_Date","Due_Date")

  wrangledData.show(5)

  //-----Association Rule Learning (FP matching)---------
  //FPMatching(wrangledData.select("Product","Early_Collection_Days").limit(100))


  //------------------Model #1 - Analysis Without Dates---------------------------------------

  val dataWithoutDate: DataFrame = wrangledData.drop("Invoice_Date","Discounting_Date","Collection_Date","Due_Date").cache()

  //------Feature Engineering-----------
  val featuredData :DataFrame = dataWithoutDate.cumSumWithGroupBy("Invoice_No","Payer","Invoice_Amount","Early_Collection_Days","Net_Amount_Received",
    "Collection_Incentive_on_Amount_Received")
    .countWithGroupBy("Invoice_No","Payer","Invoice_Amount")
    .cumWeightedAverage("Invoice_No","Payer","Early_Collection_Days","Invoice_Amount")

/*  val featuredIntermediateData = featuredData
    .sumBasedOnCondition("Invoice_No","Payer","Early_Collection_Days", x => if(x < 0) 1.00 else 0.00)
    .cumAverageWithGroupBy("Invoice_No","Payer","Early_Collection_Days")*/

  featuredData.show(5)

  val data = featuredData.drop("Invoice_No","Payer")

  val assembler = new VectorAssembler()
    .setInputCols(Array(data.columns.filter(x => x != "Early_Collection_Days"): _*))
    .setOutputCol("features")

  val vectorizedData = assembler.transform(data)

  val dataForModel = vectorizedData.withColumnRenamed("Early_Collection_Days","label").select("features","label")

  val splits = dataForModel.randomSplit(Array(0.9, 0.1), seed = 11L)
  val trainingData = splits(0).cache()
  val testData = splits(1)
  
  //-------Linear Regression Model-------------
  //LinearRegressionModel(trainingData,testData)

  //Linear Regression Using Validation
  val resultValidatedLR = TrainValidationForLR(trainingData,testData)
  resultValidatedLR.show()
  println(resultValidatedLR.getRMSE)


  //------------ Using normalized data-----------------
/*  val normDataMinMax = dataForModel.minMaxScaledData.drop("features").withColumnRenamed("scaledFeatures","features")
  val splitNormData = normDataMinMax.randomSplit(Array(0.9, 0.1), seed = 11L)
  val normTrainingData = splitNormData(0).cache()
  val normTestData = splitNormData(1)

  val resultNormValidatedLR = TrainValidationForLR(normTrainingData,normTestData)
  resultNormValidatedLR.show()
  println(resultNormValidatedLR.getRMSE)*/

  //--------------Model #2 - Analysis With Dates--------------
}
