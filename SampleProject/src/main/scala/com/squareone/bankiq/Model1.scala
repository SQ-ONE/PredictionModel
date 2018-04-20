package com.squareone.bankiq

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import com.squareone.bankiq.ModelEvaluator._
import com.squareone.bankiq.NormalizeFuctions._
import com.squareone.bankiq.FeatureComputation._

object Model1 {
  def apply(wrangledData: DataFrame) = {
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
/*      val normDataMinMax = dataForModel.minMaxScaledData.drop("features").withColumnRenamed("scaledFeatures","features")
      val splitNormData = normDataMinMax.randomSplit(Array(0.9, 0.1), seed = 11L)
      val normTrainingData = splitNormData(0).cache()
      val normTestData = splitNormData(1)

      val resultNormValidatedLR = TrainValidationForLR(normTrainingData,normTestData)
      resultNormValidatedLR.show()
      println(resultNormValidatedLR.getRMSE)*/
  }

}
