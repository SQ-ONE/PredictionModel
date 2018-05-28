package com.squareone.bankiq

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler
import com.squareone.bankiq.ModelEvaluator._
import com.squareone.bankiq.NormalizeFuctions._
import com.squareone.bankiq.FeatureComputation._

object Model3 {
  def apply(wrangledData: DataFrame) = {
    val dataWithoutDate: DataFrame = wrangledData.drop("Invoice_Date","Discounting_Date","Collection_Date","Due_Date")

    //------Feature Engineering-----------
    val featuredData :DataFrame = dataWithoutDate.countWithGroupBy("invoice_amount")()
      .cumSumWithGroupBy("early_collection_days","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")()
      .cumRatio("early_collection_days","period")()

    featuredData.show(5)

    val data = featuredData.drop("invoice_no","payer","balance_os","collection_incentive_on_amount_received",
      "disc_chrges_for_discouting_tenure","gross_collection","net_amount_received","usance_till_collection_days")

    val assembler = new VectorAssembler()
      .setInputCols(Array(data.columns.filter(x => x != "early_collection_days"): _*))
      .setOutputCol("features")

    val vectorizedData = assembler.transform(data)

    val dataForModel = vectorizedData.withColumnRenamed ("Early_Collection_Days","label").select("features","label")

/*    val splits = dataForModel.randomSplit(Array(0.9, 0.1), seed = 11L)
    val trainingData = splits(0).cache()
    val testData = splits(1)

    //Linear Regression Using Validation
    val resultValidatedLR = TrainValidationForLR(trainingData,testData)
    resultValidatedLR.show()
    println(resultValidatedLR.getRMSE)*/

    //RandomForrest
    val normDataMinMax = dataForModel.returnNormData(2).drop("features").withColumnRenamed("normFeatures","features")
    val splitNormData = normDataMinMax.randomSplit(Array(0.9, 0.1), seed = 11L)
    val normTrainingData = splitNormData(0).cache()
    val normTestData = splitNormData(1)

    val resultNormValidatedRFR = RandomForrestModel(normTrainingData,normTestData)
    resultNormValidatedRFR.show()
    println(resultNormValidatedRFR.getRMSE)
  }
}
