package com.squareone.bankiq

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import com.squareone.bankiq.ModelEvaluator._
import com.squareone.bankiq.NormalizeFuctions._
import com.squareone.bankiq.FeatureComputation._
import org.apache.spark.sql.functions.udf

object Model1 {
  def apply(wrangledData: DataFrame) = {
    val dataWithoutDate: DataFrame = wrangledData.drop("invoice_date","discounting_date","collection_date","due_date").cache()

    //------Feature Engineering-----------
    val featuredData :DataFrame = dataWithoutDate.countWithGroupBy("invoice_amount")()
      .cumSumWithGroupBy("invoice_amount","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")()
      .cumRatio("early_collection_days","discounting_tenure")()
      .sumBasedOnCondition("early_collection_days",x => if( x > 0.00)x else 0.00)()
      .withColumnRenamed("count_invoice_amount","count").withColumnRenamed("cum_condition_early_collection_days","cum_delayed_days")

    featuredData.show(5)

    val dataWithDroppedColumns = featuredData.drop("balance_os","collection_incentive_on_amount_received",
      "disc_chrges_for_discouting_tenure","gross_collection","net_amount_received","usance_till_collection_days")

    val extendedFeatures = dataWithDroppedColumns.calRatio("cum_invoice_amount","count").calRatio("cum_early_collection_days","count")
      .calRatio("cum_delayed_days","count").calRatio("cum_collection_incentive_on_amount_received","count")

    val data = dataWithDroppedColumns.drop("invoice_no","payer")

    val assembler = new VectorAssembler()
      .setInputCols(Array(data.columns.filter(x => x != "early_collection_days"): _*))
      .setOutputCol("features")

    val vectorizedData = assembler.transform(data)

    val dataForModel = vectorizedData.withColumnRenamed("early_collection_days","label").select("features","label")

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
/*    val normDataMinMax = dataForModel.minMaxScaledData.drop("features").withColumnRenamed("scaledFeatures","features")
      val splitNormData = normDataMinMax.randomSplit(Array(0.9, 0.1), seed = 11L)
      val normTrainingData = splitNormData(0).cache()
      val normTestData = splitNormData(1)

      val resultNormValidatedLR = TrainValidationForLR(normTrainingData,normTestData)
      resultNormValidatedLR.show()
      println(resultNormValidatedLR.getRMSE)*/
  }

}
