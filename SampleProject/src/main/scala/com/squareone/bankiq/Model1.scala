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
    val featuredData :DataFrame = dataWithoutDate.countWithGroupBy("invoice_amount")(1)
      .cumSumWithGroupBy("invoice_amount","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")(1)
      .cumRatio("early_collection_days","discounting_tenure")(1)
      .sumBasedOnCondition("early_collection_days",x => if( x > 0.00)x else 0.00)(1)
      .withColumnRenamed("dealer_count_invoice_amount","dealer_count").withColumnRenamed("dealer_cum_condition_early_collection_days","dealer_cum_delayed_days")

    val productBasedFeatures = featuredData.countWithGroupBy("invoice_amount")(3)
      .cumSumWithGroupBy("invoice_amount","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")(3)
      .cumRatio("early_collection_days","discounting_tenure")(3)
      .sumBasedOnCondition("early_collection_days",x => if( x > 0.00)x else 0.00)(3)
      .withColumnRenamed("product_count_invoice_amount","product_count").withColumnRenamed("product_cum_condition_early_collection_days","product_cum_delayed_days")

    val managerBasedFeatures = productBasedFeatures.countWithGroupBy("invoice_amount")(4)
      .cumSumWithGroupBy("invoice_amount","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")(4)
      .cumRatio("early_collection_days","discounting_tenure")(4)
      .sumBasedOnCondition("early_collection_days",x => if( x > 0.00)x else 0.00)(4)
      .withColumnRenamed("manager_count_invoice_amount","manager_count").withColumnRenamed("manager_cum_condition_early_collection_days","manager_cum_delayed_days")

    val dataWithDroppedColumns = managerBasedFeatures.drop("balance_os","collection_incentive_on_amount_received",
      "disc_chrges_for_discouting_tenure","gross_collection","net_amount_received","usance_till_collection_days")

    val extendedDealerFeatures = dataWithDroppedColumns.calRatio("dealer_cum_invoice_amount","dealer_count").calRatio("dealer_cum_early_collection_days","dealer_count")
      .calRatio("dealer_cum_delayed_days","dealer_count").calRatio("dealer_cum_collection_incentive_on_amount_received","dealer_count")

    val data = dataWithDroppedColumns.drop("invoice_no","payer","product","rm_ase_asm")

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
/*    val resultValidatedLR = TrainValidationForLR(trainingData,testData)
    resultValidatedLR.show()
    println(resultValidatedLR.getRMSE)*/

    //------------ Using normalized data-----------------
    val normDataMinMax = dataForModel.returnNormData(2)
      val splitNormData = normDataMinMax.randomSplit(Array(0.9, 0.1), seed = 11L)
      val normTrainingData = splitNormData(0).cache()
      val normTestData = splitNormData(1)

      val resultNormValidatedLR = TrainValidationForLR(normTrainingData,normTestData)
  }

}
