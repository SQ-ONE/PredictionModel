package com.squareone.bankiq

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler
import com.squareone.bankiq.ModelEvaluator._
import com.squareone.bankiq.NormalizeFuctions._
import com.squareone.bankiq.FeatureComputation._
import com.squareone.bankiq.DataWrangling._

object Model3 {
  def apply(wrangledData: DataFrame) = {
    val dataWithoutDate: DataFrame = wrangledData.drop("invoice_date","discounting_date","collection_date","due_date").cache()

    //------Feature Engineering-----------
    val featuredData :DataFrame = dataWithoutDate.countWithGroupBy("invoice_amount")()
      .cumSumWithGroupBy("invoice_amount","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")()
      //.cumRatio("early_collection_days","discounting_tenure")()
      //.sumBasedOnCondition("early_collection_days",x => if( x > 0.00)x else 0.00)()
      //.withColumnRenamed("dealer_count_invoice_amount","dealer_count").withColumnRenamed("dealer_cum_condition_early_collection_days","dealer_cum_delayed_days")

    featuredData.show(5)

    val productBasedFeatures = featuredData.countWithGroupBy("invoice_amount")(3)
      .cumSumWithGroupBy("invoice_amount","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")(3)
      //.cumRatio("early_collection_days","discounting_tenure")(3)
      //.sumBasedOnCondition("early_collection_days",x => if( x > 0.00)x else 0.00)(3)
      //.withColumnRenamed("product_count_invoice_amount","product_count").withColumnRenamed("product_cum_condition_early_collection_days","product_cum_delayed_days")

    productBasedFeatures.show(5)

    val managerBasedFeatures = productBasedFeatures.countWithGroupBy("invoice_amount")(4)
      .cumSumWithGroupBy("invoice_amount","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")(4)
      //.cumRatio("early_collection_days","discounting_tenure")(4)
      //.sumBasedOnCondition("early_collection_days",x => if( x > 0.00)x else 0.00)(4)
      //.withColumnRenamed("manager_count_invoice_amount","manager_count").withColumnRenamed("manager_cum_condition_early_collection_days","manager_cum_delayed_days")

    val dataWithDroppedColumns = managerBasedFeatures.drop("balance_os","collection_incentive_on_amount_received",
      "disc_chrges_for_discouting_tenure","gross_collection","net_amount_received","usance_till_collection_days")

  /*  val extendedDealerFeatures = dataWithDroppedColumns.calRatio("dealer_cum_invoice_amount","dealer_count")
      .calRatio("dealer_cum_early_collection_days","dealer_count").calRatio("dealer_cum_delayed_days","dealer_count")
      .calRatio("dealer_cum_collection_incentive_on_amount_received","dealer_count").calRatio("product_cum_invoice_amount","product_count")
      .calRatio("product_cum_early_collection_days","product_count").calRatio("product_cum_delayed_days","product_count")
      .calRatio("product_cum_collection_incentive_on_amount_received","product_count").calRatio("manager_cum_invoice_amount","manager_count")
      .calRatio("manager_cum_early_collection_days","manager_count").calRatio("manager_cum_delayed_days","manager_count")
      .calRatio("manager_cum_collection_incentive_on_amount_received","manager_count")*/

    val dataCrude = dataWithDroppedColumns.drop("invoice_no","payer","product","rm_ase_asm")

    val data = dataCrude.limitDecimal(dataCrude.columns: _*)

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
    val normDataMinMax = dataForModel.returnNormData(2)
    val splitNormData = normDataMinMax.randomSplit(Array(0.9, 0.1), seed = 11L)
    val normTrainingData = splitNormData(0).cache()
    val normTestData = splitNormData(1)

    val resultNormValidatedRFR = RandomForrestModel(normTrainingData,normTestData)
    resultNormValidatedRFR.show()
    println(resultNormValidatedRFR.getRMSE)
    println(resultNormValidatedRFR.getR2)
    println(resultNormValidatedRFR.getMAE)
  }
}
