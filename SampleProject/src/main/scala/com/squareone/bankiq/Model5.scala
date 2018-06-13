package com.squareone.bankiq

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import com.squareone.bankiq.ModelEvaluator._
import com.squareone.bankiq.NormalizeFuctions._
import com.squareone.bankiq.FeatureComputation._
import com.squareone.bankiq.DataWrangling._
import org.slf4j.{Logger, LoggerFactory}

object Model5 {
  private val logger: Logger = LoggerFactory.getLogger("model5")
  def apply(preparedData: DataFrame) = {
    val monthAdded = preparedData.getMonthFromDate("due_date","invoice_date")
      .drop("invoice_date","discounting_date","collection_date","due_date").cache()

    val featuredData :DataFrame = monthAdded.countWithGroupBy("invoice_amount")()
      .cumSumWithGroupBy("invoice_amount","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")()

    val productBasedFeatures = featuredData.countWithGroupBy("invoice_amount")(3)
      .cumSumWithGroupBy("invoice_amount","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")(3)

    val managerBasedFeatures = productBasedFeatures.countWithGroupBy("invoice_amount")(4)
      .cumSumWithGroupBy("invoice_amount","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")(4)

    val monthBasedFeatures = managerBasedFeatures.countWithGroupBy("invoice_amount")(5).countWithGroupBy("invoice_amount")(6)
      .cumSumWithGroupBy("invoice_amount","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")(5)
      .cumSumWithGroupBy("invoice_amount","usance_till_collection_days","early_collection_days","collection_incentive_on_amount_received")(6)

    val dataWithDroppedColumns = monthBasedFeatures.drop("balance_os","collection_incentive_on_amount_received",
      "disc_chrges_for_discouting_tenure","gross_collection","net_amount_received","usance_till_collection_days")

    val dataCrude = dataWithDroppedColumns.drop("invoice_no","payer","product","rm_ase_asm")
      .calRatio("early_collection_days","discounting_tenure").withColumnRenamed("ratio_early_collection_days_discounting_tenure","label")
      .drop("early_collection_days")

    val data = dataCrude.limitDecimal(dataCrude.columns: _*)

    val assembler = new VectorAssembler()
      .setInputCols(data.columns.filter(x => x != "label"))
      .setOutputCol("features")

    val vectorizedData = assembler.transform(data)

    val dataForModel = vectorizedData.select("features","label")

    //RandomForrest
    val normDataMinMax = dataForModel.returnNormData(2)
    val splitNormData = normDataMinMax.randomSplit(Array(0.9, 0.1), seed = 11L)
    val normTrainingData = splitNormData(0).cache()
    val normTestData = splitNormData(1)

    val resultNormValidatedRFR = RandomForrestModel(normTrainingData,normTestData)

    logger.info(resultNormValidatedRFR.getRMSE.toString)
    logger.info(resultNormValidatedRFR.getR2.toString)
    logger.info(resultNormValidatedRFR.getMAE.toString)
  }
}
