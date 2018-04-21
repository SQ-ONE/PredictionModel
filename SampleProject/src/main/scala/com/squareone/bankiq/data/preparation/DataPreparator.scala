package com.squareone.bankiq.data.preparation

import com.squareone.bankiq.data.preparation.DataCleaning._
import com.squareone.bankiq.data.preparation.DataPreparation._
import com.squareone.bankiq.data.preparation.DataWrangling._
import com.squareone.bankiq.data.preparation.FeatureComputation._
import com.squareone.bankiq.data.source.TrainingData
import org.apache.predictionio.controller.PPreparator
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

case class PreparedData(df: DataFrame)

class DataPreparator extends PPreparator[TrainingData, PreparedData] {
  override def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {

    val cleanData: DataFrame = trainingData.df.removeSpaceFromHeader
      .removeAllSpaces
      .removeHyphen("Balance_O/s", "Collection_Incentive_on_Amount_Received", "Net_Amount_Received")
      .removePercent("Rate")
      .removeParenthesis("Gross_Collection", "Usance_till_Collection_Days", "Disc_Chrges_for_Discouting_Tenure",
        "Early_Collection_Days", "Collection_Incentive_on_Amount_Received", "Net_Amount_Received")
      .removeComma("Invoice_Amount", "Gross_Collection", "Usance_till_Collection_Days", "Disc_Chrges_for_Discouting_Tenure",
        "Early_Collection_Days", "Collection_Incentive_on_Amount_Received", "Net_Amount_Received")

    val preparedData: DataFrame = cleanData.drop("Sr_No", "Dealer_Name", "Rec")
      .deleteRowsWithNull("Collection_Date")
      .convertLowerCase("Region")
      .convertRegionToNumeric("Region")
      .convertCatergoryToFrequency("Product")
      .convertLowerCase("RM/ASE/ASM")
      .convertCatergoryToFrequency("RM/ASE/ASM")

    val wrangledData: DataFrame = preparedData.parseColumnAsDouble(0.00, "RM/ASE/ASM", "Product", "Invoice_Amount", "Region", "Gross_Collection",
      "Balance_O/s", "Usance_till_Collection_Days", "Discounting_Tenure", "Rate", "Disc_Chrges_for_Discouting_Tenure", "Early_Collection_Days",
      "Collection_Incentive_on_Amount_Received", "Net_Amount_Received")


    val dataWithoutDate: DataFrame = wrangledData.drop("Invoice_Date", "Discounting_Date", "Collection_Date", "Due_Date")

    val featuredData: DataFrame = dataWithoutDate.cumSumWithGroupBy("Invoice_No", "Payer", "Invoice_Amount", "Early_Collection_Days", "Net_Amount_Received",
      "Collection_Incentive_on_Amount_Received")
      .countWithGroupBy("Invoice_No", "Payer", "Invoice_Amount")
      .cumWeightedAverage("Invoice_No", "Payer", "Early_Collection_Days", "Invoice_Amount")


    val data = featuredData.drop("Invoice_No", "Payer")

    val assembler = new VectorAssembler()
      .setInputCols(Array(data.columns.filter(x => x != "Early_Collection_Days"): _*))
      .setOutputCol("features")

    val vectorizedData = assembler.transform(data)

    val dataForModel = vectorizedData.withColumnRenamed("Early_Collection_Days", "label").select("features", "label")

    new PreparedData(dataForModel)
  }
}
