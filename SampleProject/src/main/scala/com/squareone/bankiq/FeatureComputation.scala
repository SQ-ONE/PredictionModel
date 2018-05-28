package com.squareone.bankiq

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

object FeatureComputation {
  val partitionOnPayer = Window.partitionBy("payer").orderBy("invoice_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
  val partitionOnPayerAndProduct = Window.partitionBy("payer","product").orderBy("invoice_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)

  def getWindowfunction(value: Int) = value match {
    case 1 => partitionOnPayer
    case 2 => partitionOnPayerAndProduct
    case _ => partitionOnPayer
  }

  implicit class compute(data: DataFrame) {
    def cumSum(indexColumn: String,cumColumn: String) = {
      data.select(expr(indexColumn),sum(cumColumn).over(Window.partitionBy(lit(0)).rowsBetween(Long.MinValue, 0)))
    }
    private def cumSumWithGroupBySingleColumn(cumColumn: String, dataInput: DataFrame = data,windowFunction: WindowSpec = partitionOnPayer): DataFrame = {
      dataInput.withColumn("cum_"+ cumColumn,sum(cumColumn).over(windowFunction))
    }
    def cumSumWithGroupBy(column: String*)(windowFunction: WindowSpec = partitionOnPayer):DataFrame = {
      column.foldLeft(data){(memoDF,colName) => cumSumWithGroupBySingleColumn(colName,memoDF,windowFunction)}
    }

    private def countWithGroupBySingleColumn(countColumn: String, dataInput: DataFrame = data,windowFunction: WindowSpec = partitionOnPayer): DataFrame = {
      dataInput.withColumn("count_"+ countColumn,count(countColumn).over(windowFunction))
    }
    def countWithGroupBy(column: String*)(windowFunction: WindowSpec = partitionOnPayer): DataFrame = {
      column.foldLeft(data){(memoDF,colName) => countWithGroupBySingleColumn(colName,memoDF,windowFunction)}
    }
    def cumWeightedAverage(weightColumn: String,valueColumn: String)(windowFunction: WindowSpec = partitionOnPayer): DataFrame = {
      val multipliedData = data.withColumn("multiplied_column",data(weightColumn)*data(valueColumn))
      cumSumWithGroupBySingleColumn("multiplied_column",multipliedData)
        .withColumnRenamed("cum_multipled_column","cum_weighted_"+weightColumn + "_" +valueColumn)
    }

    private def cumAverageWithGroupBySingleColumn(avgColumn: String, dataInput: DataFrame = data,windowFunction: WindowSpec = partitionOnPayer): DataFrame = {
      dataInput.withColumn("avg_"+avgColumn,avg(avgColumn).over(windowFunction))
    }

    def cumAverageWithGroupBy(column: String*)(windowFunction: WindowSpec = partitionOnPayer):DataFrame = {
      column.foldLeft(data){(memoDF,colName) => cumAverageWithGroupBySingleColumn(colName,memoDF,windowFunction)}
    }
    def sumBasedOnCondition(cumColumn: String, condition: Double => Double,dataInput: DataFrame = data)(windowFunction: WindowSpec = partitionOnPayer): DataFrame = {
      val udfCondition = udf(condition)
      val conditionedData = dataInput.withColumn("condition_column",udfCondition(data(cumColumn)))
      cumSumWithGroupBySingleColumn("condition_column",conditionedData,windowFunction).drop("condition_column")
        .withColumnRenamed("cum_condition_column","cum_condition_"+ cumColumn)
    }

    def cumRatio(numColumn: String,denColumn: String)(windowFunction: WindowSpec = partitionOnPayer): DataFrame = {
      val divide = udf{(num: Double, den: Double) =>
        if(den != 0.00) num/den else 0.00
      }
      val ratioedData = data.withColumn("ratio_column",divide(data(numColumn),data(denColumn)))
      cumSumWithGroupBySingleColumn("ratio_column",ratioedData).drop("ratio_column")
        .withColumnRenamed("cum_ratio_column","cum_ratio_"+numColumn + "_" +denColumn)
    }

    def calRatio(numColumn: String,denColumn: String) = {
      val divide = udf{(num: Double, den: Double) =>
        if(den != 0.00) num/den else 0.00
      }
      data.withColumn("ratio_" + numColumn + "_" + denColumn,divide(data(numColumn),data(denColumn)))
    }
    def calCondition(colName: String,condition: Double => Double) = {
      val conditionFunction = udf(condition)
      data.withColumn("condition_"+colName,conditionFunction(data(colName)))
    }
  }
}
