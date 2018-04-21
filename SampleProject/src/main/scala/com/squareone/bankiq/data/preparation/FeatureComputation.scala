package com.squareone.bankiq.data.preparation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object FeatureComputation {
  implicit class compute(data: DataFrame) {
    def cumSum(indexColumn: String,cumColumn: String) = {
      data.select(expr(indexColumn),sum(cumColumn).over(Window.partitionBy(lit(0)).rowsBetween(Long.MinValue, 0)))
    }
    private def cumSumWithGroupBySingleColumn(index: String,groupByColumn: String, cumColumn: String, dataInput: DataFrame = data): DataFrame = {
      val cumData = dataInput.select(expr(index),sum(cumColumn).over(Window.partitionBy(groupByColumn).rowsBetween(Long.MinValue,0)).alias("cum"+ cumColumn))
      dataInput.join(cumData,index)
    }
    def cumSumWithGroupBy(index: String,groupByColumn: String,column: String*):DataFrame = {
      column.foldLeft(data){(memoDF,colName) => cumSumWithGroupBySingleColumn(index,groupByColumn,colName,memoDF)}
    }

    private def countWithGroupBySingleColumn(index: String,groupByColumn: String, countColumn: String, dataInput: DataFrame = data): DataFrame = {
      val countData = dataInput.select(expr(index),count(countColumn).over(Window.partitionBy(groupByColumn).rowsBetween(Long.MinValue,0)).alias("count"+ countColumn))
      dataInput.join(countData,index)
    }
    def countWithGroupBy(index: String,groupByColumn: String,column: String*): DataFrame = {
      column.foldLeft(data){(memoDF,colName) => countWithGroupBySingleColumn(index,groupByColumn,colName,memoDF)}
    }
    def cumWeightedAverage(index: String,groupByColumn: String,weightColumn: String,valueColumn: String): DataFrame = {
      val multipliedData = data.select(data(index),data(groupByColumn),(data(weightColumn)*data(valueColumn)).alias("MultipliedColumn"))
      val countData = multipliedData.select(expr(index),sum("MultipliedColumn").over(Window.partitionBy(groupByColumn)
        .rowsBetween(Long.MinValue,0)).alias("weighted"+ valueColumn + "Against" + weightColumn))
      data.join(countData,index)
    }
    private def cumAverageWithGroupBySingleColumn(index: String,groupByColumn: String, avgColumn: String, dataInput: DataFrame = data): DataFrame = {
      val avgData = dataInput.select(expr(index),avg(avgColumn).over(Window.partitionBy(groupByColumn).rowsBetween(Long.MinValue,0)).alias("avg"+ avgColumn))
      dataInput.join(avgData,index)
    }
    def cumAverageWithGroupBy(index: String,groupByColumn: String,column: String*):DataFrame = {
      column.foldLeft(data){(memoDF,colName) => cumAverageWithGroupBySingleColumn(index,groupByColumn,colName,memoDF)}
    }
    def sumBasedOnCondition(index: String,groupByColumn: String, targetColumn: String, condition: Double => Double,dataInput: DataFrame = data): DataFrame = {
      val udfCondition = udf(condition)
      val conditionedData = dataInput.select(data(index),data(groupByColumn),udfCondition(data(targetColumn)).alias("ConditionedColumn"))
      val conData = conditionedData.select(expr(index),sum("ConditionedColumn").over(Window.partitionBy(groupByColumn).rowsBetween(Long.MinValue,0)).alias("conditional"+ targetColumn))
      dataInput.join(conData,index)
    }
    def cumRatio(index: String,numColumn: String,denColumn: String): DataFrame = {
      val ratioData = data.select(data(index),(data(numColumn)/data(denColumn)).alias("ratio"+numColumn+denColumn))
      data.join(ratioData,index)
    }
  }
}
