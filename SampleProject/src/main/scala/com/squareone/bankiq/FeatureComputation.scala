package com.squareone.bankiq

import java.sql.Date

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import java.util.Calendar

object FeatureComputation {
  val partitionOnPayer = Window.partitionBy("payer").orderBy("invoice_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
  val partitionOnPayerAndProduct = Window.partitionBy("payer","product").orderBy("invoice_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
  val partitionOnProduct = Window.partitionBy("product").orderBy("invoice_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
  val partitionOnManager = Window.partitionBy("rm_ase_asm").orderBy("invoice_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
  val partitionOnDueMonth = Window.partitionBy("month_due_date").orderBy("invoice_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
  val partitionOnCollectionMonth = Window.partitionBy("month_collection_date").orderBy("invoice_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)


  def getWindowfunction(value: Int) = value match {
    case 1 => partitionOnPayer
    case 2 => partitionOnPayerAndProduct
    case 3 => partitionOnProduct
    case 4 => partitionOnManager
    case 5 => partitionOnDueMonth
    case 6 => partitionOnCollectionMonth
    case _ => partitionOnPayer
  }
  def getPrefix(value: Int): String = value match {
    case 1 => "dealer_"
    case 2 => "dealer_product_"
    case 3 => "product_"
    case 4 => "manager_"
    case 5 => "due_date_month_"
    case 6 => "collection_date_month_"
    case _ => "dealer_"
  }
  implicit class compute(data: DataFrame) {
    def cumSum(indexColumn: String,cumColumn: String) = {
      data.select(expr(indexColumn),sum(cumColumn).over(Window.partitionBy(lit(0)).rowsBetween(Long.MinValue, 0)))
    }
    private def cumSumWithGroupBySingleColumn(cumColumn: String, dataInput: DataFrame = data,windowFunction: WindowSpec = partitionOnPayer,prefix: String): DataFrame = {
      dataInput.withColumn(prefix + "cum_"+ cumColumn,sum(cumColumn).over(windowFunction))
    }
    def cumSumWithGroupBy(column: String*)(value: Int = 1):DataFrame = {
      column.foldLeft(data){(memoDF,colName) => cumSumWithGroupBySingleColumn(colName,memoDF, getWindowfunction(value),getPrefix(value))}
    }
    private def countWithGroupBySingleColumn(countColumn: String, dataInput: DataFrame = data,windowFunction: WindowSpec = partitionOnPayer,prefix: String): DataFrame = {
      dataInput.withColumn(prefix +"count_"+ countColumn,count(countColumn).over(windowFunction))
    }
    def countWithGroupBy(column: String*)(value: Int = 1): DataFrame = {
      column.foldLeft(data){(memoDF,colName) => countWithGroupBySingleColumn(colName,memoDF,getWindowfunction(value),getPrefix(value))}
    }
    def cumWeightedAverage(weightColumn: String,valueColumn: String)(value: Int = 1): DataFrame = {
      val multipliedData = data.withColumn("multiplied_column",data(weightColumn)*data(valueColumn))
      cumSumWithGroupBySingleColumn("multiplied_column",multipliedData,getWindowfunction(value),getPrefix(value))
        .withColumnRenamed(getPrefix(value)+"cum_multipled_column",getPrefix(value)+"cum_weighted_"+weightColumn + "_" +valueColumn)
    }
    private def cumAverageWithGroupBySingleColumn(avgColumn: String, dataInput: DataFrame = data,windowFunction: WindowSpec = partitionOnPayer,prefix: String): DataFrame = {
      dataInput.withColumn(prefix + "avg_"+avgColumn,avg(avgColumn).over(windowFunction))
    }

    def cumAverageWithGroupBy(column: String*)(value: Int = 1):DataFrame = {
      column.foldLeft(data){(memoDF,colName) => cumAverageWithGroupBySingleColumn(colName,memoDF,getWindowfunction(value),getPrefix(value))}
    }
    def sumBasedOnCondition(cumColumn: String, condition: Double => Double,dataInput: DataFrame = data)(value: Int = 1): DataFrame = {
      val udfCondition = udf(condition)
      val conditionedData = dataInput.withColumn("condition_column",udfCondition(data(cumColumn)))
      cumSumWithGroupBySingleColumn("condition_column",conditionedData,getWindowfunction(value),getPrefix(value)).drop("condition_column")
        .withColumnRenamed(getPrefix(value) + "cum_condition_column",getPrefix(value)+"cum_condition_"+ cumColumn)
    }
    def cumRatio(numColumn: String,denColumn: String)(value: Int = 1): DataFrame = {
      val divide = udf{(num: Double, den: Double) =>
        if(den != 0.00) num/den else 0.00
      }
      val ratioedData = data.withColumn("ratio_column",divide(data(numColumn),data(denColumn)))
      cumSumWithGroupBySingleColumn("ratio_column",ratioedData,getWindowfunction(value),getPrefix(value)).drop("ratio_column")
        .withColumnRenamed(getPrefix(value)+"cum_ratio_column",getPrefix(value)+"cum_ratio_"+numColumn + "_" +denColumn)
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
    private def getMonthFormDateColumn(colName: String, dataInput: DataFrame = data) = {
      val cal = Calendar.getInstance
      val monthConverter = udf{(s: Date) => cal.setTime(s); cal.get(Calendar.MONTH)}
      dataInput.withColumn("month_"+colName,monthConverter(dataInput(colName)))
    }
    def getMonthFromDate(column: String*) = {
      column.foldLeft(data){(memoDB,colName) => getMonthFormDateColumn(colName,memoDB)}
    }
    private def getYearFormDateColumn(colName: String, dataInput: DataFrame = data) = {
      val cal = Calendar.getInstance
      val yearConverter = udf{(s: Date) => cal.setTime(s); cal.get(Calendar.YEAR)}
      dataInput.withColumn("year_"+colName,yearConverter(dataInput(colName)))
    }
    def getYearFromDate(column: String*) = {
      column.foldLeft(data){(memoDB,colName) => getYearFormDateColumn(colName,memoDB)}
    }
  }
}
