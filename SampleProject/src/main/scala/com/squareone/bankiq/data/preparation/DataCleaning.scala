package com.squareone.bankiq.data.preparation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object DataCleaning {
  val spark = SparkSession.builder().getOrCreate()

  implicit class Clean(data: DataFrame) {

    def removeHyphen(column: String*): DataFrame = {
      replaceEntity[Double](data, "-", 0.00, column)
    }

    private def replaceEntity[T](data: DataFrame, entity: String, replacement: T, column: Seq[String]) = {
      column.foldLeft(data) { (memoDF: DataFrame, colName: String) => removeEntityForColumn[T](memoDF, colName, entity, replacement) }
    }

    private def removeEntityForColumn[T](data: DataFrame, colName: String, entity: String, replacement: T) = {
      data.withColumn(colName, when(col(colName).contains(entity), replacement).otherwise(col(colName)))
    }

    def removePercent(column: String*): DataFrame = {
      regexSwap(data, "\\%", "", column)
    }

    def removeComma(column: String*): DataFrame = {
      regexSwap(data, "\\,", "", column)
    }

    def removeParenthesis(column: String*): DataFrame = {
      def replaceParenthesis(colName: String): Column = {
        regexp_replace(regexp_replace(col(colName), "\\(", "-"), "\\)", "")
      }

      column.foldLeft(data) { (memoDF, colName) => memoDF.withColumn(colName, replaceParenthesis(colName)) }
    }

    def removeAllSpaces = {
      removeSpaces(data.columns)
    }

    def removeSpaces(column: Seq[String]): DataFrame = {
      regexSwap(data, "\\s+", "", column)
    }

    private def regexSwap(data: DataFrame, entity: String, replacement: String, column: Seq[String]) = {
      column.foldLeft(data) { (memoDF: DataFrame, colName: String) => regexSwapFromColumn(memoDF, colName, entity, replacement) }
    }

    private def regexSwapFromColumn(data: DataFrame, colName: String, entity: String, replacement: String) = {
      data.withColumn(colName, regexp_replace(col(colName), entity, replacement))
    }

    def removeSpaceFromHeader = {
      def refineName(name: String): String = {
        name.trim.replace(" ", "_")
      }

      data.columns.foldLeft(data) { (memoDF, colName) => memoDF.withColumnRenamed(colName, refineName(colName)) }
    }
  }

}