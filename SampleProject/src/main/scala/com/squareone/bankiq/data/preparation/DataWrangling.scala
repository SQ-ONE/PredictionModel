package com.squareone.bankiq.data.preparation

import java.sql.Date

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataWrangling {
  implicit class wrangle(data: DataFrame) {
    private def parseAsDouble(data: DataFrame,colName: String,fill: Double): DataFrame = {
      val parseDouble = udf {(s: String) =>
        try { Some(s.toDouble).get } catch { case e: Exception => fill }
      }
      data.withColumn(colName,parseDouble(data(colName)))
    }
    def parseColumnAsDouble(fill: Double,column: String*): DataFrame={
      column.foldLeft(data){(memoDB,colName) => parseAsDouble(memoDB,colName,fill)}
    }
    def parseAllColumnsAsDouble(fill: Double): DataFrame = {
      parseColumnAsDouble(fill,data.columns: _*)
    }

    private def returnDate(date: String, pattern: String): Date = {
      val format = new java.text.SimpleDateFormat(pattern)
      new java.sql.Date(format.parse(date).getTime)
    }

    private def parseAsDate(data: DataFrame,colName: String,pattern: String) = {
      val parseDate = udf {(s: String) =>
        try { Some(returnDate(s,pattern)) } catch { case e: Exception => null }
      }
      data.withColumn(colName,parseDate(data(colName)))
    }

    def parseColumnAsDate(pattern: String,column: String*): DataFrame = {
      column.foldLeft(data){(memoDB,colName) => parseAsDate(memoDB,colName,pattern)}
    }
  }
}
