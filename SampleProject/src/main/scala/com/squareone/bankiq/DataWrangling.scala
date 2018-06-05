package com.squareone.bankiq

import java.sql.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat

object DataWrangling {
  implicit class wrangle(data: DataFrame) extends  Serializable{
    private def parseAsDouble(data: DataFrame,colName: String,fill: Double): DataFrame = {
      val parseDouble = udf {(s: String) =>
        try { Some(s.toDouble).get } catch { case e: Exception => fill }
      }
      data.withColumn(colName,parseDouble(data(colName)))
    }
    private def parseAsDate(data: DataFrame,colName: String,pattern: String): DataFrame = {
      val convertDate = udf { (s: String) =>
        convertFormat(s, pattern)
      }
      data.withColumn(colName,convertDate(data(colName))).withColumn(colName,col(colName).cast("date"))
    }
    def convertFormat(date: String,pattern: String) = {
      val formatter = new SimpleDateFormat("yyyy-MM-dd")
      val parser = new SimpleDateFormat((pattern))
      try{formatter.format(parser.parse(date))} catch {case e: Exception => "2017-01-01"}
    }
    def parseColumnAsDouble(fill: Double,column: String*): DataFrame={
      column.foldLeft(data){(memoDB,colName) => parseAsDouble(memoDB,colName,fill)}
    }
    def parseColumnAsDate(column: String*)(pattern: String = "yyyy-MM-dd"): DataFrame={
      column.foldLeft(data){(memoDB,colName) => parseAsDate(memoDB,colName,pattern)}
    }
    def limitDecimal(column: String*): DataFrame = {
      column.foldLeft(data){(memoDB,colName) => memoDB.withColumn(colName,bround(memoDB(colName),2))}
    }
    def parseAllColumnsAsDouble(fill: Double): DataFrame = {
      parseColumnAsDouble(fill,data.columns: _*)
    }
  }
}
