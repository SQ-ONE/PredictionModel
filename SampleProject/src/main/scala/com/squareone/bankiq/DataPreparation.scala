package com.squareone.bankiq

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataPreparation {

  val spark = SparkService.getSparkSession()
  import spark.implicits._

  implicit class Prepare(data: DataFrame) {

    private def removeEntityForColumn[T](data: DataFrame, colName: String, entity: String, replacement: T): DataFrame = {
      data.withColumn(colName, when(col(colName).contains(entity), replacement).otherwise(col(colName)))
    }

    private def replaceEntity[T](data: DataFrame, entity: String, replacement: T, column: Seq[String]): DataFrame = {
      column.foldLeft(data){(memoDF: DataFrame,colName: String) => removeEntityForColumn[T](memoDF,colName,entity,replacement)}
    }

    def dropColumns(column: String*): DataFrame = {
      column.foldLeft(data) { (memoDF, colName) => memoDF.drop(colName) }
    }

    private def convertColumnToLowerCase(data: DataFrame,colName: String) = {
      data.withColumn(colName, lower(col(colName)))
    }

    def convertLowerCase(column: String*): DataFrame = {
      column.foldLeft(data){(memoDB,colName) => convertColumnToLowerCase(memoDB,colName)}
    }

    def convertRegionToNumeric(column: String*): DataFrame = {
      def forOneColumn(data: DataFrame,colName: String) = {
        val convertRegion = udf {(make: String) =>
          if(make == "south") 1.00
          else if(make == "north") 2.00
          else if(make == "west") 3.00
          else 4.00
        }
        data.withColumn(colName,convertRegion(data(colName)))
      }
      column.foldLeft(data){(memoDB,colName) => forOneColumn(memoDB,colName)}
    }

    def deleteRowsWithNull(column: String*): DataFrame = {
      data.na.drop(column)
    }

    def convertCatergoryToFrequency(colName: String): DataFrame = {
      val categoryMap = data.groupBy(colName).count().collect
        .foldLeft(Map.empty: Map[String,Double]){(memoMap,row) => memoMap + (row(0).toString -> row(1).toString.toDouble)}
      val convertCategory = udf {(make: String) =>
        categoryMap.getOrElse[Double](make,0.00)
      }
      data.withColumn(colName,convertCategory(data(colName)))
    }
  }
}
