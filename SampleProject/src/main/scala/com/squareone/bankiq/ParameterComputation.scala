package com.squareone.bankiq

import org.apache.spark.sql.Dataset

object ParameterComputation{
  implicit class compute(data: Dataset[MIS]) {
    def calAverage(groupByColumn: String, column: String) = {
      data.groupBy(groupByColumn).avg(column)
    }
  }
}
