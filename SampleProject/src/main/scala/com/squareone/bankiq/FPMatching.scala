package com.squareone.bankiq

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FPMatching {
  def apply(data: DataFrame){
    val dataRenamed = data.toDF("X","Y")
    def convertToArray = udf{(x: Double,y: Double) =>
      Array(x,y)}

    val dataForFP = dataRenamed.withColumn("items",convertToArray(dataRenamed("X"),dataRenamed("Y"))).select("items")
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.1).setMinConfidence(0.4)

    val model = fpgrowth.fit(dataForFP)

    model.freqItemsets.show()
    model.associationRules.show()
    model.transform(dataForFP).show()
  }
}
