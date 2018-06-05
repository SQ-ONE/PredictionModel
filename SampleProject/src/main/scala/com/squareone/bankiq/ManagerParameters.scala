package com.squareone.bankiq

import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import com.squareone.bankiq.FeatureComputation._
import com.squareone.bankiq.utility._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.cassandra._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import com.squareone.bankiq.DataWrangling._

import scala.util.Success

object ManagerParameters {
  private val config = ConfigFactory.load( "application.conf" )
  val spark = SparkService.getSparkSession()
  val sc = spark.sparkContext
  import spark.implicits._

  val manager = config.getConfig("dbTables").getString("db.cassandra.manager")
  val keyspace = config.getConfig("dbTables").getString("db.cassandra.keySpace")
  val renameColumns: Seq[String] = Seq("rm_ase_asm","manager_count","manager_cum_invoice_amount","manager_cum_usance_till_collection_days"
    ,"manager_cum_early_collection_days","manager_cum_collection_incentive_on_amount_received",
    "manager_cum_ratio_early_collection_days_discounting_tenure","manager_cum_delayed_days")

  def getManagerParameters: Dataset[Manager] = {
    try(spark.read.cassandraFormat(manager, keyspace).load().as[Manager]) catch { case e: Exception => spark.createDataset(sc.emptyRDD[Manager])}
  }

  def currentManagerParameters(data: Dataset[MIS]) = {
    val fileParameters = data.toDF().calRatio("early_collection_days","discounting_tenure")
      .calCondition("early_collection_days",x => if(x>0)x else 0).groupBy("rm_ase_asm").agg(count("invoice_no")
      ,sum("invoice_amount"),sum("usance_till_collection_days"),sum("early_collection_days"),sum("collection_incentive_on_amount_received")
      ,sum("ratio_early_collection_days_discounting_tenure"),sum("condition_early_collection_days"))
    fileParameters.limitDecimal(fileParameters.columns.filter(_ != "rm_ase_asm"): _*).toDF(renameColumns: _*).as[Manager]
  }

  def updateManagerParameters(data: Dataset[MIS]) = {
    val existingParams = getManagerParameters
    val currentFileParams = currentManagerParameters(data)
    val newParams = existingParams.union(currentFileParams).groupBy("rm_ase_asm").agg(sum("count"),sum("sumInvoiceAmount"),sum("sumUsance"),sum("sumEarlyColectionDays"),
      sum("sumCollectionIncentive"),sum("sumRatioEarlyCollectionDayVsPeriod")).toDF(renameColumns: _*).as[Manager]
    newParams.write.cassandraFormat(manager,keyspace)
  }

  def addFeaturestoInvoice(data: Dataset[Invoice]) = {
    val existingParams = getManagerParameters
    data.join(existingParams,"manager")
  }
}
