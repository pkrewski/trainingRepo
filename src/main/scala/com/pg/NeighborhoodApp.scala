package com.pg

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Spark Application to calculate total sales for each upc for every Block Group
 */
object NeighborhoodApp {

  var ??? = null

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(s"NA app")
    setConfigurationProperties(conf)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val mySQL_user = args(0)
    val mySQL_passwd = args(1)
    val hiveDB = args(2)
    run(sqlContext, mySQL_user, mySQL_passwd, hiveDB)
  }

  def setConfigurationProperties(conf: SparkConf): Unit = {
    conf.set("spark.sql.autoBroadcastJoinThreshold", "???")

    // configure Kryo serialization
    ???
  }

  def loadStoreSales(sqlContext: HiveContext, hiveDB: String): DataFrame = {
    // load from Hive table
    ???
  }

  def loadTradeAreaSales(sqlContext: HiveContext, taSalesPath: String): DataFrame = {
    // load directly from HDFS
    import sqlContext.implicits._

    val TASalesRDD: RDD[TASales] = ???

    TASalesRDD.toDF()
  }

  def loadTA2StoreKey(sqlContext: HiveContext, taKeyPath: String): DataFrame = {
    // read json file from HDFS
    ???
  }

  def distributeTASales(taSales: DataFrame, ta2StoreKey: DataFrame): DataFrame = {
    // returned DataFrame should have following schema:
    // month, retailer_name, store_num, upc, sales_usd_amt, sales_unit_qty
    ???

  }

  def combineStoreSales(storeSales: DataFrame, distTASales: DataFrame): DataFrame = {
    // union storeSales with distTASales
    ???
  }

  def loadStore2BGKey(sqlContext: HiveContext, user: String, password: String): DataFrame = {
    sqlContext.
      read.format("jdbc").
      option("url", "jdbc:mysql://ip-10-22-36-23.us-west-2.compute.internal/neighborhood").
      option("driver", "com.mysql.jdbc.Driver").
      option("dbtable", "store_to_block_group_key").
      option("user", user).
      option("password", password).
      load()
  }

  def distributeSalesToBlockGroups(storeSales: DataFrame, store2BGKey: DataFrame): DataFrame = {
    // join storeSales with store2BGKey mapping and apportion appropriate level of sales from each store to block group
    // that overlaps with the stores trade area. Remember to scale store sales by the factor by which trade area of
    // each store overlaps with a given block group area (factor field).
    ???
  }

  def storeInHive(sqlContext: HiveContext, bgSales: DataFrame, hiveDB: String): Unit = {
    // we want to leverage dynamic partition insert
    // following configuration parameters have to be enabled to make it possible
      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    // register block group sales dataframe as temporary table
      bgSales.registerTempTable(???)

    // perform dynamic partition insert into block_group_sales table
      sqlContext.sql(
        s"""
         ???
        """.stripMargin)
  }

  def run(sqlContext: HiveContext, user: String, password: String, hiveDB: String) {

    // read and parse store_sales
    val storeSales = loadStoreSales(sqlContext, hiveDB)

    // read ta_sales
    val taSales = loadTradeAreaSales(sqlContext, "/incoming/upload/ta_sales")

    // read ta_2_store_key
    val ta2StoreKey = loadTA2StoreKey(sqlContext, "/incoming/upload/trade_area_to_store_key")

    // distribute trade area sales evenly between all stores in a given Trade Area
    val distTASales = distributeTASales(taSales, ta2StoreKey)

    // combine sales from stores with sales from ta
    val allStoreSales = combineStoreSales(storeSales, distTASales)

    // read store_2_bg_key
    val store2BGKey = loadStore2BGKey(sqlContext, user, password)

    // distribute on block groups
    val bgSales = distributeSalesToBlockGroups(allStoreSales, store2BGKey)

    // save to hive
    storeInHive(sqlContext, bgSales, hiveDB)
  }

}

// class to store Trade Area sales
case class TASales(month: String, trade_area_name: String,
                   upc: String, sales_usd_amt: Double, sales_unit_qty: Double)