package com.pg

import org.apache.spark.sql.{SaveMode, SQLContext, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


object NeighborhoodApp {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(s"NA app")
    setConfigurationProperties(conf)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val mySQL_user = args(0)
    val mySQL_passwd = args(1)
    run(sqlContext, mySQL_user, mySQL_passwd)
  }

  def setConfigurationProperties(conf: SparkConf): Unit = {
    conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
  }

  def loadStoreSales(sqlContext: HiveContext): DataFrame = {
    // load from Hive table
    sqlContext.sql("SELECT * FROM na.store_sales")
  }

  def loadTradeAreaSales(sqlContext: HiveContext, taSalesPath: String): DataFrame = {
    // load directly from HDFS
    import sqlContext.implicits._

    val TASalesRDD =
    sqlContext.sparkContext.textFile(taSalesPath).
      map(line => line.split("\t")).
      map(tokens => TASales(tokens(0), tokens(1), tokens(2), tokens(3).toDouble, tokens(5).toDouble))

    val TASalesDF = TASalesRDD.toDF()
    return TASalesDF
  }

  def loadTA2StoreKey(sqlContext: HiveContext, taKeyPath: String): DataFrame = {
    sqlContext.read.json(taKeyPath)
  }

  def distributeTASales(taSales: DataFrame, ta2StoreKey: DataFrame): DataFrame = {
    val taStoreCnt = ta2StoreKey.groupBy("trade_area_name").count()
    val ta2StoreKeyWithCnt = ta2StoreKey.join(taStoreCnt, Seq("trade_area_name"))

    val joined = taSales.join(ta2StoreKeyWithCnt, Seq("trade_area_name"))

    joined.
      withColumn("sales_usd_amt", joined("sales_usd_amt") / joined("count")).
      withColumn("sales_unit_qty", joined("sales_unit_qty") / joined("count")).
      select("month", "retailer_name", "store_num", "upc", "sales_usd_amt", "sales_unit_qty")

  }

  def combineStoreSales(storeSales: DataFrame, distTASales: DataFrame): DataFrame = {
    storeSales.
      unionAll(distTASales)

  }

  def loadStore2BGKey(sqlContext: HiveContext, user: String, password: String): DataFrame = {
    sqlContext.
      read.format("jdbc").
      option("url", "jdbc:mysql://localhost/sparksql").
      option("driver", "com.mysql.jdbc.Driver").
      option("dbtable", "ta_sales").
      option("user", user).
      option("password", password).
      load()
  }

  def distributeSalesToBlockGroups(storeSales: DataFrame, store2BGKey: DataFrame): DataFrame = {
    storeSales.
      join(store2BGKey, Seq("retailer_name", "store_num")).
      groupBy("month", "block_group_id", "upc").
      agg(sum(storeSales("sales_usd_amt") * store2BGKey("factor")), sum(storeSales("sales_unit_qty") * store2BGKey("factor")))
  }

  def distributeSalesToBlockGroupsWithAcc(storeSales: DataFrame, store2BGKey: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.broadcast

    val df = broadcast(store2BGKey)

    storeSales.
      join(store2BGKey, Seq("retailer_name", "store_num")).
      groupBy("month", "block_group_id", "upc").
      agg(sum(storeSales("sales_usd_amt") * store2BGKey("factor")), sum(storeSales("sales_unit_qty") * store2BGKey("factor")))
  }

  def storeInHive(sqlContext: HiveContext, bgSales: DataFrame): Unit = {
      bgSales.write.partitionBy("month").saveAsTable("na.block_group_sales")
  }

  def run(sqlContext: HiveContext, user: String, password: String) {

    // read and parse store_sales
    val storeSales = loadStoreSales(sqlContext)

    // read ta_sales
    val taSales = loadTradeAreaSales(sqlContext, "path...")

    // read ta_2_store_key
    val ta2StoreKey = loadTA2StoreKey(sqlContext, "path...")

    // distribute trade area sales evenly between all stores in a given Trade Area
    val distTASales = distributeTASales(taSales, ta2StoreKey)

    // combine sales from stores with sales from ta
    val allStoreSales = combineStoreSales(storeSales, distTASales)

    // read store_2_bg_key
    val store2BGKey = loadStore2BGKey(sqlContext, user, password)

    // distribute on block groups
    val bgSales = distributeSalesToBlockGroups(allStoreSales, store2BGKey)

    // save to hive
    storeInHive(sqlContext, bgSales)
  }

}

case class TASales(month: String, trade_area_name: String,
                   upc: String, sales_usd_amt: Double, sales_unit_qty: Double)