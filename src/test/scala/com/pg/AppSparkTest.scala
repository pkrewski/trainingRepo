package com.pg

import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.types.{StructType, DoubleType, StringType, StructField}
import org.scalatest._
import Matchers._
import org.apache.spark.sql.functions._

class NATest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers {

  var sqlContext: TestHiveContext = _

  override def beforeAll() {
    sqlContext = SparkContextFactory.getSqlContext
    sqlContext.sql( """CREATE DATABASE IF NOT EXISTS na""")
    sqlContext.sql( """DROP TABLE IF EXISTS na.store_sales""")
    sqlContext.sql( """DROP TABLE IF EXISTS na.st2bg""")

    sqlContext.sql( """
        CREATE TABLE IF NOT EXISTS na.store_sales (
                      month string,
                      retailer_name string,
                      store_num string,
                      upc string,
                      sales_usd_amt double,
                      sales_unit_qty double
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
                    """)

    sqlContext.sql( """
        CREATE TABLE IF NOT EXISTS na.st2bg (
                      retailer_name string,
                      store_num string,
                      block_group_id string,
                      factor double
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE
                    """)

    sqlContext.sql( """LOAD DATA LOCAL INPATH 'src/test/resources/store_sales.txt' INTO TABLE na.store_sales""")
    sqlContext.sql( """LOAD DATA LOCAL INPATH 'src/test/resources/store_2_bg_key.txt' INTO TABLE na.st2bg""")

  }


  test("Load store sales") {
    val storeSales = NeighborhoodApp.loadStoreSales(sqlContext)

    storeSales.collect() should have size 10
    storeSales.select("retailer_name").distinct().collect().map(_.getString(0)) should contain allOf("Retailer1", "Retailer2")
    storeSales.agg(sum("sales_usd_amt")).collect()(0).getDouble(0) should equal(19.0)
  }

  test("Load Trade Area sales") {
    val taSales = NeighborhoodApp.loadTradeAreaSales(sqlContext, "src/test/resources/ta_sales.txt")

    taSales.collect() should have size 4
    taSales.select("upc").distinct().collect().map(_.getString(0)) should contain allOf("01", "02", "03")
    taSales.agg(sum("sales_usd_amt")).collect()(0).getDouble(0) should equal(7.0)
  }

  test("Load Trade Area to store key") {
    val ta2storeKey = NeighborhoodApp.loadTA2StoreKey(sqlContext, "src/test/resources/ta_2_store_key.txt")

    ta2storeKey.collect() should have size 4
    ta2storeKey.select("trade_area_name").distinct().collect().map(_.getString(0)) should contain allOf("TA1", "TA2")
  }

  test("Distribute Trade Area sales") {
    val taSales = NeighborhoodApp.loadTradeAreaSales(sqlContext, "src/test/resources/ta_sales.txt")
    val ta2storeKey = NeighborhoodApp.loadTA2StoreKey(sqlContext, "src/test/resources/ta_2_store_key.txt")
    val distTASales = NeighborhoodApp.distributeTASales(taSales, ta2storeKey)

    distTASales.collect() should have size 8
    distTASales.select("retailer_name").filter(distTASales("retailer_name") === "Retailer3").count() should equal(4)
  }

  test("Combine all store sales") {
    val taSales = NeighborhoodApp.loadTradeAreaSales(sqlContext, "src/test/resources/ta_sales.txt")
    val ta2storeKey = NeighborhoodApp.loadTA2StoreKey(sqlContext, "src/test/resources/ta_2_store_key.txt")
    val distTASales = NeighborhoodApp.distributeTASales(taSales, ta2storeKey)
    val storeSales = NeighborhoodApp.loadStoreSales(sqlContext)
    val allStoreSales = NeighborhoodApp.combineStoreSales(storeSales, distTASales)

    allStoreSales.collect() should have size 18
    allStoreSales.filter(allStoreSales("month") === "201601").agg(sum("sales_unit_qty")).collect()(0).getDouble(0) should equal(19.0)
  }

  def loadStore2BGKey(sqlContext: TestHiveContext): DataFrame = {
    sqlContext.sql("SELECT * FROM na.st2bg")
  }

  test("Distribute sales to block groups") {
    val taSales = NeighborhoodApp.loadTradeAreaSales(sqlContext, "src/test/resources/ta_sales.txt")
    val ta2storeKey = NeighborhoodApp.loadTA2StoreKey(sqlContext, "src/test/resources/ta_2_store_key.txt")
    val distTASales = NeighborhoodApp.distributeTASales(taSales, ta2storeKey)
    val storeSales = NeighborhoodApp.loadStoreSales(sqlContext)
    val allStoreSales = NeighborhoodApp.combineStoreSales(storeSales, distTASales)
    val store2BG = loadStore2BGKey(sqlContext)

    val bgUpcSales = NeighborhoodApp.distributeSalesToBlockGroups(allStoreSales, store2BG)

    allStoreSales.collect().foreach(println)
    allStoreSales.filter(allStoreSales("month") === "201601").count() should equal(13)
    val oneMonth = allStoreSales.filter(allStoreSales("month") === "201601")
    oneMonth.agg(sum("sales_usd_amt"), sum("sales_unit_qty")).collect().map(r => (r.getDouble(0), r.getDouble(1))).toList(0) should equal(17.0, 19.0)
  }

  def createTestBGSales(): DataFrame = {
    object schema {
      val bgId = StructField("block_group_id", StringType)
      val upc = StructField("upc", StringType)
      val salesUsdAmt = StructField("sales_usd_amt", DoubleType)
      val salesUnitQty = StructField("sales_unit_qty", DoubleType)
      val month = StructField("month", StringType)

      val struct = StructType(Array(bgId, upc, salesUsdAmt, salesUnitQty, month))
    }

    val bgSalesRDD = sqlContext.sparkContext.makeRDD(
      Array(
        Array("1", "01", "5.0", "3.0", "201601"),
        Array("2", "01", "3.0", "1.0", "201601"),
        Array("2", "02", "1.0", "2.0", "201601")
      )
    ).map(arr => Row(arr(0), arr(1), arr(2).toDouble, arr(3).toDouble, arr(4)))

    sqlContext.createDataFrame(bgSalesRDD, schema.struct)
  }

  test("Save block group sales to Hive") {
    val bgSales = createTestBGSales()

    NeighborhoodApp.storeInHive(sqlContext, bgSales)

    val bgSalesFromHive = sqlContext.sql("SELECT * FROM na.block_group_sales")
    bgSalesFromHive.collect() should have size 3
    bgSalesFromHive.agg(sum("sales_usd_amt")).collect()(0).getDouble(0) should equal (9.0)

  }


  ignore("Check configuration - broadcast join") {
    val conf = sqlContext.sparkContext.getConf
    NeighborhoodApp.setConfigurationProperties(conf)
    val broadcastSize = conf.get("spark.sql.autoBroadcastJoinThreshold").toInt

    broadcastSize should be >= (10485760)
  }


}
