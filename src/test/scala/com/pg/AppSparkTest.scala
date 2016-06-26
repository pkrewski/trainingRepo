package com.pg

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest._
import org.apache.spark.sql.functions._
import TestUtils._

// Test class. It inherits from FunSuite - scala class with all the amenities to write unit tests
class NATest extends FunSuite with BeforeAndAfterAll with Matchers {

  // test spark context to be used in all the spark tests
  var sqlContext: TestHiveContext = _

  // this method runs before any test in the suite
  override def beforeAll() {
    sqlContext = SparkContextFactory.getSqlContext  // initialize test spark context
    prepareTables(sqlContext) // prepare test hive tables
  }

  test("Dummy test that should always pass") {
    Array(1) should have size 1
    Array(1, 2) should contain allOf (1, 2)
    true should equal (true)
  }

  ignore("Load store sales") {
    val storeSales = NeighborhoodApp.loadStoreSales(sqlContext, "na")

    storeSales.collect() should have size 10
    storeSales.select("retailer_name").distinct().collect().map(_.getString(0)) should contain allOf("Retailer1", "Retailer2")
    storeSales.agg(sum("sales_usd_amt")).collect()(0).getDouble(0) should equal(19.0)
  }

  ignore("Load Trade Area sales") {
    val taSales = NeighborhoodApp.loadTradeAreaSales(sqlContext, "src/test/resources/ta_sales.txt")

    taSales.collect() should have size 4
    taSales.select("upc").distinct().collect().map(_.getString(0)) should contain allOf("01", "02", "03")
    taSales.agg(sum("sales_usd_amt")).collect()(0).getDouble(0) should equal(7.0)
  }

  ignore("Load Trade Area to store key") {
    val ta2storeKey = NeighborhoodApp.loadTA2StoreKey(sqlContext, "src/test/resources/ta_2_store_key.txt")

    ta2storeKey.collect() should have size 4
    ta2storeKey.select("trade_area_name").distinct().collect().map(_.getString(0)) should contain allOf("TA1", "TA2")
  }

  ignore("Distribute Trade Area sales") {
    val taSales = NeighborhoodApp.loadTradeAreaSales(sqlContext, "src/test/resources/ta_sales.txt")
    val ta2storeKey = NeighborhoodApp.loadTA2StoreKey(sqlContext, "src/test/resources/ta_2_store_key.txt")

    val distTASales = NeighborhoodApp.distributeTASales(taSales, ta2storeKey)

    distTASales.collect() should have size 8
    distTASales.select("retailer_name").filter(distTASales("retailer_name") === "Retailer3").count() should equal(4)
  }

  ignore("Combine all store sales") {
    val allStoreSales: DataFrame = loadAllStoreSales(sqlContext)

    allStoreSales.collect() should have size 18
    allStoreSales.filter(allStoreSales("month") === "201601").agg(sum("sales_unit_qty")).collect()(0).getDouble(0) should equal(19.0)
  }

  ignore("Distribute sales to block groups") {
    val allStoreSales = loadAllStoreSales(sqlContext)
    val store2BG = loadStore2BGKey(sqlContext)

    val bgUpcSales = NeighborhoodApp.distributeSalesToBlockGroups(allStoreSales, store2BG)

    allStoreSales.filter(allStoreSales("month") === "201601").count() should equal(13)
    val oneMonth = allStoreSales.filter(allStoreSales("month") === "201601")
    oneMonth.agg(sum("sales_usd_amt"), sum("sales_unit_qty")).collect().map(r => (r.getDouble(0), r.getDouble(1))).toList(0) should equal(17.0, 19.0)
  }

  ignore("Save block group sales to Hive") {
    val bgSales = createTestBGSales(sqlContext)

    NeighborhoodApp.storeInHive(sqlContext, bgSales, "na")

    val bgSalesFromHive = sqlContext.sql("SELECT * FROM na.block_group_sales")
    bgSalesFromHive.collect() should have size 3
    bgSalesFromHive.agg(sum("sales_usd_amt")).collect()(0).getDouble(0) should equal (9.0)

  }

}
