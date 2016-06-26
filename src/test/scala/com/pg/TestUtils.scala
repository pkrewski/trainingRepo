package com.pg

import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types.{StructType, DoubleType, StringType, StructField}


object TestUtils {
  def loadAllStoreSales(sqlContext: TestHiveContext) = {
    val taSales = NeighborhoodApp.loadTradeAreaSales(sqlContext, "src/test/resources/ta_sales.txt")
    val ta2storeKey = NeighborhoodApp.loadTA2StoreKey(sqlContext, "src/test/resources/ta_2_store_key.txt")
    val distTASales = NeighborhoodApp.distributeTASales(taSales, ta2storeKey)
    val storeSales = NeighborhoodApp.loadStoreSales(sqlContext, "na")
    val allStoreSales = NeighborhoodApp.combineStoreSales(storeSales, distTASales)
    allStoreSales
  }

  // create block group sales for test purposes
  def createTestBGSales(sqlContext: TestHiveContext): DataFrame = {
    object schema {
      val bgId = StructField("block_group_id", StringType)
      val upc = StructField("upc", StringType)
      val salesUsdAmt = StructField("sales_usd_amt", DoubleType)
      val salesUnitQty = StructField("sales_unit_qty", DoubleType)
      val month = StructField("month", StringType)

      // construct the schema
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

  // load store to block group mapping for test purposes
  def loadStore2BGKey(sqlContext: TestHiveContext): DataFrame = {
    sqlContext.sql("SELECT * FROM na.st2bg")
  }

  // create test hive tables to unit-test methods from NeighborhoodApp class
  def prepareTables(sqlContext: TestHiveContext): Unit = {
    sqlContext.sql( """CREATE DATABASE IF NOT EXISTS na""")
    sqlContext.sql( """DROP TABLE IF EXISTS na.store_sales""")
    sqlContext.sql( """DROP TABLE IF EXISTS na.st2bg""")
    sqlContext.sql( """DROP TABLE IF EXISTS na.block_group_sales""")

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

    sqlContext.sql( """
        CREATE TABLE IF NOT EXISTS na.block_group_sales (
                      block_group_id string,
                      upc string,
                      sales_usd_amt double,
                      sales_unit_qty double
        )
        PARTITIONED BY (month string)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE
                    """)

    sqlContext.sql( """LOAD DATA LOCAL INPATH 'src/test/resources/store_sales.txt' INTO TABLE na.store_sales""")
    sqlContext.sql( """LOAD DATA LOCAL INPATH 'src/test/resources/store_2_bg_key.txt' INTO TABLE na.st2bg""")
  }
}
