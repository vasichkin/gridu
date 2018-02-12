/**
  * Created by vkhomenko on 12/6/17.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DateType => _, StringType => _, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.commons.net.util.SubnetUtils



object FinalExam {
  def main(args: Array[String]) {
    val home = "/Users/vasyl/Projects/Hadoop/SparkHelloWorld/"
    val dataFile = home + "purchases.csv"
    val geoDataFile = home + "GeoLite2-Country-Blocks-IPv4.csv"
    val geoLocFile = home + "GeoLite2-Country-Locations-en.csv"
    val sc = new SparkContext(new SparkConf().set("spark.master", "local").setAppName("Final Exam Dataframe"))

    val sqlContext = new SQLContext(sc)
    val geoData = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(geoDataFile)

    val geoLoc = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(geoLocFile)

    val customSchema = StructType(Array(
      StructField("date", DateType, true),
      StructField("time", StringType, true),
      StructField("ip", StringType, true),
      StructField("item", StringType, true),
      StructField("cat", StringType, true),
      StructField("price", StringType, true)))

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .schema(customSchema)
      .option("mode", "PERMISSIVE")
      .option("header", "false")
      .option("delimiter", "\t")
      .option("nullValue", "Empty")
      .option("dateFormat","yyyy-MM-dd")
      .load(dataFile)

    // 3 Most bought items
    val ranked = df.select("item")
      .groupBy("item")
      .count()
      .orderBy(desc("Count"))
      .limit(10)


    // Most bought items for categories
    val byCatName = Window.partitionBy("cat").orderBy(desc("count"))
    val df1 = df.orderBy("cat").groupBy("cat", "item").count()
    val rankByCat = df1.withColumn("rank", rank.over(byCatName))
    // Limit by catagory??


    // UDF for IP to integer
    val ipToLong: (String => Int) = (net: String) => {var addr = new SubnetUtils(net,"255.255.255.254"); addr.getInfo().asInteger(addr.getInfo().getAddress)}
    val ipToIntfunc = udf(ipToLong)

    val netMaxToLong: (String => Int) = (net: String) => {var addr = new SubnetUtils(net); addr.getInfo().asInteger(addr.getInfo().getHighAddress)}
    val netMaxToLongFunc = udf(netMaxToLong)

    val netMinToLong: (String => Int) = (net: String) => {var addr = new SubnetUtils(net); addr.getInfo().asInteger(addr.getInfo().getLowAddress)}
    val netMinToLongFunc = udf(netMinToLong)

    // Add column with integer value of IP
    val ndf = df.withColumn("ipInt", ipToIntfunc(col("ip")))
    // Add column with integer value of network boundaries. Add country name
    val networks = geoData.withColumn("ipMin", netMinToLongFunc(col("network"))).withColumn("ipMax", netMaxToLongFunc(col("network"))).join(geoLoc, "geoname_id")


    // Join based on IP and network boundaries. Group by country, orderm limit.
    val RankedByCountry = ndf.as("orders")
      .join(networks.as("networks"),
        col("orders.ipInt").geq(col("networks.ipMin"))
          .and(col("orders.ipInt").leq(col("networks.ipMax"))), "inner")
      .select("date", "item", "cat", "price", "country_name")
      .groupBy("country_name").count()
      .orderBy(desc("Count"))
      .limit(10)



    //ranked.show()
    //rankByCat.show()
    //RankedByCountry.show()

    //      save
    //      .format("com.databricks.spark.csv")
    //      .option("header", "true")
    //      .save("prices.csv")

  }


}
