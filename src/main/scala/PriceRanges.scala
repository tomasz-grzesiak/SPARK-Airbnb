import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

object PriceRanges {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("PriceRanges")
      .enableHiveSupport()
      .getOrCreate()

    val priceRangesArray = Array((0, 50), (51, 70), (71, 100), (101, 150), (151, Int.MaxValue))

    val priceRangesDS = session.createDataFrame(priceRangesArray)
      .withColumn("price_range_id", monotonically_increasing_id + 1)
      .withColumnRenamed("_1", "lower_bound")
      .withColumnRenamed("_2", "upper_bound")
      .select("price_range_id", "lower_bound", "upper_bound")


    priceRangesDS.write.insertInto("price_ranges")
  }
}
