import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

object RoomTypes {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("RoomTypes")
      .enableHiveSupport()
      .getOrCreate()

    val berlinListingsDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true).option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("BerlinListings.csv")
      .cache()

    val parisListingsDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("ParisListings.csv")
      .cache()

    val madridListingsDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true).option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("MadridListings.csv")
      .cache()

    val listingsDS = berlinListingsDS
      .union(parisListingsDS)
      .union(madridListingsDS)

    val roomTypesDS = listingsDS.select("room_type")
      .distinct()
      .withColumn("room_type_id", monotonically_increasing_id + 1)
      .select("room_type_id", "room_type")

    roomTypesDS.write.insertInto("room_types")
  }
}
