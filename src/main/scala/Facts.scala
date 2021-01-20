import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, udf, count, avg, sum}

object Facts {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("Facts")
      .enableHiveSupport()
      .getOrCreate()

    val berlinListingsDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true).option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("labs/spark/BerlinListings.csv")
      .cache()

    val parisListingsDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("labs/spark/ParisListings.csv")
      .cache()

    val madridListingsDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true).option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("labs/spark/MadridListings.csv")
      .cache()

    val listingsDS = berlinListingsDS
      .union(parisListingsDS)
      .union(madridListingsDS)

    val berlinCalendarDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true).option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("labs/spark/BerlinCalendar.csv")
      .cache()

    val madridCalendarDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true).option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("labs/spark/MadridCalendar.csv")
      .drop("adjusted_price", "minimum_nights", "maximum_nights")
      .cache()

    val parisCalendarDS = sqlContext.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      option("quote", "\"").
      option("escape", "\"").
      option("multiline", true).
      csv("labs/spark/ParisCalendar.csv").cache()

    val calendarDS = berlinCalendarDS
      .union(madridCalendarDS)
      .union(parisCalendarDS)

    val roomTypesDS = sqlContext.sql("SELECT * FROM room_types")
    val neighbourhoodsDS = sqlContext.sql("SELECT * FROM neighbourhoods")
    val timesDS = sqlContext.sql("SELECT * FROM times")
    val priceRangesDS = sqlContext.sql("SELECT * FROM price_ranges")

    val sentiments - sqlContext.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      option("quote", "\"").
      option("escape", "\"").
      option("multiline", true).
      csv("labs/spark/ParisCalendar.csv").cache()

    def priceParser: (String => Float) = {s => if (s != null) s.replaceAll("[$,]", "").toFloat else 0}
    val priceParse = udf(priceParser)

    def priceMerger: ((Float, Float) => Float) = {(calendarPrice, listingPrice) => if (calendarPrice == 0.0) listingPrice else calendarPrice}
    val priceMerge = udf(priceMerger)

    val factsDS = calendarDS.withColumn("calendarPrice", priceParse(col("price")))
      .join(listingsDS.withColumnRenamed("price", "listingPrice"), calendarDS("listing_id") === listingsDS("id"))
      .withColumn("price", priceMerge(col("calendarPrice"), col("listingPrice")))
      .drop("calendarPrice", "listingPrice")
      .join(roomTypesDS, listingsDS("room_type") === roomTypesDS("room_type"))
      .join(neighbourhoodsDS, listingsDS("neighbourhood") === neighbourhoodsDS("neighbourhood"))
      .join(timesDS, calendarDS("date") === timesDS("date"))
      .join(priceRangesDS, col("price") >= priceRangesDS("lower_bound") && col("price") <= priceRangesDS("upper_bound"))
      .groupBy(col("neighbourhood_id"), col("date_id"), col("price_range_id"), col("room_type_id"))
      .agg(count("listing_id").as("count"), avg("availability_365").as("availability"), sum("price").as("price_sum"))
      .withColumn("score", lit(0))
      .withColumn("positive_review_count", lit(0))
      .withColumn("negative_review_count", lit(0))
      .select("count", "availability", "price_sum", "score", "positive_review_count", "negative_review_count", "room_type_id", "date_id", "neighbourhood_id", "price_range_id")


    factsDS.write.insertInto("facts")
  }
}
