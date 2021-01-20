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

    val berlinCalendarDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true).option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("BerlinCalendar.csv")
      .cache()

    val madridCalendarDS = sqlContext.read.format("org.apache.spark.csv")
      .option("header", true).option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline", true)
      .csv("MadridCalendar.csv")
      .drop("adjusted_price", "minimum_nights", "maximum_nights")
      .cache()

    val parisCalendarDS = sqlContext.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      option("quote", "\"").
      option("escape", "\"").
      option("multiline", true).
      csv("ParisCalendar.csv").cache()

    val calendarDS = berlinCalendarDS
      .union(madridCalendarDS)
      .union(parisCalendarDS)

    val roomTypesDS = sqlContext.sql("SELECT * FROM room_types")
    val neighbourhoodsDS = sqlContext.sql("SELECT * FROM neighbourhoods")
    val timesDS = sqlContext.sql("SELECT * FROM times")
    val priceRangesDS = sqlContext.sql("SELECT * FROM price_ranges")


    val sentimentsDS = sqlContext.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      option("quote", "\"").
      option("escape", "\"").
      option("multiline", true).
      csv("reviews.csv").cache()

    def notNullParser: (Int => Int) = {s => if (s == null) 0 else s}
    val notNullParse = udf(priceParser)

    val sentimentsPositiveDS = sentimentsDS.filter(col("sentiment") >= 0.05)
      .groupBy(col("listing_id"))
      .agg(count("sentiment"))

    val sentimentsNegativeDS = sentimentsDS.filter(col("sentiment") <= -0.05)
      .groupBy(col("listing_id"))
      .agg(count("sentiment"))

    val sentimentsDS = sentimentsPositiveDS.join(sentimentsNegativeDS, sentimentsPositiveDS("listing_id") === sentimentsNegativeDS("listing_id"), "full_outer")
      .withColumn("sentiment_positive", notNullParse(sentimentsPositiveDS("sentiment")))
      .withColumn("sentiment_negative", notNullParse(sentimentsNegativeDS("sentiment")))
      .select(sentimentsPositiveDS("listing_id").as("listing_id"), "sentiment_positive", "sentiment_negative")

    def priceParser: (String => Float) = {s => if (s != null) s.replaceAll("[$,]", "").toFloat else 0}
    val priceParse = udf(priceParser)

    def priceMerger: ((Float, Float) => Float) = {(calendarPrice, listingPrice) => if (calendarPrice == 0.0) listingPrice else calendarPrice}
    val priceMerge = udf(priceMerger)

    val factsDS = calendarDS.withColumn("calendarPrice", priceParse(col("price")))
      .join(listingsDS.withColumnRenamed("price", "listingPrice"), calendarDS("listing_id") === listingsDS("id"), "right_outer")
      .withColumn("price", priceMerge(col("calendarPrice"), col("listingPrice")))
      .drop("calendarPrice", "listingPrice")
      .join(roomTypesDS, listingsDS("room_type") === roomTypesDS("room_type"), "left_outer")
      .join(neighbourhoodsDS, listingsDS("neighbourhood") === neighbourhoodsDS("neighbourhood"), "left_outer")
      .join(timesDS, calendarDS("date") === timesDS("date"), "left_outer")
      .join(priceRangesDS, col("price") >= priceRangesDS("lower_bound") && col("price") <= priceRangesDS("upper_bound"), "left_outer")
      .join(sentimentsDS, listingsDS("id") === sentimentsDS("listing_id"), "left_outer")
      .groupBy(col("neighbourhood_id"), col("date_id"), col("price_range_id"), col("room_type_id"))
      .agg(count("id").as("count"), avg("availability_365").as("availability"), sum("price").as("price_sum"), count("positive_review_count").as("positive"), count("negative_review_count").as("negative"))
      .select("count", "availability", "price_sum", "positive_review_count", "negative_review_count", "room_type_id", "date_id", "neighbourhood_id", "price_range_id")


    factsDS.write.insertInto("facts")
  }
}
