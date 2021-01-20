import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, dayofweek, monotonically_increasing_id, month, quarter, weekofyear, year}

object Times {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("Times")
      .enableHiveSupport()
      .getOrCreate()

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

    val timesDS = calendarDS.select("date")
      .distinct()
      .withColumn("date_id", monotonically_increasing_id + 1)
      .withColumn("day_of_week", dayofweek(col("date")))
      .withColumn("week", weekofyear(col("date")))
      .withColumn("month", month(col("date")))
      .withColumn("quarter", quarter(col("date")))
      .withColumn("year", year(col("date")))
      .select("date_id", "date", "day_of_week", "week", "month", "quarter", "year")

    timesDS.write.insertInto("times")
  }
}