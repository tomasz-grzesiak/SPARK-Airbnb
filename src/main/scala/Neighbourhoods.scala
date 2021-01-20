import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{monotonically_increasing_id, lit}

object Neighbourhoods {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("Neighbourhoods")
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

    val berlinNeighbourhoodsDS = berlinListingsDS
      .select("neighbourhood", "neighbourhood_group")
      .distinct()
      .withColumn("neighbourhood_id", monotonically_increasing_id + 1)
      .withColumn("city", lit("Berlin"))
      .select("neighbourhood_id", "neighbourhood", "neighbourhood_group", "city")

    val parisNeighbourhoodsDS = parisListingsDS
      .select("neighbourhood", "neighbourhood_group")
      .distinct()
      .withColumn("neighbourhood_id", monotonically_increasing_id + 1)
      .withColumn("city", lit("Paris"))
      .select("neighbourhood_id", "neighbourhood", "neighbourhood_group", "city")

    val madridNeighbourhoodsDS = madridListingsDS
      .select("neighbourhood", "neighbourhood_group")
      .distinct()
      .withColumn("neighbourhood_id", monotonically_increasing_id + 1)
      .withColumn("city", lit("Madrid"))
      .select("neighbourhood_id", "neighbourhood", "neighbourhood_group", "city")

    berlinNeighbourhoodsDS.write.insertInto("neighbourhoods")
    parisNeighbourhoodsDS.write.insertInto("neighbourhoods")
    madridNeighbourhoodsDS.write.insertInto("neighbourhoods")
  }
}
