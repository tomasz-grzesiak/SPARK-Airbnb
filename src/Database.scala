spark.sql("drop table if exists neighbourhoods")
spark.sql("drop table if exists times")
spark.sql("drop table if exists price_ranges")
spark.sql("drop table if exists facts")
spark.sql("drop table if exists room_types")

spark.sql("""CREATE TABLE IF NOT EXISTS `neighbourhoods` (
 `neighbourhood_id` long,
 `neighbourhood` string,
 `neighbourhood_group` string,
 `city` string)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE IF NOT EXISTS `times` (
 `date_id` long,
 `date` string,
 `day_of_week` string,
 `week_of_month` int,
 `month` int,
 `quarter` int,
 `year` int)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE IF NOT EXISTS `room_types` (
 `room_type_id` long,
 `room_type` string)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE IF NOT EXISTS `price_ranges` (
 `price_range_id` long,
 `lower_bound` int,
 `upper_bound` int)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE IF NOT EXISTS `facts` (
 `count` int,
 `availability` int,
 `price_sum` int,
 `positive_review_count` int,
 `negative_review_count` int,
 `room_type_id` long,
 `date_id` long,
 `neighbourhood_id` long,
 `price_range_id` long)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")