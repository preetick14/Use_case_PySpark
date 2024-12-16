from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_date, date_sub, to_date

spark = SparkSession.builder.appName("Log File Analysis").getOrCreate()

df = spark.read.option("multiline", "true").json("/app/sample_logs.json")
df.show()

logs_df = df.withColumn('timestamp', to_timestamp(col('timestamp'), 'yyyy-MM-dd\'T\'HH:mm:ss'))
logs_df.show()

# Top 3 servers with the highest ERROR logs

logs_past_week_df = logs_df.filter(col("timestamp") >= date_sub(current_date(), 7))
error_logs_df = logs_past_week_df.filter(col("log_level") == "ERROR")
top_servers_df = error_logs_df.groupBy("server_id").count()
top_servers_df.orderBy(col("count").desc()).show(3)

# Average logs per day by server

logs_past_week_df = logs_past_week_df.withColumn("date", to_date(col("timestamp")))
daily_logs_df = logs_past_week_df.groupBy("server_id", "date").count()
average_logs_df = daily_logs_df.groupBy("server_id").avg("count")
average_logs_df.show()

# Most common log messages for each severity level

log_message_summary_df = logs_past_week_df.groupBy("log_level", "message").count()
log_message_summary_df.orderBy(col("count").desc()).show()

spark.stop()