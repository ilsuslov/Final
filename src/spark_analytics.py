# spark_analytics.py - PySpark с HDFS
# Читает из Kafka, пишет рекомендации в Kafka и данные в HDFS

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
import json

print("📊 [Spark Analytics] Инициализация...")

spark = SparkSession.builder \
    .appName("MarketplaceAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.kafka.bootstrap.servers", "localhost:9095") \
    .config("spark.kafka.security.protocol", "SSL") \
    .config("spark.kafka.ssl.truststore.location", "../ssl/truststore.jks") \
    .config("spark.kafka.ssl.truststore.password", "changeit") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000") \
    .getOrCreate()

print("✅ [Spark] Сессия создана")

# Чтение из Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9095") \
    .option("subscribe", "products-filtered-replicated") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.location", "../ssl/truststore.jks") \
    .option("kafka.ssl.truststore.password", "changeit") \
    .load()

# Парсинг JSON
parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
    .selectExpr("""
        from_json(json_string, '
            product_id STRING,
            name STRING,
            price STRUCT<amount: DOUBLE, currency: STRING>,
            category STRING
        ') as data
    """) \
    .select("data.*")

# Генерация рекомендаций
analytics_df = parsed_df.withColumn(
    "score",
    when(col("price.amount") < 10000, 0.95).otherwise(0.60)
).withColumn(
    "user_id", lit("demo_user")
).withColumn(
    "reason", lit("Популярно в вашей категории")
)

# Запись рекомендаций в Kafka
query = analytics_df.selectExpr(
    "CAST(user_id AS STRING) as key",
    "to_json(struct(name as product_name, score, reason)) as value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9095") \
    .option("topic", "recommendations") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .outputMode("append") \
    .start()

# Запись сырых данных в HDFS (для аналитики)
hdfs_query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://hdfs-namenode:9000/data/products") \
    .option("checkpointLocation", "/tmp/hdfs-checkpoint") \
    .outputMode("append") \
    .start()

print("✅ [Spark] Потоковая обработка запущена")
print(" Данные пишутся в HDFS: hdfs://hdfs-namenode:9000/data/products")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n️ Остановлено")
    spark.stop()
