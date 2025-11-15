from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when

# 1️⃣ Créer la session Spark et ajouter les drivers JDBC
spark = SparkSession.builder \
    .appName("EventsPipeline") \
    .config("spark.jars", 
            "C:/Users/hp/Desktop/amazon_product_reviewz/jars/postgresql-42.6.0.jar;"
            "C:/Users/hp/Desktop/amazon_product_reviewz/jars/clickhouse-jdbc-0.3.2-patch1-all.jar") \
    .getOrCreate()

# ----------------------------
# 2️⃣ Lire la table PostgreSQL
# ----------------------------
postgres_url = "jdbc:postgresql://host.docker.internal:5432/kafka_db"
postgres_properties = {
    "user": "postgres",
    "password": "testpassword123",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(
    url=postgres_url,
    table="events",
    properties=postgres_properties
)

df.show(5)

# -----------------------------------
# 3️⃣ Calculer métriques par type d'événement
# -----------------------------------
metrics_df = df.groupBy("event_type").agg(
    count("*").alias("total_events"),
    count(when(col("status") == "SUCCESS", True)).alias("success_count"),
    count(when(col("status") == "ERROR", True)).alias("error_count"),
    avg("request_latency_ms").alias("avg_latency_ms")
)

metrics_df.show()

# -----------------------------------
# 4️⃣ Écrire les métriques dans ClickHouse
# -----------------------------------
clickhouse_url = "jdbc:clickhouse://clickhouse:8123/default"
clickhouse_properties = {
    "user": "default",
    "password": "monMotDePasse123",  # ✅ mot de passe correct
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

metrics_df.write.jdbc(
    url=clickhouse_url,
    table="event_metrics",
    mode="append",
    properties=clickhouse_properties
)

# 5️⃣ Optionnel : sauvegarder en JSON local
metrics_df.write.mode("overwrite").json(
    "C:/Users/hp/Desktop/amazon_product_reviewz/output/metrics.json"
)

# 6️⃣ Stopper Spark
spark.stop()
