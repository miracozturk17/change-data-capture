# Spark master local.
# spark-submit --master local --packages org.apache.hadoop:hadoop-aws:3.2.0,io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 kafka_to_spark.py


# Ilgili kutuphaneler eklendi.
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import get_json_object
from pyspark.sql.types import StringType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType, BooleanType


# Spark dizini bulundu.
import findspark
findspark.init("/opt/spark")


# MinIO bilgileri belirlendi.
# NOT: Erisim bilgileri bu sekilde kullanilmamali.
accessKeyId='istanbul'
secretAccessKey='istanbul'


# SparkSession olusturuldu.
spark = SparkSession.builder \
.appName("Spark Example MinIO") \
.master("local[2]") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
.config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
.config("spark.hadoop.fs.s3a.access.key", accessKeyId) \
.config("spark.hadoop.fs.s3a.secret.key", secretAccessKey) \
.config("spark.hadoop.fs.s3a.path.style.access", True) \
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
.getOrCreate()


# Kafka Topic dinlemesinde bulunuldu.
df = (spark 
.readStream 
.format("kafka") 
.option("kafka.bootstrap.servers", "kafka:9092") 
.option("subscribe", "dbserver1.public.customers") 
.load())


# Her batch islemi icin bu foreach icerisinde calisacak fonksiyon yapilandirildi.
# JSON semasi tanimlandi. (Kafka dan gelen bilgiler.)
def write_json_to_parquet(batch_df, batch_id):
    json_schema = StructType([
        StructField("schema", StructType([
            StructField("type", StringType(), True),
            StructField("fields", ArrayType(StructType([
                StructField("type", StringType(), True),
                StructField("optional", BooleanType(), True),
                StructField("default", StringType(), True),
                StructField("field", StringType(), True),
            ])), True),
            StructField("optional", BooleanType(), True),
            StructField("name", StringType(), True),
            StructField("field", StringType(), True),
        ]), True),
        
        StructField("payload", StructType([
            StructField("before", StructType([
                StructField("customerId", IntegerType(), False),
                StructField("customerFName", StringType(), True),
                StructField("customerLName", StringType(), True),
                StructField("customerEmail", StringType(), True),
                StructField("customerPassword", StringType(), True),
                StructField("customerStreet", StringType(), True),
                StructField("customerCity", StringType(), True),
                StructField("customerState", StringType(), True),
                StructField("customerZipcode", StringType(), True),
            ]), True),
            
            StructField("after", StructType([
                StructField("customerId", IntegerType(), False),
                StructField("customerFName", StringType(), True),
                StructField("customerLName", StringType(), True),
                StructField("customerEmail", StringType(), True),
                StructField("customerPassword", StringType(), True),
                StructField("customerStreet", StringType(), True),
                StructField("customerCity", StringType(), True),
                StructField("customerState", StringType(), True),
                StructField("customerZipcode", StringType(), True),
            ]), True),
            
            StructField("source", StructType([
                StructField("version", StringType(), False),
                StructField("connector", StringType(), False),
                StructField("name", StringType(), False),
                StructField("ts_ms", LongType(), False),
                StructField("snapshot", StringType(), True),
                StructField("db", StringType(), False),
                StructField("sequence", StringType(), True),
                StructField("schema", StringType(), False),
                StructField("table", StringType(), False),
                StructField("txId", LongType(), True),
                StructField("lsn", LongType(), True),
                StructField("xmin", LongType(), True),
            ]), False),
            
            StructField("op", StringType(), False),
            StructField("ts_ms", LongType(), True),
            
            StructField("transaction", StructType([
                StructField("id", StringType(), False),
                StructField("total_order", LongType(), False),
                StructField("data_collection_order", LongType(), False),
            ]), True),
        ]), False),
    ])


    # Veriler semaya uygun hale getirildi.
    batch_df = batch_df.selectExpr("cast(value as string) as json") \
        .select(F.from_json("json", schema=json_schema).alias("data")) \
        .select(
            F.col("data.payload.after.customerId").alias("after_customerId"),
            F.col("data.payload.after.customerFName").alias("after_customerFName"),
            F.col("data.payload.after.customerLName").alias("after_customerLName"),
            F.col("data.payload.after.customerEmail").alias("after_customerEmail"),
            F.col("data.payload.after.customerPassword").alias("after_customerPassword"),
            F.col("data.payload.after.customerStreet").alias("after_customerStreet"),
            F.col("data.payload.after.customerCity").alias("after_customerCity"),
            F.col("data.payload.after.customerState").alias("after_customerState"),
            F.col("data.payload.after.customerZipcode").alias("after_customerZipcode"),
            F.col("data.payload.before.customerId").alias("before_customerId"),
            F.col("data.payload.before.customerFName").alias("before_customerFName"),
            F.col("data.payload.before.customerLName").alias("before_customerLName"),
            F.col("data.payload.before.customerEmail").alias("before_customerEmail"),
            F.col("data.payload.before.customerPassword").alias("before_customerPassword"),
            F.col("data.payload.before.customerStreet").alias("before_customerStreet"),
            F.col("data.payload.before.customerCity").alias("before_customerCity"),
            F.col("data.payload.before.customerState").alias("before_customerState"),
            F.col("data.payload.before.customerZipcode").alias("before_customerZipcode"),
            F.col("data.payload.op").alias("op"),
            F.col("data.payload.ts_ms").alias("ts_ms")
        )


    # Gerekli sutunlar/sema terminalde gosterildi. (Kontrol amacli.) 
    batch_df.printSchema()


    # MinIO uzerindeki belirlenen konumuna ilgili dosya yazildi.
    output_path = f"s3a://datasets/iris_parquet"
    batch_df.write.format("parquet").mode("append").save(output_path)
    print(f"Parquet dosyasi basariyla yazildi: {output_path}")


# output_dir = "file:///tmp/streaming/data_output"  
# checkpoint bilgilerinin kaydedilecegi dizin belirlendi.
checkpoint_dir = "file:///tmp/streaming/read_from_kafka_test22"


# Ilgili bilgiler konsola yazildi. (Kontrol amacli.) 
console_query = (df
.writeStream
.format("console")
.outputMode("append")
.trigger(processingTime="2 second")
.option("checkpointLocation", checkpoint_dir)
.option("numRows", 20)
.option("truncate", False)
.start())


# Ilgili bilgilerin dosyasi MinIo ya yazildi.  
minio_query = (df
.writeStream
.foreachBatch(write_json_to_parquet)
.option("checkpointLocation", checkpoint_dir)
.start())


# Veri akislari baslatildi.
minio_query.awaitTermination()
console_query.awaitTermination()