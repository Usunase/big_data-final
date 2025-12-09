"""
Spark Structured Streaming job - Đọc từ Kafka, dự đoán và gửi lại kết quả
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel
import time

def streaming_prediction():
    # Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("HousePriceStreamingPrediction") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 60)
    print("SPARK STREAMING - DỰ ĐOÁN GIÁ NHÀ")
    print("=" * 60)
    
    # Load mô hình đã huấn luyện
    model_path = "models/house_price_model"
    print(f"📂 Đang tải mô hình từ: {model_path}")
    model = PipelineModel.load(model_path)
    print("✓ Đã tải mô hình thành công")
    
    # Schema cho dữ liệu từ Kafka
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("MedInc", DoubleType(), True),
        StructField("HouseAge", DoubleType(), True),
        StructField("AveRooms", DoubleType(), True),
        StructField("AveBedrms", DoubleType(), True),
        StructField("Population", DoubleType(), True),
        StructField("AveOccup", DoubleType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("actual_price", DoubleType(), True)
    ])
    
    # Đọc dữ liệu từ Kafka
    print("📥 Đang kết nối đến Kafka topic: house-prices-input")
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "house-prices-input") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON
    df_parsed = df_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Dự đoán
    predictions = model.transform(df_parsed)
    
    # Chuẩn bị dữ liệu để gửi lại Kafka
    result = predictions.select(
        col("id"),
        col("actual_price"),
        col("prediction").alias("predicted_price"),
        (col("prediction") - col("actual_price")).alias("error"),
        ((col("prediction") - col("actual_price")) / col("actual_price") * 100).alias("error_percentage")
    )
    
    # Chuyển thành JSON để gửi vào Kafka
    kafka_output = result.select(
        to_json(struct("*")).alias("value")
    )
    
    # Ghi kết quả vào Kafka topic mới
    query = kafka_output \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "house-prices-output") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()
    
    # Console output để debug
    console_query = result \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    print("=" * 60)
    print("✓ Streaming đã bắt đầu!")
    print("📊 Đang xử lý dữ liệu và gửi kết quả vào: house-prices-output")
    print("=" * 60)
    
    # Chờ cho đến khi bị dừng
    try:
        query.awaitTermination()
        console_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⚠️  Đang dừng streaming...")
        query.stop()
        console_query.stop()
        spark.stop()
        print("✓ Đã dừng streaming")

if __name__ == "__main__":
    streaming_prediction()