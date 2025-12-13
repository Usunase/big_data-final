"""
Spark Structured Streaming job - Đọc từ Kafka, dự đoán và gửi lại kết quả
ĐÃ SỬA: Thêm timeout để tự động dừng sau khi xử lý xong
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel
import time
import sys

def streaming_prediction():
    # Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("HousePriceStreamingPrediction") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
        .getOrCreate()

    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 60)
    print("SPARK STREAMING - DỰ ĐOÁN GIÁ NHÀ")
    print("=" * 60)
    
    # Load mô hình đã huấn luyện
    model_path = "models/house_price_model"
    print(f"📂 Đang tải mô hình từ: {model_path}")
    try:
        model = PipelineModel.load(model_path)
        print("✓ Đã tải mô hình thành công")
    except Exception as e:
        print(f"❌ Lỗi khi tải mô hình: {e}")
        spark.stop()
        sys.exit(1)
    
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
        .option("kafka.bootstrap.servers", "192.168.80.127:9092") \
        .option("subscribe", "house-prices-input") \
        .option("startingOffsets", "earliest") \
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
        .option("kafka.bootstrap.servers", "192.168.80.127:9092") \
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
    
    # ĐÃ SỬA: Thêm timeout để tự động dừng
    timeout_seconds = 120  # 2 phút
    print(f"⏰ Streaming sẽ chạy trong {timeout_seconds} giây")
    
    try:
        # Đợi với timeout
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            if not query.isActive or not console_query.isActive:
                print("⚠️  Query đã dừng bất ngờ")
                break
            time.sleep(5)  # Check mỗi 5 giây
        
        print(f"\n✓ Đã hoàn thành streaming sau {int(time.time() - start_time)} giây")
        
    except KeyboardInterrupt:
        print("\n⚠️  Nhận được tín hiệu dừng...")
    
    finally:
        print("🛑 Đang dừng streaming queries...")
        query.stop()
        console_query.stop()
        spark.stop()
        print("✓ Đã dừng streaming hoàn toàn")

if __name__ == "__main__":
    streaming_prediction()