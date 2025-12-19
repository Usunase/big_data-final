"""
Spark ML job để huấn luyện mô hình Random Forest
Đọc dữ liệu từ HDFS và lưu model lên HDFS
"""
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import os
import sys

# Cấu hình HDFS
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://192.168.80.148:9000")
HDFS_DATA_DIR = os.getenv("HDFS_DATA_DIR", "/bigdata/house_prices")
HDFS_MODEL_DIR = os.getenv("HDFS_MODEL_DIR", "/bigdata/house_prices/models")

def train_model():
    # Khởi tạo Spark Session với HDFS
    spark = SparkSession.builder \
        .appName("HousePriceModelTraining") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.local.dir", "/tmp/spark_local") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 60)
    print("BẮT ĐẦU HUẤN LUYỆN MÔ HÌNH")
    print("=" * 60)
    print(f"HDFS Namenode: {HDFS_NAMENODE}")
    print(f"HDFS Data Dir: {HDFS_DATA_DIR}")
    print(f"HDFS Model Dir: {HDFS_MODEL_DIR}")
    
    # Đọc dữ liệu huấn luyện từ HDFS
    hdfs_train_path = f"{HDFS_DATA_DIR}/train_data.csv"
    print(f"\n📂 Đang đọc dữ liệu từ HDFS: {hdfs_train_path}")
    
    try:
        df = spark.read.csv(hdfs_train_path, header=True, inferSchema=True)
    except Exception as e:
        print(f"❌ Lỗi khi đọc dữ liệu từ HDFS: {e}")
        print(f"💡 Đảm bảo đã upload dữ liệu lên HDFS bằng script upload_to_hdfs.py")
        spark.stop()
        sys.exit(1)
    
    print(f"\n✓ Đã đọc {df.count()} mẫu từ {data_path}")
    print("\nSchema:")
    df.printSchema()
    
    # Các cột đặc trưng (tất cả trừ cột target)
    feature_cols = [col for col in df.columns if col != 'target']
    
    # Tạo vector assembler
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )
    
    # Tạo mô hình Random Forest
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="target",
        numTrees=100,
        maxDepth=10,
        seed=42
    )
    
    # Tạo pipeline
    pipeline = Pipeline(stages=[assembler, rf])
    
    # Chia dữ liệu train/test
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
    
    print(f"\n✓ Dữ liệu train: {train_data.count()} mẫu")
    print(f"✓ Dữ liệu test: {test_data.count()} mẫu")
    
    # Huấn luyện mô hình
    print("\n🔄 Đang huấn luyện mô hình Random Forest...")
    model = pipeline.fit(train_data)
    
    # Đánh giá mô hình
    predictions = model.transform(test_data)
    
    evaluator_rmse = RegressionEvaluator(
        labelCol="target",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    evaluator_r2 = RegressionEvaluator(
        labelCol="target",
        predictionCol="prediction",
        metricName="r2"
    )
    
    evaluator_mae = RegressionEvaluator(
        labelCol="target",
        predictionCol="prediction",
        metricName="mae"
    )
    
    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    
    print("\n" + "=" * 60)
    print("KẾT QUẢ ĐÁNH GIÁ MÔ HÌNH")
    print("=" * 60)
    print(f"RMSE: {rmse:.4f}")
    print(f"MAE:  {mae:.4f}")
    print(f"R²:   {r2:.4f}")
    print("=" * 60)
    
    # Lưu mô hình lên HDFS
    hdfs_model_path = f"{HDFS_MODEL_DIR}/house_price_model"
    print(f"\n💾 Đang lưu mô hình lên HDFS: {hdfs_model_path}")
    
    try:
        model.write().overwrite().save(hdfs_model_path)
        print(f"✓ Đã lưu mô hình vào HDFS: {hdfs_model_path}")
    except Exception as e:
        print(f"❌ Lỗi khi lưu mô hình lên HDFS: {e}")
        # Fallback: lưu local nếu HDFS lỗi
        local_model_path = "models/house_price_model"
        os.makedirs("models", exist_ok=True)
        model.write().overwrite().save(local_model_path)
        print(f"⚠️  Đã lưu mô hình local (fallback): {local_model_path}")
    
    # Hiển thị một số dự đoán mẫu
    print("\nMột số dự đoán mẫu:")
    predictions.select("target", "prediction").show(10, truncate=False)
    
    spark.stop()
    print("\n✓ Hoàn thành quá trình huấn luyện!")

if __name__ == "__main__":
    train_model()