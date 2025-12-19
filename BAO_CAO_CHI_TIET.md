# BÁO CÁO CHI TIẾT: HỆ THỐNG MACHINE LEARNING STREAMING VỚI SPARK, KAFKA VÀ AIRFLOW

## MỤC LỤC

1. [Tổng quan dự án](#1-tổng-quan-dự-án)
2. [Kiến trúc hệ thống](#2-kiến-trúc-hệ-thống)
3. [Phân tích chi tiết các thành phần](#3-phân-tích-chi-tiết-các-thành-phần)
4. [Luồng xử lý dữ liệu](#4-luồng-xử-lý-dữ-liệu)
5. [Kết quả và đánh giá](#5-kết-quả-và-đánh-giá)
6. [Kết luận](#6-kết-luận)

---

## 1. TỔNG QUAN DỰ ÁN

### 1.1. Mục tiêu

Dự án xây dựng một hệ thống Machine Learning streaming end-to-end để dự đoán giá nhà real-time sử dụng:
- **Apache Spark ML**: Huấn luyện và dự đoán với mô hình Random Forest
- **Apache Kafka**: Hệ thống message queue cho streaming data
- **Apache Airflow**: Orchestration và điều phối toàn bộ pipeline
- **Python**: Ngôn ngữ lập trình chính cho các thành phần

### 1.2. Kiến trúc phân tán

Hệ thống được triển khai trên kiến trúc phân tán gồm 3 máy:
- **Machine 1 (192.168.80.147)**: Airflow orchestrator
- **Machine 2 (192.168.80.127)**: Kafka cluster
- **Machine 3 (192.168.80.207)**: Spark cluster

### 1.3. Dataset

Sử dụng **California Housing Dataset** từ scikit-learn với các đặc trưng:
- `MedInc`: Thu nhập trung bình
- `HouseAge`: Tuổi nhà
- `AveRooms`: Số phòng trung bình
- `AveBedrms`: Số phòng ngủ trung bình
- `Population`: Dân số
- `AveOccup`: Mật độ chiếm dụng trung bình
- `Latitude`: Vĩ độ
- `Longitude`: Kinh độ
- `target`: Giá nhà trung bình (đơn vị: $100,000)

---

## 2. KIẾN TRÚC HỆ THỐNG

### 2.1. Sơ đồ tổng quan

```
┌─────────────────┐
│   Airflow DAG   │ (Orchestration)
└────────┬────────┘
         │
         ├──> Prepare Data ──> Train Model (Spark ML)
         │
         ├──> Start Kafka ──> Start Spark Streaming
         │
         └──> Producer ──> Kafka ──> Spark Streaming ──> Kafka Output
                                              │
                                              └──> Visualization
```

### 2.2. Các thành phần chính

1. **Data Preparation** (`data/prepare_data.py`)
2. **Model Training** (`spark_jobs/train_model.py`)
3. **Streaming Prediction** (`spark_jobs/streaming_predict.py`)
4. **Kafka Producer** (`streaming/kafka_producer.py`)
5. **Kafka Consumer & Visualization** (`visualization/kafka_consumer.py`)
6. **Airflow Orchestration** (`dags/ml_pipeline_dag.py`)

---

## 3. PHÂN TÍCH CHI TIẾT CÁC THÀNH PHẦN

### 3.1. Data Preparation (`data/prepare_data.py`)

#### 3.1.1. Mục đích
Chuẩn bị và chia dữ liệu thành hai tập:
- **Training data**: 80% dữ liệu để huấn luyện mô hình
- **Streaming data**: 20% dữ liệu để mô phỏng streaming

#### 3.1.2. Chức năng chính

```python
def prepare_data():
    # Tải dataset California Housing
    housing = fetch_california_housing()
    df = pd.DataFrame(housing.data, columns=housing.feature_names)
    df['target'] = housing.target
    
    # Chia 80% train, 20% streaming
    train_df, streaming_df = train_test_split(df, test_size=0.2, random_state=42)
    
    # Lưu vào CSV
    train_df.to_csv('data/train_data.csv', index=False)
    streaming_df.to_csv('data/streaming_data.csv', index=False)
```

#### 3.1.3. Đặc điểm
- Sử dụng `random_state=42` để đảm bảo reproducibility
- Tự động tạo thư mục `data/` nếu chưa tồn tại
- Tạo file `README.md` mô tả dataset

#### 3.1.4. Kết quả
- `data/train_data.csv`: ~16,512 mẫu (80%)
- `data/streaming_data.csv`: ~4,128 mẫu (20%)

---

### 3.2. Model Training (`spark_jobs/train_model.py`)

#### 3.2.1. Mục đích
Huấn luyện mô hình Random Forest Regressor để dự đoán giá nhà sử dụng Spark ML.

#### 3.2.2. Kiến trúc mô hình

**Pipeline gồm 2 stages:**
1. **VectorAssembler**: Kết hợp các đặc trưng thành vector
2. **RandomForestRegressor**: Mô hình hồi quy Random Forest

**Tham số mô hình:**
- `numTrees`: 100 cây
- `maxDepth`: 10
- `seed`: 42 (đảm bảo reproducibility)

#### 3.2.3. Quy trình huấn luyện

```python
# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("HousePriceModelTraining") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# 2. Đọc dữ liệu
df = spark.read.csv("file://data/train_data.csv", header=True, inferSchema=True)

# 3. Tạo pipeline
pipeline = Pipeline(stages=[assembler, rf])

# 4. Chia train/test (80/20)
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# 5. Huấn luyện
model = pipeline.fit(train_data)

# 6. Đánh giá
predictions = model.transform(test_data)
```

#### 3.2.4. Metrics đánh giá

Mô hình được đánh giá bằng 3 metrics:
- **RMSE** (Root Mean Squared Error): Sai số bình phương trung bình
- **MAE** (Mean Absolute Error): Sai số tuyệt đối trung bình
- **R²** (Coefficient of Determination): Hệ số xác định

#### 3.2.5. Lưu trữ mô hình

Mô hình được lưu vào: `models/house_price_model/`

Cấu trúc thư mục:
```
models/house_price_model/
├── metadata/
│   └── part-00000-*.txt
└── stages/
    ├── 0_VectorAssembler_*/
    └── 1_RandomForestRegressor_*/
        ├── data/
        ├── metadata/
        └── treesMetadata/
```

#### 3.2.6. Đặc điểm kỹ thuật
- Sử dụng Spark ML Pipeline để dễ dàng triển khai
- Hỗ trợ distributed training trên Spark cluster
- Tự động infer schema từ CSV
- Memory configuration: 4GB driver, 4GB executor

---

### 3.3. Streaming Prediction (`spark_jobs/streaming_predict.py`)

#### 3.3.1. Mục đích
Đọc dữ liệu streaming từ Kafka, áp dụng mô hình đã huấn luyện để dự đoán, và gửi kết quả về Kafka topic khác.

#### 3.3.2. Kiến trúc Spark Streaming

**Input:**
- Kafka topic: `house-prices-input`
- Format: JSON với schema định nghĩa sẵn

**Output:**
- Kafka topic: `house-prices-output`
- Format: JSON chứa id, actual_price, predicted_price, error, error_percentage

#### 3.3.3. Schema dữ liệu

```python
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
```

#### 3.3.4. Quy trình xử lý

```python
# 1. Load mô hình đã huấn luyện
model = PipelineModel.load("models/house_price_model")

# 2. Đọc stream từ Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.80.127:9092") \
    .option("subscribe", "house-prices-input") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Parse JSON
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 4. Dự đoán
predictions = model.transform(df_parsed)

# 5. Tính toán metrics
result = predictions.select(
    col("id"),
    col("actual_price"),
    col("prediction").alias("predicted_price"),
    (col("prediction") - col("actual_price")).alias("error"),
    ((col("prediction") - col("actual_price")) / col("actual_price") * 100).alias("error_percentage")
)

# 6. Gửi kết quả về Kafka
kafka_output = result.select(to_json(struct("*")).alias("value"))

query = kafka_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.80.127:9092") \
    .option("topic", "house-prices-output") \
    .option("checkpointLocation", "/tmp/checkpoint-house-prices-output") \
    .start()
```

#### 3.3.5. Tính năng đặc biệt

**Timeout mechanism:**
- Streaming job tự động dừng sau 120 giây (2 phút)
- Đảm bảo job không chạy vô hạn trong môi trường production

**Checkpoint:**
- Sử dụng checkpoint để đảm bảo exactly-once semantics
- Lưu tại `/tmp/checkpoint-house-prices-output`

**Console output:**
- Hiển thị kết quả dự đoán trên console để debug
- Format: append mode với truncate=False

#### 3.3.6. Cấu hình Kafka

- **Bootstrap servers**: `192.168.80.127:9092`
- **Starting offsets**: `earliest` (đọc từ đầu nếu chưa có offset)
- **Fail on data loss**: `false` (không fail nếu offset bị reset)

#### 3.3.7. Dependencies

- `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0`: Kafka connector cho Spark

---

### 3.4. Kafka Producer (`streaming/kafka_producer.py`)

#### 3.4.1. Mục đích
Mô phỏng nguồn dữ liệu streaming bằng cách gửi dữ liệu từ file CSV vào Kafka topic.

#### 3.4.2. Chức năng chính

```python
def send_streaming_data(interval=2, num_records=None):
    # Đọc dữ liệu streaming
    df = pd.read_csv('data/streaming_data.csv')
    
    if num_records:
        df = df.head(num_records)
    
    # Tạo producer
    producer = create_producer()
    
    # Gửi từng record
    for idx, row in df.iterrows():
        message = {
            'id': idx,
            'MedInc': float(row['MedInc']),
            # ... các trường khác
            'actual_price': float(row['target'])
        }
        producer.send('house-prices-input', value=message)
        time.sleep(interval)
```

#### 3.4.3. Tính năng

**Retry logic:**
- Tự động retry kết nối đến Kafka nếu chưa sẵn sàng
- Max retries: 10 lần với delay 5 giây

**Tham số dòng lệnh:**
- `interval`: Khoảng thời gian giữa các message (mặc định: 2 giây)
- `num_records`: Số lượng records gửi (mặc định: tất cả)

**Message format:**
- JSON serialization với UTF-8 encoding
- Kafka API version: (2, 5, 0)

#### 3.4.4. Cấu hình

- **Bootstrap servers**: `192.168.80.127:9092`
- **Topic**: `house-prices-input`
- **Value serializer**: JSON dumps với UTF-8 encoding

#### 3.4.5. Output

In ra console thông tin mỗi message đã gửi:
```
📤 Đã gửi bản ghi 1/200 | MedInc=8.32 | Actual Price=$452.60K
```

---

### 3.5. Kafka Consumer & Visualization (`visualization/kafka_consumer.py`)

#### 3.5.1. Mục đích
Đọc kết quả dự đoán từ Kafka và hiển thị trực quan hóa real-time.

#### 3.5.2. Kiến trúc Visualization

**Class: `RealtimeVisualizer`**

**Thành phần:**
- **Data structures**: Sử dụng `deque` với max length để lưu trữ dữ liệu
- **Matplotlib**: Hiển thị biểu đồ với animation
- **Kafka Consumer**: Đọc từ topic `house-prices-output`

#### 3.5.3. Biểu đồ hiển thị

**Plot 1: Actual vs Predicted Prices**
- Line chart so sánh giá thực tế và giá dự đoán
- X-axis: Sample Index
- Y-axis: Price ($1000s)
- Legend: Actual Price (blue), Predicted Price (red)

**Plot 2: Prediction Error**
- Bar chart hiển thị sai số tuyệt đối
- X-axis: Sample Index
- Y-axis: Absolute Error ($1000s)
- Color: Coral với alpha=0.7

**Metrics display:**
- MAE (Mean Absolute Error)
- RMSE (Root Mean Squared Error)
- Số lượng samples đã xử lý

#### 3.5.4. Quy trình hoạt động

```python
# 1. Khởi tạo consumer
consumer = KafkaConsumer(
    'house-prices-output',
    bootstrap_servers=['192.168.80.127:9092'],
    group_id='viz-consumer',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# 2. Poll messages
messages = consumer.poll(timeout_ms=100)

# 3. Update data structures
for record in records:
    data = record.value
    self.ids.append(data['id'])
    self.actual_prices.append(data['actual_price'] * 100)
    self.predicted_prices.append(data['predicted_price'] * 100)
    self.errors.append(abs(data['error']) * 100)

# 4. Update plot với FuncAnimation
ani = FuncAnimation(self.fig, self.update_plot, interval=1000)
```

#### 3.5.5. Tính năng

**Real-time updates:**
- Cập nhật biểu đồ mỗi 1 giây
- Sử dụng `FuncAnimation` từ matplotlib

**Data management:**
- Giới hạn số điểm hiển thị (max_points=100)
- Sử dụng deque để tự động loại bỏ dữ liệu cũ

**Consumer group:**
- Group ID: `viz-consumer`
- Auto commit: True
- Auto offset reset: `earliest`

#### 3.5.6. Output

Console output cho mỗi message:
```
📊 ID:    1 | Actual: $452.60K | Predicted: $445.23K | Error:   1.63%
```

---

### 3.6. Airflow Orchestration (`dags/ml_pipeline_dag.py`)

#### 3.6.1. Mục đích
Điều phối và tự động hóa toàn bộ pipeline từ chuẩn bị dữ liệu đến visualization.

#### 3.6.2. Cấu hình hệ thống phân tán

```python
KAFKA_HOST, KAFKA_PORT, KAFKA_USER = "192.168.80.127", 9092, "nindang"
SPARK_HOST, SPARK_USER = "192.168.80.207", "nindang"
SPARK_MASTER = f"spark://{SPARK_HOST}:7077"
PROJECT_DIR = "/home/haminhchien/Documents/bigdata/final_project"
```

#### 3.6.3. DAG chính: `ml_streaming_pipeline_distributed`

**Các tasks:**

1. **start_kafka_remote**
   - Khởi động Kafka cluster trên máy remote
   - Sử dụng SSH để chạy docker-compose
   - Kiểm tra containers đã chạy

2. **check_kafka_remote**
   - Kiểm tra Kafka đã sẵn sàng
   - Sử dụng socket connection test
   - Max retries: 30 với delay 10 giây

3. **ensure_kafka_output_topic**
   - Tạo Kafka topic `house-prices-output` nếu chưa tồn tại
   - Replication factor: 1
   - Partitions: 1

4. **start_spark_remote**
   - Khởi động Spark Master và Worker trên máy remote
   - Sử dụng SSH để chạy Spark scripts
   - Kiểm tra processes đã chạy

5. **check_spark_remote**
   - Kiểm tra Spark Master đã sẵn sàng
   - Port: 7077
   - Max retries: 10 với delay 5 giây

6. **prepare_data**
   - Chạy script `data/prepare_data.py`
   - Kiểm tra dữ liệu đã có sẵn

7. **train_model**
   - Submit Spark job để huấn luyện mô hình
   - Cấu hình: 4GB driver, 4GB executor, 2 executors, 2 cores/executor

8. **send_data_to_remote_kafka**
   - Chạy Kafka producer để gửi dữ liệu
   - Tham số: interval=1, num_records=200

9. **start_streaming_job**
   - Khởi động Spark Streaming job
   - Xóa checkpoint cũ để đọc lại từ đầu
   - Timeout: 5 phút

10. **wait_for_streaming**
    - Đợi streaming xử lý hoàn thành
    - Sleep: 5 phút (300 giây)

11. **cleanup**
    - Dọn dẹp processes và checkpoints
    - Trigger rule: `all_done` (chạy dù thành công hay thất bại)

#### 3.6.4. Dependencies giữa các tasks

```
start_kafka_remote >> check_kafka >> ensure_kafka_output_topic
start_spark_remote >> check_spark
[ensure_kafka_output_topic, check_spark] >> prepare_data >> train_model >> send_streaming_data >> start_streaming_job >> wait_processing >> cleanup
```

#### 3.6.5. DAG Visualization: `ml_streaming_visualization`

**Task:**
- `run_visualization`: Chạy script `visualization/kafka_consumer.py`

#### 3.6.6. Helper functions

**check_remote_ready(host, port, name, max_retries, delay)**
- Kiểm tra service đã sẵn sàng bằng socket connection
- Retry logic với delay

**wait_for_streaming_complete()**
- Đợi streaming hoàn thành với timeout

#### 3.6.7. Default arguments

```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
```

#### 3.6.8. Đặc điểm

- **Schedule**: None (manual trigger)
- **Catchup**: False
- **Tags**: ['distributed', 'machine-learning', 'kafka', 'spark']
- **SSH-based execution**: Sử dụng SSH để chạy commands trên máy remote
- **Error handling**: Retry logic và cleanup tasks

---

## 4. LUỒNG XỬ LÝ DỮ LIỆU

### 4.1. Luồng tổng quan

```
1. Airflow DAG được trigger
   ↓
2. Khởi động Kafka cluster (Machine 2)
   ↓
3. Khởi động Spark cluster (Machine 3)
   ↓
4. Chuẩn bị dữ liệu (chia train/streaming)
   ↓
5. Huấn luyện mô hình Random Forest
   ↓
6. Lưu mô hình vào models/house_price_model/
   ↓
7. Khởi động Spark Streaming job
   ↓
8. Producer gửi dữ liệu vào Kafka topic: house-prices-input
   ↓
9. Spark Streaming đọc từ Kafka, dự đoán, gửi kết quả vào house-prices-output
   ↓
10. Consumer đọc kết quả và hiển thị visualization
```

### 4.2. Luồng dữ liệu chi tiết

**Stage 1: Data Preparation**
```
California Housing Dataset
    ↓
prepare_data.py
    ↓
train_data.csv (80%)    streaming_data.csv (20%)
```

**Stage 2: Model Training**
```
train_data.csv
    ↓
Spark ML Pipeline
    ├── VectorAssembler
    └── RandomForestRegressor
    ↓
Trained Model (models/house_price_model/)
```

**Stage 3: Streaming Prediction**
```
streaming_data.csv
    ↓
kafka_producer.py
    ↓
Kafka Topic: house-prices-input
    ↓
Spark Streaming (streaming_predict.py)
    ├── Load Model
    ├── Parse JSON
    ├── Predict
    └── Calculate Metrics
    ↓
Kafka Topic: house-prices-output
    ↓
kafka_consumer.py
    ↓
Real-time Visualization
```

### 4.3. Data formats

**Input to Kafka (house-prices-input):**
```json
{
  "id": 0,
  "MedInc": 8.3252,
  "HouseAge": 41.0,
  "AveRooms": 6.984127,
  "AveBedrms": 1.023810,
  "Population": 322.0,
  "AveOccup": 2.555556,
  "Latitude": 37.88,
  "Longitude": -122.23,
  "actual_price": 4.526
}
```

**Output from Kafka (house-prices-output):**
```json
{
  "id": 0,
  "actual_price": 4.526,
  "predicted_price": 4.4523,
  "error": -0.0737,
  "error_percentage": -1.63
}
```

---

## 5. KẾT QUẢ VÀ ĐÁNH GIÁ

### 5.1. Metrics mô hình

Sau khi huấn luyện, mô hình Random Forest được đánh giá bằng các metrics:
- **RMSE**: Root Mean Squared Error (càng thấp càng tốt)
- **MAE**: Mean Absolute Error (càng thấp càng tốt)
- **R²**: Coefficient of Determination (càng gần 1 càng tốt)

### 5.2. Streaming performance

**Throughput:**
- Producer gửi với interval 1-2 giây/record
- Spark Streaming xử lý real-time với latency thấp
- Consumer hiển thị kết quả với update rate 1 giây

**Scalability:**
- Hệ thống hỗ trợ distributed processing
- Spark cluster có thể scale thêm workers
- Kafka hỗ trợ multiple partitions và replication

### 5.3. Visualization insights

**Biểu đồ hiển thị:**
- So sánh trực quan giữa giá thực tế và giá dự đoán
- Phân tích sai số dự đoán theo từng sample
- Metrics tổng hợp (MAE, RMSE) được cập nhật real-time

### 5.4. Độ tin cậy hệ thống

**Fault tolerance:**
- Kafka checkpoint đảm bảo exactly-once semantics
- Spark Streaming có khả năng recover từ checkpoint
- Airflow retry logic cho các tasks

**Error handling:**
- Producer có retry logic khi Kafka chưa sẵn sàng
- Spark Streaming có failOnDataLoss=false để tránh crash
- Airflow có cleanup task để dọn dẹp resources

---

## 6. KẾT LUẬN

### 6.1. Thành tựu đạt được

1. **Xây dựng thành công pipeline ML streaming end-to-end**
   - Từ chuẩn bị dữ liệu đến visualization
   - Tự động hóa hoàn toàn với Airflow

2. **Triển khai trên kiến trúc phân tán**
   - 3 máy riêng biệt cho các services
   - Distributed processing với Spark cluster

3. **Real-time prediction**
   - Streaming data processing với Spark Structured Streaming
   - Low latency prediction pipeline

4. **Visualization real-time**
   - Biểu đồ cập nhật liên tục
   - Metrics được tính toán và hiển thị động

### 6.2. Công nghệ sử dụng

- **Apache Spark 4.0.0**: Distributed computing và ML
- **Apache Kafka**: Message queue cho streaming
- **Apache Airflow**: Workflow orchestration
- **Python**: Ngôn ngữ lập trình chính
- **Matplotlib**: Visualization
- **Docker**: Containerization cho Kafka

### 6.3. Ứng dụng thực tế

Hệ thống này có thể được áp dụng cho:
- **Real-time price prediction**: Dự đoán giá nhà, giá cổ phiếu
- **IoT data processing**: Xử lý dữ liệu từ sensors
- **Recommendation systems**: Hệ thống gợi ý real-time
- **Fraud detection**: Phát hiện gian lận trong giao dịch

### 6.4. Hạn chế và cải thiện

**Hạn chế hiện tại:**
- Model được huấn luyện offline, chưa có online learning
- Visualization chỉ hiển thị trên local machine
- Chưa có monitoring và alerting system

**Hướng cải thiện:**
- Thêm model versioning và A/B testing
- Triển khai visualization trên web dashboard
- Tích hợp monitoring tools (Prometheus, Grafana)
- Thêm data validation và quality checks
- Implement model retraining pipeline tự động

### 6.5. Kết luận

Dự án đã thành công xây dựng một hệ thống Machine Learning streaming hoàn chỉnh với các công nghệ Big Data hiện đại. Hệ thống có khả năng xử lý dữ liệu real-time, scale được và có độ tin cậy cao. Đây là một foundation tốt để phát triển các ứng dụng ML production-ready.

---

## PHỤ LỤC

### A. Cấu trúc thư mục dự án

```
final_project/
├── dags/
│   └── ml_pipeline_dag.py          # Airflow DAG điều khiển toàn bộ
├── data/
│   ├── prepare_data.py              # Chia dữ liệu train/streaming
│   ├── train_data.csv               # Dữ liệu huấn luyện
│   └── streaming_data.csv          # Dữ liệu streaming
├── docker/
│   └── docker-compose.yml          # Kafka + Zookeeper (tham khảo)
├── spark_jobs/
│   ├── train_model.py               # Huấn luyện mô hình Spark ML
│   └── streaming_predict.py        # Spark Streaming dự đoán
├── streaming/
│   └── kafka_producer.py            # Mô phỏng streaming vào Kafka
├── visualization/
│   └── kafka_consumer.py            # Trực quan hóa kết quả
├── models/
│   └── house_price_model/           # Mô hình đã huấn luyện
├── requirements.txt                 # Python dependencies
└── README.md                        # Hướng dẫn sử dụng
```

### B. Dependencies chính

**Python packages:**
- `pyspark==4.0.0`
- `kafka-python`
- `pandas`
- `scikit-learn`
- `matplotlib`
- `apache-airflow==2.7.0`

**Spark packages:**
- `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0`

### C. Cấu hình hệ thống

**Kafka:**
- Bootstrap servers: `192.168.80.127:9092`
- Topics: `house-prices-input`, `house-prices-output`

**Spark:**
- Master: `spark://192.168.80.207:7077`
- Driver memory: 4GB
- Executor memory: 4GB
- Executors: 2
- Cores per executor: 2

**Airflow:**
- Web server port: 8080
- Default user: admin/admin

---

**Tài liệu được tạo bởi:** Hệ thống phân tích tự động  
**Ngày tạo:** 2025-12-15  
**Phiên bản:** 1.0

