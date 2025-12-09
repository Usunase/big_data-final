# ML Streaming Pipeline với Spark, Kafka và Airflow

Dự án này triển khai một pipeline học máy end-to-end sử dụng Spark ML, Kafka và Airflow để:
- Huấn luyện mô hình dự đoán giá nhà
- Streaming dữ liệu qua Kafka
- Dự đoán real-time với Spark Streaming
- Trực quan hóa kết quả

## 📋 Yêu cầu hệ thống

- Python 3.9+
- Spark 4.0.0
- Kafka 3.8.0 (chạy qua Docker)
- Docker và Docker Compose
- Airflow (để điều khiển pipeline)

## 🚀 Cài đặt

### 1. Cài đặt Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Cài đặt Spark 4.0.0

Tải và cài đặt Spark 4.0.0 từ [Apache Spark Downloads](https://spark.apache.org/downloads.html)

```bash
# Ví dụ trên Linux
wget https://archive.apache.org/dist/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz
tar -xzf spark-4.0.0-bin-hadoop3.tgz
export SPARK_HOME=/path/to/spark-4.0.0-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
```

### 3. Cài đặt Airflow

```bash
# Tạo virtual environment (khuyến nghị)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# hoặc venv\Scripts\activate  # Windows

# Cài đặt Airflow
pip install apache-airflow==2.7.0
pip install apache-airflow-providers-apache-spark==4.0.0

# Khởi tạo Airflow database
airflow db init

# Tạo user admin
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

## 📁 Cấu trúc dự án

```
final_project/
├── dags/
│   └── ml_pipeline_dag.py          # Airflow DAG điều khiển toàn bộ
├── data/
│   └── prepare_data.py              # Chia dữ liệu train/streaming
├── docker/
│   └── docker-compose.yml          # Kafka + Zookeeper
├── spark_jobs/
│   ├── train_model.py               # Huấn luyện mô hình Spark ML
│   └── streaming_predict.py        # Spark Streaming dự đoán
├── streaming/
│   └── kafka_producer.py            # Mô phỏng streaming vào Kafka
├── visualization/
│   └── kafka_consumer.py            # Trực quan hóa kết quả
├── requirements.txt                 # Python dependencies
└── README.md                        # File này
```

## 🎯 Cách chạy

### Phương pháp 1: Chạy qua Airflow (Khuyến nghị)

#### Bước 1: Khởi động Airflow

```bash
# Terminal 1: Khởi động Airflow webserver
airflow webserver --port 8080

# Terminal 2: Khởi động Airflow scheduler
airflow scheduler
```

Truy cập Airflow UI: http://localhost:8080
- Username: admin
- Password: admin

#### Bước 2: Cấu hình Spark connection trong Airflow

1. Vào Airflow UI → Admin → Connections
2. Tìm hoặc tạo connection với ID: `spark_default`
3. Cấu hình:
   - Conn Type: `Spark`
   - Host: `local[*]` (hoặc Spark master URL của bạn)
   - Port: `7077` (nếu dùng Spark standalone)
   - Extra: `{"queue": "default"}`

#### Bước 3: Chạy DAG

1. Vào Airflow UI → DAGs
2. Tìm DAG `ml_streaming_pipeline`
3. Bật DAG (toggle switch)
4. Click "Trigger DAG" để chạy

DAG sẽ tự động:
- ✅ Khởi động Kafka (Docker)
- ✅ Kiểm tra Kafka sẵn sàng
- ✅ Chuẩn bị dữ liệu
- ✅ Huấn luyện mô hình Spark ML
- ✅ Khởi động Spark Streaming job
- ✅ Gửi dữ liệu streaming vào Kafka
- ✅ Đợi xử lý hoàn thành
- ✅ Dọn dẹp

#### Bước 4: Chạy Visualization (tùy chọn)

1. Tìm DAG `ml_streaming_visualization`
2. Trigger DAG để chạy consumer và hiển thị biểu đồ

---

### Phương pháp 2: Chạy thủ công từng bước

#### Bước 1: Khởi động Kafka

```bash
cd docker
docker-compose up -d
```

Kiểm tra Kafka đã chạy:
```bash
docker ps
```

#### Bước 2: Chuẩn bị dữ liệu

```bash
cd /home/haminhchien/Documents/bigdata/final_project
python data/prepare_data.py
```

Kết quả:
- `data/train_data.csv` - Dữ liệu huấn luyện
- `data/streaming_data.csv` - Dữ liệu streaming

#### Bước 3: Huấn luyện mô hình

```bash
spark-submit \
    --master local[*] \
    --driver-memory 4g \
    --executor-memory 4g \
    spark_jobs/train_model.py
```

Mô hình sẽ được lưu vào: `models/house_price_model/`

#### Bước 4: Khởi động Spark Streaming job

```bash
spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
    --driver-memory 4g \
    --executor-memory 4g \
    spark_jobs/streaming_predict.py
```

Job này sẽ chạy liên tục, đọc từ Kafka topic `house-prices-input` và gửi kết quả vào `house-prices-output`.

#### Bước 5: Gửi dữ liệu streaming (Terminal mới)

```bash
python streaming/kafka_producer.py 1 200
```

Tham số:
- `1`: Khoảng thời gian giữa các message (giây)
- `200`: Số lượng records gửi (None = tất cả)

#### Bước 6: Trực quan hóa kết quả (Terminal mới)

```bash
python visualization/kafka_consumer.py
```

Sẽ hiển thị biểu đồ real-time so sánh giá thực tế vs dự đoán.

---

## 🔧 Cấu hình

### Thay đổi đường dẫn project trong DAG

Nếu project path khác, sửa trong `dags/ml_pipeline_dag.py`:

```python
params={'project_dir': '/your/project/path'}
```

### Thay đổi cấu hình Spark

Sửa memory và cores trong:
- `dags/ml_pipeline_dag.py` (task `train_model` và `start_streaming_job`)
- `spark_jobs/train_model.py`
- `spark_jobs/streaming_predict.py`

### Thay đổi Kafka settings

Sửa trong `docker/docker-compose.yml`:
- Ports
- Memory limits
- Topic replication factor

---

## 🐛 Troubleshooting

### Lỗi: Kafka không kết nối được

```bash
# Kiểm tra Kafka đang chạy
docker ps | grep kafka

# Xem logs
docker logs kafka

# Restart Kafka
cd docker
docker-compose restart
```

### Lỗi: Spark không tìm thấy package

Đảm bảo đã cài đúng version:
```bash
pip install pyspark==4.0.0
```

Và package spark-sql-kafka đúng version:
```
org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0
```

### Lỗi: Airflow không submit được Spark job

1. Kiểm tra Spark connection trong Airflow UI
2. Đảm bảo `SPARK_HOME` được set trong environment
3. Kiểm tra Airflow có quyền truy cập Spark

### Lỗi: Model không tìm thấy

Đảm bảo đã chạy `train_model.py` trước khi chạy streaming:
```bash
ls models/house_price_model/
```

---

## 📊 Kết quả mong đợi

### Sau khi huấn luyện:
- Mô hình được lưu trong `models/house_price_model/`
- Metrics: RMSE, MAE, R² được in ra console

### Sau khi streaming:
- Dữ liệu được gửi vào Kafka topic `house-prices-input`
- Spark xử lý và gửi kết quả vào `house-prices-output`
- Biểu đồ hiển thị so sánh actual vs predicted prices

---

## 📝 Notes

- Spark 4.0.0 yêu cầu Scala 2.13
- Kafka 3.8.0 tương thích với Spark 4.0.0
- Tất cả dependencies phải tương thích với Scala 2.13

---

## 👤 Tác giả

Final Project - Big Data

# big_data-final
