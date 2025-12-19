# Hướng Dẫn Hệ Thống với Hadoop và RabbitMQ

## Tổng Quan

Hệ thống đã được cập nhật để sử dụng:
- **Hadoop HDFS**: Lưu trữ dữ liệu train và model
- **RabbitMQ**: Giao tiếp giữa các service (dùng hostname, không dùng IP)
- **2 DAG riêng biệt**: Train và Predict
- **Không dùng SSH**: Tất cả điều khiển qua RabbitMQ/Celery

## Kiến Trúc Hệ Thống

```
┌─────────────────────────────────────────────────────────────┐
│ Máy Airflow (hostname: airflow-master)                     │
│ - Airflow Orchestrator                                      │
│ - RabbitMQ Server (localhost:5672)                          │
│ - Chỉ biết: queue names (spark, node_57)                   │
└─────────────────────────────────────────────────────────────┘
                    │
        ┌───────────┼───────────┐
        │           │           │
        ▼           ▼           ▼
┌──────────┐ ┌──────────┐ ┌──────────┐
│  Hadoop  │ │  Kafka   │ │  Spark   │
│ (127)    │ │ (127)    │ │ (207)    │
│ HDFS     │ │ Queue:   │ │ Queue:   │
│          │ │ node_57  │ │ spark    │
└──────────┘ └──────────┘ └──────────┘
     │            │            │
     └────────────┴────────────┘
              │
              ▼
    Connect tới RabbitMQ qua hostname
    (airflow-master:5672)
```

**Đặc điểm**:
- Máy Airflow **không cần biết IP** của các máy khác
- Các máy khác chỉ cần biết **hostname** của máy Airflow
- RabbitMQ làm trung gian kết nối

## Cài Đặt

**Xem file `SETUP_GUIDE.md` để hướng dẫn chi tiết từng bước.**

### Tóm Tắt Nhanh

### 1. Máy Airflow

```bash
# Cài RabbitMQ
sudo apt-get install rabbitmq-server -y
sudo systemctl start rabbitmq-server

# Lấy hostname
hostname
# Ghi lại hostname này (ví dụ: airflow-master)

# Cài dependencies
pip install -r requirements.txt
```

### 2. Máy Kafka & Spark

Trên mỗi máy:

```bash
# 1. Thêm hostname máy Airflow vào /etc/hosts
sudo nano /etc/hosts
# Thêm: <IP_AIRFLOW>  airflow-master

# 2. Cài dependencies
pip install celery pika

# 3. Chạy Celery worker
# Máy Kafka:
export CELERY_BROKER_URL="amqp://guest:guest@airflow-master:5672//"
celery -A mycelery.system_worker worker -Q node_57 -n kafka@%h --loglevel=INFO

# Máy Spark:
export CELERY_BROKER_URL="amqp://guest:guest@airflow-master:5672//"
celery -A mycelery.system_worker worker -Q spark -n spark@%h --loglevel=INFO
```

### 3. Máy Hadoop

```bash
cd ~/hadoop/sbin
./start-dfs.sh
hdfs dfsadmin -report
```

## Sử Dụng

### DAG 1: Train Model Pipeline

**Tên DAG**: `train_model_pipeline`

**Luồng xử lý**:
1. Kiểm tra các service (RabbitMQ, Hadoop, Spark)
2. Chuẩn bị dữ liệu local
3. Upload dữ liệu lên HDFS
4. Gửi message bắt đầu training qua RabbitMQ
5. Huấn luyện mô hình trên Spark (đọc từ HDFS, lưu model lên HDFS)
6. Gửi message hoàn thành training qua RabbitMQ

**Chạy DAG**:
```bash
# Từ Airflow UI, trigger DAG: train_model_pipeline
```

### DAG 2: Predict Streaming Pipeline

**Tên DAG**: `predict_streaming_pipeline`

**Luồng xử lý**:
1. Kiểm tra các service (RabbitMQ, Kafka, Spark)
2. Khởi động Kafka cluster
3. Tạo Kafka topics
4. Khởi động Spark cluster
5. Gửi message bắt đầu prediction qua RabbitMQ
6. Gửi dữ liệu streaming vào Kafka
7. Spark Streaming đọc từ Kafka, load model từ HDFS, dự đoán
8. Gửi kết quả vào Kafka output topic
9. Gửi message hoàn thành prediction qua RabbitMQ

**Chạy DAG**:
```bash
# Từ Airflow UI, trigger DAG: predict_streaming_pipeline
```

## Cấu Hình HDFS

### Upload Dữ Liệu Lên HDFS

```bash
cd /home/haminhchien/Documents/bigdata/final_project
python data/upload_to_hdfs.py
```

Script này sẽ:
- Kiểm tra HDFS sẵn sàng
- Upload `train_data.csv` lên `/bigdata/house_prices/train_data.csv`
- Upload `streaming_data.csv` lên `/bigdata/house_prices/streaming_data.csv`

### Kiểm Tra Dữ Liệu Trên HDFS

```bash
# List files
hdfs dfs -ls /bigdata/house_prices

# Xem nội dung
hdfs dfs -cat /bigdata/house_prices/train_data.csv | head -10
```

## RabbitMQ Queues

Hệ thống sử dụng các queues sau:

1. **training_status**: Thông báo trạng thái training
   - Message khi bắt đầu: `{'status': 'started', 'timestamp': '...'}`
   - Message khi hoàn thành: `{'status': 'completed', 'timestamp': '...', 'model_path': '...'}`

2. **prediction_status**: Thông báo trạng thái prediction
   - Message khi bắt đầu: `{'status': 'started', 'timestamp': '...'}`
   - Message khi hoàn thành: `{'status': 'completed', 'timestamp': '...'}`

### Xem Messages trong RabbitMQ

```bash
# Truy cập RabbitMQ Management UI
http://192.168.80.147:15672
# Username: guest
# Password: guest
```

## Environment Variables

Các biến môi trường có thể được set:

```bash
export HDFS_NAMENODE="hdfs://192.168.80.148:9000"
export HDFS_DATA_DIR="/bigdata/house_prices"
export HDFS_MODEL_DIR="/bigdata/house_prices/models"
export KAFKA_BOOTSTRAP_SERVERS="192.168.80.127:9092"
```

## Troubleshooting

### Lỗi kết nối HDFS

```bash
# Kiểm tra HDFS đang chạy
hdfs dfsadmin -report

# Kiểm tra firewall
telnet 192.168.80.148 9000
```

### Lỗi kết nối RabbitMQ

```bash
# Kiểm tra RabbitMQ
sudo systemctl status rabbitmq-server

# Kiểm tra port
telnet 192.168.80.147 5672
```

### Lỗi Spark không đọc được từ HDFS

Đảm bảo Spark có cấu hình đúng:
```python
.config("spark.hadoop.fs.defaultFS", "hdfs://192.168.80.148:9000")
.config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
```

## Files Đã Thay Đổi

1. **data/upload_to_hdfs.py**: Script upload data lên HDFS
2. **spark_jobs/train_model.py**: Đọc từ HDFS, lưu model lên HDFS
3. **spark_jobs/streaming_predict.py**: Đọc model từ HDFS
4. **dags/train_dag.py**: DAG cho training với RabbitMQ
5. **dags/predict_dag.py**: DAG cho prediction với RabbitMQ
6. **utils/rabbitmq_client.py**: Client để giao tiếp với RabbitMQ
7. **mycelery/system_worker.py**: Cập nhật broker từ Redis sang RabbitMQ
8. **requirements.txt**: Thêm pika (RabbitMQ client)

## Lưu Ý Quan Trọng

- **Không dùng IP trong code**: Tất cả kết nối qua hostname và RabbitMQ
- **Không cần SSH**: Tất cả điều khiển qua Celery/RabbitMQ
- **Hostname mapping**: Các máy Kafka/Spark cần thêm hostname máy Airflow vào `/etc/hosts`
- **RabbitMQ là trung gian**: Máy Airflow chỉ biết queue names, không cần biết IP các máy khác
- Đảm bảo tất cả các máy có thể truy cập lẫn nhau qua network
- Hadoop, Kafka, Spark đã được cài đặt và cấu hình đúng
- RabbitMQ đang chạy trên máy Airflow và accessible từ các máy khác (port 5672)

## Xem Thêm

- **SETUP_GUIDE.md**: Hướng dẫn setup chi tiết từng bước
- **RABBITMQ_CONFIG.md**: Cấu hình RabbitMQ chi tiết

