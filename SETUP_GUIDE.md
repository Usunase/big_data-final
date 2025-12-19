# Hướng Dẫn Setup Hệ Thống Phân Tán với Hostname và RabbitMQ

## Tổng Quan

Hệ thống sử dụng **hostname** và **RabbitMQ** làm trung gian để kết nối các máy, **không cần dùng IP trong code**. Máy Airflow chỉ cần biết hostname của chính nó, các máy khác sẽ connect tới RabbitMQ qua hostname này.

## Kiến Trúc

```
┌─────────────────────────────────────────────────────────────┐
│ Máy Airflow (haminhchien-Precision-5520)                    │
│ Hostname: airflow-master (hoặc hostname hiện tại)            │
│ - Airflow Orchestrator                                      │
│ - RabbitMQ Server (localhost:5672)                          │
│ - Chỉ biết: queue names (spark, node_57)                    │
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

## Bước 1: Setup Máy Airflow (Máy Chủ)

### 1.1. Lấy Hostname

```bash
hostname
# Output: haminhchien-Precision-5520
```

**Ghi lại hostname này** - các máy khác sẽ dùng để connect tới RabbitMQ.

**Khuyến nghị**: Đặt hostname ngắn gọn hơn (tùy chọn):

```bash
sudo hostnamectl set-hostname airflow-master
# hoặc giữ nguyên hostname hiện tại
```

### 1.2. Cài Đặt RabbitMQ

```bash
# Cài đặt RabbitMQ
sudo apt-get update
sudo apt-get install rabbitmq-server -y

# Khởi động và enable
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server
sudo systemctl status rabbitmq-server

# Hoặc dùng Docker
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

### 1.3. Kiểm Tra RabbitMQ

```bash
# Kiểm tra port đang lắng nghe
sudo netstat -tuln | grep 5672
# hoặc
ss -tuln | grep 5672

# Test kết nối local
telnet localhost 5672

# Truy cập Management UI
# http://localhost:15672
# Username: guest
# Password: guest
```

### 1.4. Cài Đặt Dependencies

```bash
cd /home/haminhchien/Documents/bigdata/final_project
pip install -r requirements.txt
```

### 1.5. Cấu Hình Firewall (Nếu Cần)

Đảm bảo port 5672 (RabbitMQ) có thể truy cập từ các máy khác:

```bash
# Nếu dùng ufw
sudo ufw allow 5672/tcp
sudo ufw allow 15672/tcp  # Management UI (optional)

# Kiểm tra
sudo ufw status
```

## Bước 2: Setup Máy Kafka (192.168.80.127)

### 2.1. Cấu Hình Hostname Mapping

Thêm hostname của máy Airflow vào `/etc/hosts`:

```bash
sudo nano /etc/hosts

# Thêm dòng sau (thay <IP_AIRFLOW> bằng IP thực của máy Airflow)
<IP_AIRFLOW>  airflow-master
# Ví dụ:
192.168.1.7  airflow-master
```

**Lưu ý**: Thay `<IP_AIRFLOW>` bằng IP thực của máy Airflow. Bạn có thể tìm IP bằng:

```bash
# Trên máy Airflow
hostname -I | awk '{print $1}'
```

### 2.2. Test Kết Nối Tới RabbitMQ

```bash
# Test kết nối bằng hostname
telnet airflow-master 5672

# Hoặc dùng ping
ping airflow-master
```

### 2.3. Cài Đặt Python Dependencies

```bash
# Cài đặt Celery và dependencies
pip install celery pika

# Copy thư mục mycelery từ máy Airflow hoặc clone project
# Đảm bảo có file: mycelery/system_worker.py
```

### 2.4. Chạy Celery Worker cho Kafka

```bash
cd /path/to/project

# Set broker URL dùng hostname (KHÔNG dùng IP)
export CELERY_BROKER_URL="amqp://guest:guest@airflow-master:5672//"

# Chạy worker cho queue node_57
celery -A mycelery.system_worker worker \
  -Q node_57 \
  -n kafka@%h \
  --loglevel=INFO \
  --broker="$CELERY_BROKER_URL"
```

**Hoặc tạo file `start_kafka_worker.sh`**:

```bash
#!/bin/bash
export CELERY_BROKER_URL="amqp://guest:guest@airflow-master:5672//"
cd /path/to/project
celery -A mycelery.system_worker worker \
  -Q node_57 \
  -n kafka@%h \
  --loglevel=INFO \
  --broker="$CELERY_BROKER_URL"
```

Chạy:
```bash
chmod +x start_kafka_worker.sh
./start_kafka_worker.sh
```

### 2.5. Kiểm Tra Worker Đã Kết Nối

Trên máy Airflow, truy cập RabbitMQ Management UI:
- http://localhost:15672
- Vào tab **Queues** → tìm queue `node_57`
- Vào tab **Connections** → thấy connection từ máy Kafka

## Bước 3: Setup Máy Spark (192.168.80.207)

### 3.1. Cấu Hình Hostname Mapping

```bash
sudo nano /etc/hosts

# Thêm dòng sau
<IP_AIRFLOW>  airflow-master
# Ví dụ:
192.168.1.7  airflow-master
```

### 3.2. Test Kết Nối

```bash
telnet airflow-master 5672
ping airflow-master
```

### 3.3. Cài Đặt Python Dependencies

```bash
pip install celery pika
# Copy mycelery từ máy Airflow
```

### 3.4. Chạy Celery Worker cho Spark

```bash
cd /path/to/project

export CELERY_BROKER_URL="amqp://guest:guest@airflow-master:5672//"

celery -A mycelery.system_worker worker \
  -Q spark \
  -n spark@%h \
  --loglevel=INFO \
  --broker="$CELERY_BROKER_URL"
```

**Hoặc tạo file `start_spark_worker.sh`**:

```bash
#!/bin/bash
export CELERY_BROKER_URL="amqp://guest:guest@airflow-master:5672//"
cd /path/to/project
celery -A mycelery.system_worker worker \
  -Q spark \
  -n spark@%h \
  --loglevel=INFO \
  --broker="$CELERY_BROKER_URL"
```

## Bước 4: Setup Máy Hadoop (192.168.80.127)

### 4.1. Khởi Động HDFS

```bash
cd ~/hadoop/sbin
./start-dfs.sh

# Kiểm tra
hdfs dfsadmin -report
```

### 4.2. Tạo Thư Mục HDFS (Nếu Cần)

```bash
hdfs dfs -mkdir -p /bigdata/house_prices
hdfs dfs -chmod 777 /bigdata/house_prices
```

## Bước 5: Kiểm Tra Toàn Bộ Hệ Thống

### 5.1. Trên Máy Airflow

```bash
# Kiểm tra RabbitMQ
sudo systemctl status rabbitmq-server

# Kiểm tra Airflow
airflow info

# Xem DAGs
airflow dags list | grep -E "train|predict"
```

### 5.2. Trên Máy Kafka

```bash
# Kiểm tra Celery worker đang chạy
ps aux | grep celery

# Xem log worker
# (nếu chạy foreground, sẽ thấy log real-time)
```

### 5.3. Trên Máy Spark

```bash
# Kiểm tra Celery worker đang chạy
ps aux | grep celery
```

### 5.4. Kiểm Tra RabbitMQ Connections

Truy cập http://localhost:15672 trên máy Airflow:
- **Connections**: Thấy 2 connections (từ Kafka và Spark)
- **Queues**: Thấy queues `spark`, `node_57`, `training_status`, `prediction_status`

## Bước 6: Chạy DAGs

### 6.1. Train Model Pipeline

1. Truy cập Airflow UI: http://localhost:8080
2. Tìm DAG: `train_model_pipeline`
3. Trigger DAG
4. Theo dõi logs để xem:
   - Upload data lên HDFS
   - Training model trên Spark
   - Lưu model lên HDFS

### 6.2. Predict Streaming Pipeline

1. **Đảm bảo**:
   - Celery workers đang chạy trên Kafka và Spark
   - HDFS đang chạy
   
2. Trigger DAG: `predict_streaming_pipeline`

3. Theo dõi:
   - Kafka cluster được start qua Celery
   - Spark cluster được start qua Celery
   - Streaming prediction chạy

## Troubleshooting

### Lỗi: Cannot connect to RabbitMQ từ máy khác

```bash
# Trên máy Kafka/Spark, test kết nối
telnet airflow-master 5672

# Nếu không kết nối được:
# 1. Kiểm tra /etc/hosts đã đúng chưa
cat /etc/hosts | grep airflow-master

# 2. Kiểm tra firewall trên máy Airflow
sudo ufw status

# 3. Kiểm tra RabbitMQ đang lắng nghe trên interface nào
sudo netstat -tuln | grep 5672
# Nếu chỉ thấy 127.0.0.1:5672, cần config RabbitMQ listen trên 0.0.0.0
```

### Lỗi: Celery worker không nhận task

```bash
# Kiểm tra worker đang chạy đúng queue
ps aux | grep celery

# Kiểm tra broker URL
echo $CELERY_BROKER_URL

# Xem log worker để debug
# (nếu chạy foreground sẽ thấy log)
```

### Lỗi: Hostname không resolve

```bash
# Test hostname resolution
nslookup airflow-master
# hoặc
getent hosts airflow-master

# Nếu không resolve, kiểm tra /etc/hosts
cat /etc/hosts
```

## Lưu Ý Quan Trọng

1. **Hostname vs IP**: 
   - Trong **code**: Không dùng IP, chỉ dùng `localhost` (máy Airflow) hoặc hostname
   - Trong **cấu hình hệ thống** (`/etc/hosts`, `CELERY_BROKER_URL`): Dùng hostname thay vì IP

2. **RabbitMQ là trung gian**:
   - Máy Airflow chỉ biết queue names (`spark`, `node_57`)
   - Máy Kafka/Spark chỉ biết hostname của máy Airflow (`airflow-master`)
   - Không cần biết IP của nhau

3. **Network Requirements**:
   - Các máy phải trong cùng network hoặc có thể route tới nhau
   - Port 5672 (RabbitMQ) phải accessible từ các máy khác

4. **Security**:
   - Có thể tạo user RabbitMQ riêng thay vì dùng `guest`
   - Có thể dùng SSL/TLS cho RabbitMQ (advanced)

## Files Quan Trọng

- `mycelery/system_worker.py`: Celery tasks (chạy trên tất cả máy)
- `dags/train_dag.py`: DAG training
- `dags/predict_dag.py`: DAG prediction
- `utils/rabbitmq_client.py`: RabbitMQ client (chỉ máy Airflow)
- `SETUP_GUIDE.md`: File này

## Quick Start Commands

### Máy Airflow
```bash
# Start RabbitMQ
sudo systemctl start rabbitmq-server

# Start Airflow (nếu chưa chạy)
airflow webserver -p 8080 &
airflow scheduler &
```

### Máy Kafka
```bash
export CELERY_BROKER_URL="amqp://guest:guest@airflow-master:5672//"
celery -A mycelery.system_worker worker -Q node_57 -n kafka@%h --loglevel=INFO
```

### Máy Spark
```bash
export CELERY_BROKER_URL="amqp://guest:guest@airflow-master:5672//"
celery -A mycelery.system_worker worker -Q spark -n spark@%h --loglevel=INFO
```

