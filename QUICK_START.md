# Quick Start Guide - Hệ Thống Phân Tán với Hostname

## Tổng Quan

Hệ thống sử dụng **hostname** và **RabbitMQ** để kết nối các máy, **không dùng IP trong code**.

## Kiến Trúc

- **Máy Airflow**: Chạy RabbitMQ + Airflow (hostname: `haminhchien-Precision-5520` hoặc `airflow-master`)
- **Máy Kafka** (192.168.80.127): Celery worker queue `node_57`
- **Máy Spark** (192.168.80.207): Celery worker queue `spark`
- **Máy Hadoop** (192.168.80.127): HDFS

## Setup Nhanh (3 Bước)

### Bước 1: Máy Airflow

```bash
# 1. Lấy hostname
hostname
# Ghi lại: haminhchien-Precision-5520 (hoặc đổi thành airflow-master)

# 2. Cài RabbitMQ
sudo apt-get install rabbitmq-server -y
sudo systemctl start rabbitmq-server

# 3. Cài dependencies
cd /home/haminhchien/Documents/bigdata/final_project
pip install -r requirements.txt

# 4. Mở firewall (nếu cần)
sudo ufw allow 5672/tcp
```

### Bước 2: Máy Kafka & Spark

**Trên mỗi máy**:

```bash
# 1. Setup hostname mapping
# Thay <IP_AIRFLOW> bằng IP thực của máy Airflow
sudo bash -c 'echo "<IP_AIRFLOW>  airflow-master" >> /etc/hosts'

# Hoặc dùng script helper
./scripts/setup_hosts.sh <IP_AIRFLOW> airflow-master

# 2. Test kết nối
ping airflow-master
telnet airflow-master 5672

# 3. Cài dependencies
pip install celery pika

# 4. Start worker
# Máy Kafka:
./scripts/start_kafka_worker.sh airflow-master

# Máy Spark:
./scripts/start_spark_worker.sh airflow-master
```

### Bước 3: Máy Hadoop

```bash
cd ~/hadoop/sbin
./start-dfs.sh
hdfs dfsadmin -report
```

## Kiểm Tra

### Trên Máy Airflow

```bash
# Kiểm tra RabbitMQ
sudo systemctl status rabbitmq-server

# Kiểm tra connections trong RabbitMQ UI
# http://localhost:15672
# Vào tab Connections → thấy 2 connections (Kafka + Spark)
```

### Trên Máy Kafka/Spark

```bash
# Kiểm tra worker đang chạy
ps aux | grep celery
```

## Chạy DAGs

1. Truy cập Airflow UI: http://localhost:8080
2. Trigger DAG: `train_model_pipeline` (training)
3. Trigger DAG: `predict_streaming_pipeline` (prediction)

## Xem Chi Tiết

- **SETUP_GUIDE.md**: Hướng dẫn setup chi tiết từng bước
- **README_HADOOP_RABBITMQ.md**: Tổng quan hệ thống
- **RABBITMQ_CONFIG.md**: Cấu hình RabbitMQ

## Troubleshooting

### Không kết nối được RabbitMQ

```bash
# Trên máy Kafka/Spark
# 1. Kiểm tra /etc/hosts
cat /etc/hosts | grep airflow-master

# 2. Test kết nối
telnet airflow-master 5672

# 3. Kiểm tra firewall trên máy Airflow
sudo ufw status
```

### Worker không nhận task

```bash
# Kiểm tra broker URL
echo $CELERY_BROKER_URL

# Kiểm tra worker đang chạy đúng queue
ps aux | grep celery
```

