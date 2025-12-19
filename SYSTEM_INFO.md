# Thông Tin Hệ Thống

## Hostname Máy Airflow

**Hostname hiện tại**: `haminhchien-Precision-5520`

**IP máy Airflow**: `192.168.80.147`

**Khuyến nghị**: Đổi hostname ngắn gọn hơn (tùy chọn):
```bash
sudo hostnamectl set-hostname airflow-master
```

## Cấu Hình Các Máy

### Máy Airflow (192.168.80.147)
- **Hostname**: `haminhchien-Precision-5520` (hoặc `airflow-master`)
- **Services**: 
  - RabbitMQ (localhost:5672)
  - Airflow (localhost:8080)
- **Role**: Orchestrator, không cần biết IP các máy khác

### Máy Kafka (192.168.80.127)
- **Queue**: `node_57`
- **Connect tới**: `airflow-master:5672` (qua hostname)
- **Services**: Kafka cluster (Docker)

### Máy Spark (192.168.80.207)
- **Queue**: `spark`
- **Connect tới**: `airflow-master:5672` (qua hostname)
- **Services**: Spark Master + Worker

### Máy Hadoop (192.168.80.127)
- **Services**: HDFS NameNode + DataNode
- **HDFS Namenode**: `hdfs://192.168.80.127:9000`

## Quick Commands

### Trên Máy Airflow

```bash
# Lấy hostname
hostname

# Lấy IP
hostname -I | awk '{print $1}'

# Start RabbitMQ
sudo systemctl start rabbitmq-server

# Kiểm tra RabbitMQ
sudo systemctl status rabbitmq-server
```

### Trên Máy Kafka/Spark

```bash
# Setup hostname mapping
sudo ./scripts/setup_hosts.sh 192.168.80.147 airflow-master

# Test kết nối
ping airflow-master
telnet airflow-master 5672

# Start worker
# Máy Kafka:
./scripts/start_kafka_worker.sh airflow-master

# Máy Spark:
./scripts/start_spark_worker.sh airflow-master
```

## RabbitMQ Management UI

- **URL**: http://localhost:15672 (trên máy Airflow)
- **Username**: `guest`
- **Password**: `guest`

Kiểm tra:
- **Connections**: Xem các máy đã kết nối
- **Queues**: Xem các queues (`spark`, `node_57`, `training_status`, `prediction_status`)

## Files Quan Trọng

- `SETUP_GUIDE.md`: Hướng dẫn setup chi tiết
- `QUICK_START.md`: Quick start guide
- `scripts/setup_hosts.sh`: Script setup /etc/hosts
- `scripts/start_kafka_worker.sh`: Script start Kafka worker
- `scripts/start_spark_worker.sh`: Script start Spark worker

