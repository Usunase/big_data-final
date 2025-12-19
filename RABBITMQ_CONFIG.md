# Cấu Hình RabbitMQ - Setup với Hostname

## Tổng Quan

Hệ thống sử dụng **hostname** và **RabbitMQ** làm trung gian:
- **Máy Airflow**: RabbitMQ chạy local (`localhost:5672`)
- **Các máy khác**: Connect tới RabbitMQ qua **hostname** của máy Airflow (không dùng IP)
- **Không cần SSH**: Tất cả điều khiển qua Celery/RabbitMQ

## Cấu Hình

### 1. RabbitMQ Client (`utils/rabbitmq_client.py`)

```python
# Mặc định sử dụng localhost (cùng máy)
RabbitMQClient(host='localhost', port=5672)
```

**Không cần truyền host/port** khi gọi `get_rabbitmq_client()` vì đã có default:

```python
client = get_rabbitmq_client()  # Tự động dùng localhost:5672
```

### 2. Celery Worker (`mycelery/system_worker.py`)

```python
# Broker URL dùng localhost (chỉ khi chạy trên máy Airflow)
broker='amqp://guest:guest@localhost:5672//'
```

**Lưu ý**: Khi chạy worker trên máy Kafka/Spark, override bằng environment variable:
```bash
export CELERY_BROKER_URL="amqp://guest:guest@airflow-master:5672//"
```

### 3. Airflow DAGs

Trong các DAG (`train_dag.py`, `predict_dag.py`):

```python
# Khai báo biến (chỉ để check service)
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672

# Khi sử dụng, không cần truyền host/port
client = get_rabbitmq_client()  # Tự động dùng localhost
```

## Cài Đặt RabbitMQ

### Bước 1: Trên máy chủ Airflow (máy của bạn)

**Lấy hostname**:
```bash
hostname
# Output: haminhchien-Precision-5520 (hoặc airflow-master nếu đã đổi)
```

**Ghi lại hostname này** - các máy khác sẽ dùng để connect.

```bash
# Cài đặt RabbitMQ
sudo apt-get update
sudo apt-get install rabbitmq-server

# Khởi động RabbitMQ
sudo systemctl start rabbitmq-server
sudo systemctl enable rabbitmq-server

# Kiểm tra trạng thái
sudo systemctl status rabbitmq-server

# Hoặc dùng Docker
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

### Kiểm Tra Kết Nối

```bash
# Kiểm tra port đang lắng nghe
netstat -tuln | grep 5672
# hoặc
ss -tuln | grep 5672

# Test kết nối
telnet localhost 5672
```

### RabbitMQ Management UI

Truy cập: `http://localhost:15672`
- Username: `guest`
- Password: `guest`

**Kiểm tra connections**: Vào tab **Connections** để xem các máy Kafka/Spark đã kết nối chưa.

### Bước 2: Trên các máy Kafka/Spark

**Cấu hình hostname mapping**:

```bash
# Thêm hostname máy Airflow vào /etc/hosts
sudo nano /etc/hosts

# Thêm dòng (thay <IP_AIRFLOW> bằng IP thực)
<IP_AIRFLOW>  airflow-master
# Ví dụ:
192.168.1.7  airflow-master
```

**Test kết nối**:
```bash
telnet airflow-master 5672
ping airflow-master
```

**Chạy Celery worker**:
```bash
# Máy Kafka
export CELERY_BROKER_URL="amqp://guest:guest@airflow-master:5672//"
celery -A mycelery.system_worker worker -Q node_57 -n kafka@%h --loglevel=INFO

# Máy Spark
export CELERY_BROKER_URL="amqp://guest:guest@airflow-master:5672//"
celery -A mycelery.system_worker worker -Q spark -n spark@%h --loglevel=INFO
```

## Lưu Ý

1. **Không cần IP**: Vì RabbitMQ chạy trên cùng máy, luôn dùng `localhost` hoặc `127.0.0.1`

2. **Firewall**: Không cần mở port 5672 ra ngoài vì chỉ dùng local

3. **Security**: Nếu cần bảo mật hơn, có thể:
   - Tạo user mới thay vì dùng `guest`
   - Đổi password mặc định
   - Chỉ cho phép kết nối từ localhost

4. **Default Credentials**:
   - Username: `guest`
   - Password: `guest`
   - Port: `5672` (AMQP)
   - Management UI: `15672`

## Troubleshooting

### Lỗi: Connection refused

```bash
# Kiểm tra RabbitMQ đang chạy
sudo systemctl status rabbitmq-server

# Khởi động lại nếu cần
sudo systemctl restart rabbitmq-server
```

### Lỗi: Cannot connect to RabbitMQ

```bash
# Kiểm tra log
sudo journalctl -u rabbitmq-server -n 50

# Kiểm tra port
sudo lsof -i :5672
```

### Test kết nối từ Python

```python
from utils.rabbitmq_client import get_rabbitmq_client

client = get_rabbitmq_client()
if client.connect():
    print("✓ Kết nối thành công!")
    client.close()
else:
    print("❌ Không thể kết nối")
```


