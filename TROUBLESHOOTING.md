# Troubleshooting Guide - Sửa lỗi DAG

## Lỗi: Task `start_kafka` Failed

### Nguyên nhân
- DAG đang dùng `docker-compose` (v1) gây xung đột với Docker API
- Lỗi: `docker.errors.DockerException: Error while fetching server API version: Not supported URL scheme http+docker`

### Giải pháp đã áp dụng
✅ Đã sửa DAG để dùng `docker compose` (v2) thay vì `docker-compose`

### File đã sửa
- `dags/ml_pipeline_dag.py` - Task `start_kafka`

### Thay đổi
```python
# Trước (SAI):
docker-compose down && docker-compose up -d

# Sau (ĐÚNG):
docker compose down && docker compose up -d
```

---

## Các lỗi khác có thể gặp

### 1. DAG không hiển thị trong Airflow UI

**Nguyên nhân:**
- DAG file không ở đúng thư mục Airflow tìm kiếm
- DAG có lỗi syntax

**Giải pháp:**
```bash
# Tạo symlink
mkdir -p ~/airflow/dags
ln -sf /home/haminhchien/Documents/bigdata/final_project/dags/ml_pipeline_dag.py \
    ~/airflow/dags/ml_pipeline_dag.py

# Kiểm tra syntax
/home/haminhchien/airflow/venv/bin/python3 -c \
    "import sys; sys.path.insert(0, '/home/haminhchien/airflow/dags'); \
    from ml_pipeline_dag import dag; print('OK')"
```

### 2. Port đã được sử dụng (2181, 9092)

**Nguyên nhân:**
- Kafka/Zookeeper cũ đang chạy

**Giải pháp:**
```bash
# Dừng containers cũ
docker compose -f /home/haminhchien/Documents/bigdata/final_project/docker/docker-compose.yml down

# Hoặc kill process đang dùng port
fuser 2181/tcp -k
fuser 9092/tcp -k
```

### 3. Spark connection không tìm thấy

**Nguyên nhân:**
- Connection ID sai hoặc chưa tạo

**Giải pháp:**
- Vào Airflow UI → Admin → Connections
- Tạo connection với ID: `spark_default`
- Type: `Spark`
- Host: `local[*]` (hoặc Spark master URL)

### 4. Model không tìm thấy

**Nguyên nhân:**
- Task `train_model` chưa chạy thành công

**Giải pháp:**
- Chạy task `train_model` trước
- Kiểm tra file: `models/house_price_model/metadata/part-00000`

### 5. Kafka không kết nối được

**Nguyên nhân:**
- Kafka chưa khởi động hoàn toàn
- Network issue

**Giểm tra:**
```bash
# Kiểm tra Kafka đang chạy
docker ps | grep kafka

# Kiểm tra logs
docker logs kafka | tail -20

# Test kết nối
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## Checklist trước khi chạy DAG

- [ ] Kafka và Zookeeper đang chạy (hoặc DAG sẽ tự khởi động)
- [ ] DAG file đã được symlink vào `~/airflow/dags/`
- [ ] Spark connection đã được tạo trong Airflow
- [ ] DAG không có lỗi syntax (kiểm tra trong Airflow UI)
- [ ] DAG đã được enable (toggle ON)

---

## Xem logs khi có lỗi

### Trong Airflow UI:
1. Click vào DAG → Click vào task bị lỗi
2. Click **Log** để xem chi tiết

### Trong terminal:
```bash
# Tìm log file mới nhất
find ~/airflow/logs -name "*start_kafka*" -type f -mtime -1 | head -1 | xargs tail -50

# Hoặc xem log của scheduler
tail -f ~/airflow/logs/scheduler/latest/*.log
```

---

## Lệnh hữu ích

```bash
# Kiểm tra DAGs
/home/haminhchien/airflow/venv/bin/airflow dags list | grep ml_streaming

# Test DAG syntax
/home/haminhchien/airflow/venv/bin/airflow dags test ml_streaming_pipeline 2025-12-09

# Xem task logs
/home/haminhchien/airflow/venv/bin/airflow tasks logs ml_streaming_pipeline start_kafka 2025-12-09

# Restart Airflow (nếu cần)
# Dừng: Ctrl+C trong terminal chạy Airflow
# Khởi động lại:
cd ~/airflow && source venv/bin/activate && airflow standalone
```

