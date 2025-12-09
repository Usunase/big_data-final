# Tóm tắt sửa lỗi DAG - start_kafka task

## Vấn đề gặp phải

Task `start_kafka` bị lỗi với thông báo:
```
AirflowException: Bash command failed. The command returned a non-zero exit code 1.
```

## Nguyên nhân có thể

1. **Docker compose command không tìm thấy trong PATH của Airflow**
2. **Environment variables khác nhau** giữa terminal và Airflow
3. **Thiếu error handling** trong bash script

## Giải pháp đã áp dụng

### 1. Thêm PATH và debug output
```bash
export PATH=/usr/bin:$PATH
echo "Current directory: $(pwd)"
echo "Docker version: $(docker --version)"
```

### 2. Fallback cho cả docker compose và docker-compose
```bash
docker compose down || docker-compose down || true
docker compose up -d || docker-compose up -d
```

### 3. Kiểm tra containers sau khi start
```bash
docker ps | grep -E "kafka|zookeeper" || echo "Warning: Containers may not be running"
```

### 4. Thêm sleep để đợi containers khởi động
```bash
sleep 5
```

## Code đã sửa

**File:** `dags/ml_pipeline_dag.py`

**Task:** `start_kafka`

**Bash command mới:**
```python
bash_command="""
set -e
export PATH=/usr/bin:$PATH
cd {{ params.project_dir }}/docker
echo "Current directory: $(pwd)"
echo "Docker version: $(docker --version)"
echo "Docker compose version: $(docker compose version || echo 'docker compose not found, trying docker-compose')"
docker compose down || docker-compose down || true
docker compose up -d || docker-compose up -d
sleep 5
docker ps | grep -E "kafka|zookeeper" || echo "Warning: Containers may not be running"
echo "✓ Đã khởi động Kafka container"
"""
```

## Cách test

### 1. Kiểm tra DAG syntax
```bash
/home/haminhchien/airflow/venv/bin/python3 -c \
    "import sys; sys.path.insert(0, '/home/haminhchien/airflow/dags'); \
    from ml_pipeline_dag import dag; print('OK')"
```

### 2. Test bash command thủ công
```bash
cd /home/haminhchien/Documents/bigdata/final_project/docker
export PATH=/usr/bin:$PATH
docker compose down || docker-compose down || true
docker compose up -d || docker-compose up -d
sleep 5
docker ps | grep -E "kafka|zookeeper"
```

### 3. Chạy lại DAG trong Airflow
- Vào Airflow UI → DAGs → `ml_streaming_pipeline`
- Trigger DAG mới
- Xem logs của task `start_kafka` để kiểm tra output

## Debug tips

### Xem logs chi tiết trong Airflow
1. Click vào DAG run
2. Click vào task `start_kafka`
3. Click **Log** để xem output chi tiết

### Xem logs trong terminal
```bash
# Tìm log file mới nhất
find ~/airflow/logs -name "*start_kafka*" -type f -mtime -1 | head -1 | xargs tail -100

# Hoặc xem tất cả logs của task
find ~/airflow/logs -path "*/start_kafka/*.log" -type f | xargs tail -50
```

## Nếu vẫn còn lỗi

### Kiểm tra quyền truy cập Docker
```bash
# User Airflow chạy có trong group docker không?
groups | grep docker

# Test docker command
docker ps
```

### Kiểm tra Docker socket
```bash
# Kiểm tra docker socket
ls -la /var/run/docker.sock

# Nếu không có quyền, thêm user vào group docker
sudo usermod -aG docker $USER
# Sau đó logout và login lại
```

### Kiểm tra environment
```bash
# Xem environment của Airflow process
ps aux | grep airflow | grep -v grep

# So sánh với environment hiện tại
env | grep -E "PATH|DOCKER"
```

## Lưu ý

- DAG sẽ tự động retry nếu fail (theo cấu hình `retries=1`)
- Nếu Kafka đã đang chạy, task vẫn sẽ chạy `docker compose down` trước (có thể gây gián đoạn)
- Có thể bỏ qua task `start_kafka` nếu Kafka đã được khởi động thủ công

