# Cấu hình Spark Standalone Cluster

## Cấu hình hiện tại

- **Spark Master**: `spark://192.168.1.19:7077`
- **Cluster Mode**: Standalone
- **Executors**: 2
- **Executor Cores**: 2
- **Executor Memory**: 4g
- **Driver Memory**: 4g

## DAG đã được cập nhật

### Task `train_model`
```python
conf={
    'spark.master': 'spark://192.168.1.19:7077',
}
num_executors=2
```

### Task `start_streaming_job`
```bash
spark-submit --master spark://192.168.1.19:7077 \
    --num-executors 2 \
    --executor-cores 2 \
    ...
```

## Kiểm tra Spark Master

### 1. Kiểm tra kết nối
```bash
# Ping
ping -c 1 192.168.1.19

# Kiểm tra port
nc -zv 192.168.1.19 7077
```

### 2. Kiểm tra Spark Master UI
Mở trình duyệt: http://192.168.1.19:8080

Bạn sẽ thấy:
- Spark Master Web UI
- Danh sách workers
- Running applications
- Completed applications

### 3. Test Spark submit
```bash
spark-submit --master spark://192.168.1.19:7077 \
    --class org.apache.spark.examples.SparkPi \
    $SPARK_HOME/examples/jars/spark-examples_2.13-4.0.0.jar 10
```

## Cấu hình Airflow Connection

### Option 1: Sửa Connection trong Airflow UI (Khuyến nghị)

1. Vào Airflow UI → **Admin** → **Connections**
2. Tìm `spark_default` → **Edit**
3. Sửa:
   - **Host**: `192.168.1.19` (hoặc `spark://192.168.1.19`)
   - **Port**: `7077`
   - **Extra**: `{"queue": "default"}`
4. **Save**

### Option 2: DAG đã override connection

DAG đã override connection với `conf={'spark.master': 'spark://192.168.1.19:7077'}`, nên không cần sửa connection nếu không muốn.

## Lưu ý quan trọng

### 1. Spark Master phải đang chạy
```bash
# Trên máy Spark Master (192.168.1.19)
# Kiểm tra Spark Master process
jps | grep Master

# Hoặc kiểm tra port
netstat -tuln | grep 7077
```

### 2. Spark Workers phải đang chạy
- Cần ít nhất 1 worker để chạy jobs
- Kiểm tra trong Spark Master UI: http://192.168.1.19:8080

### 3. Network connectivity
- Máy chạy Airflow phải có thể kết nối đến `192.168.1.19:7077`
- Firewall không chặn port 7077

### 4. File paths
- Spark jobs phải accessible từ Spark workers
- Nếu dùng local files, cần copy đến workers hoặc dùng shared storage (HDFS, NFS)

## Troubleshooting

### Lỗi: "Cannot connect to master"

**Nguyên nhân:**
- Spark Master chưa chạy
- Network issue
- Firewall blocking

**Giải pháp:**
```bash
# Kiểm tra Spark Master
ssh user@192.168.1.19 "jps | grep Master"

# Kiểm tra network
ping 192.168.1.19
telnet 192.168.1.19 7077
```

### Lỗi: "No executors available"

**Nguyên nhân:**
- Không có Spark workers
- Workers không đủ resources

**Giải pháp:**
- Kiểm tra Spark Master UI: http://192.168.1.19:8080
- Xem số lượng workers available
- Giảm `num_executors` hoặc `executor_memory` nếu cần

### Lỗi: "Application failed"

**Nguyên nhân:**
- File không tìm thấy trên workers
- Dependencies thiếu

**Giải pháp:**
- Đảm bảo file paths accessible từ workers
- Copy dependencies đến workers hoặc dùng `--packages`

## Test DAG

### 1. Kiểm tra DAG syntax
```bash
/home/haminhchien/airflow/venv/bin/python3 -c \
    "import sys; sys.path.insert(0, '/home/haminhchien/airflow/dags'); \
    from ml_pipeline_dag import dag; print('OK')"
```

### 2. Test Spark submit thủ công
```bash
cd /home/haminhchien/Documents/bigdata/final_project
spark-submit --master spark://192.168.1.19:7077 \
    --driver-memory 4g \
    --executor-memory 4g \
    --num-executors 2 \
    --executor-cores 2 \
    spark_jobs/train_model.py
```

### 3. Chạy DAG trong Airflow
- Vào Airflow UI → DAGs → `ml_streaming_pipeline`
- Trigger DAG
- Xem logs của task `train_model`

## Monitoring

### Spark Master UI
- URL: http://192.168.1.19:8080
- Xem:
  - Workers status
  - Running applications
  - Completed applications
  - Resources usage

### Spark Application UI
- Mỗi application có UI riêng
- URL: http://192.168.1.19:4040 (nếu chạy local)
- Hoặc link từ Spark Master UI

## Performance Tuning

### Tăng số executors
```python
num_executors=4  # Tăng từ 2 lên 4
```

### Tăng memory
```python
executor_memory='8g'  # Tăng từ 4g lên 8g
driver_memory='8g'
```

### Điều chỉnh cores
```python
executor_cores=4  # Tăng từ 2 lên 4
```

**Lưu ý:** Đảm bảo cluster có đủ resources!

