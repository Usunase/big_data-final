# Sửa lỗi Spark Connection trong Airflow

## Lỗi gặp phải

```
ERROR - Cannot execute: spark-submit --master master:7077:7077
```

## Nguyên nhân

1. **Spark connection trong Airflow** đang được cấu hình với:
   - Host: `master:7077`
   - Port: `7077`

2. **DAG đang override** với `conf={'spark.master': 'spark://master:7077'}`

3. **SparkSubmitOperator kết hợp cả 2**, dẫn đến `master:7077:7077` (port bị duplicate)

## Giải pháp

### Cách 1: Sửa Connection trong Airflow UI (Khuyến nghị)

1. Vào Airflow UI → **Admin** → **Connections**
2. Tìm connection `spark_default`
3. Click **Edit** (icon bút chì)
4. Sửa:
   - **Host**: `local[*]` (thay vì `master:7077`)
   - **Port**: Để **trống** (xóa `7077`)
5. Click **Save**

### Cách 2: DAG đã được sửa để override

DAG đã được cập nhật để override connection với:
```python
conf={
    'spark.master': 'local[*]',
}
```

Nhưng vẫn có thể gặp vấn đề nếu SparkSubmitOperator không override đúng.

### Cách 3: Không dùng Connection (Nếu vẫn lỗi)

Nếu vẫn gặp lỗi, có thể sửa DAG để không dùng connection:

```python
train_model = SparkSubmitOperator(
    task_id='train_model',
    application='{{ params.project_dir }}/spark_jobs/train_model.py',
    name='house-price-training',
    # Không dùng conn_id, chỉ dùng conf
    # conn_id='spark_default',  # Comment dòng này
    verbose=True,
    driver_memory='4g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=1,
    conf={
        'spark.master': 'local[*]',
    },
    spark_binary='spark-submit',
    params={'project_dir': '/home/haminhchien/Documents/bigdata/final_project'}
)
```

## Kiểm tra

### 1. Test Spark local mode
```bash
cd /home/haminhchien/Documents/bigdata/final_project
spark-submit --master local[*] --driver-memory 4g --executor-memory 4g \
    spark_jobs/train_model.py
```

### 2. Kiểm tra DAG syntax
```bash
/home/haminhchien/airflow/venv/bin/python3 -c \
    "import sys; sys.path.insert(0, '/home/haminhchien/airflow/dags'); \
    from ml_pipeline_dag import dag; print('OK')"
```

### 3. Xem logs trong Airflow
- Click vào task `train_model` → **Log**
- Tìm dòng `spark-submit` để xem command thực tế

## Lưu ý

- **Local mode** (`local[*]`): Chạy Spark trên cùng máy, không cần cluster
- **Standalone mode** (`spark://master:7077`): Cần Spark cluster đang chạy
- Với local mode, `num_executors` nên là `1` hoặc không set

## Nếu có Spark Standalone Cluster

Nếu bạn thực sự có Spark standalone cluster:
1. Đảm bảo Spark master đang chạy: `jps | grep Master`
2. Kiểm tra có thể kết nối: `telnet master 7077`
3. Giữ connection như cũ và xóa `conf` override trong DAG

