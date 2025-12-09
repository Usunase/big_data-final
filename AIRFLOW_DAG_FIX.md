# Hướng dẫn sửa lỗi DAG không hiển thị trong Airflow

## Vấn đề đã gặp

DAG `ml_streaming_pipeline` không hiển thị trong Airflow UI.

## Nguyên nhân

1. **DAG file không ở đúng thư mục**: Airflow tìm DAGs ở `~/airflow/dags/` nhưng file ở `~/Documents/bigdata/final_project/dags/`
2. **Lỗi syntax**: `schedule_interval` đã deprecated trong Airflow 2.4+, cần dùng `schedule`

## Giải pháp đã áp dụng

### 1. Tạo symlink từ thư mục DAGs của Airflow đến project

```bash
mkdir -p ~/airflow/dags
ln -sf /home/haminhchien/Documents/bigdata/final_project/dags/ml_pipeline_dag.py \
    ~/airflow/dags/ml_pipeline_dag.py
```

### 2. Sửa DAG file

Đã thay đổi:
- `schedule_interval=None` → `schedule=None`

## Kiểm tra DAG

### Cách 1: Dùng Airflow CLI

```bash
/home/haminhchien/airflow/venv/bin/airflow dags list | grep ml_streaming
```

### Cách 2: Kiểm tra trong Airflow UI

1. Mở http://localhost:8080
2. Vào tab **DAGs**
3. Tìm `ml_streaming_pipeline`

### Cách 3: Test DAG syntax

```bash
/home/haminhchien/airflow/venv/bin/python3 -c \
    "import sys; sys.path.insert(0, '/home/haminhchien/airflow/dags'); \
    from ml_pipeline_dag import dag; print('DAG ID:', dag.dag_id)"
```

## Nếu vẫn không thấy DAG

### 1. Kiểm tra Airflow scheduler đang chạy

```bash
ps aux | grep "airflow scheduler"
```

### 2. Xem logs của Airflow

```bash
tail -f ~/airflow/logs/scheduler/latest/*.log
```

### 3. Restart Airflow scheduler

```bash
# Tìm PID của scheduler
ps aux | grep "airflow scheduler" | grep -v grep

# Kill và restart (hoặc restart toàn bộ Airflow)
# Nếu dùng standalone:
# Ctrl+C trong terminal chạy Airflow, rồi chạy lại:
# /home/haminhchien/airflow/venv/bin/airflow standalone
```

### 4. Kiểm tra DAG có lỗi

Trong Airflow UI:
- Vào **DAGs** → tìm DAG
- Nếu có dấu X đỏ → click vào để xem lỗi

## Lưu ý

- Airflow tự động scan DAGs mỗi vài giây
- Nếu vừa thêm DAG mới, đợi 30-60 giây để Airflow phát hiện
- Symlink giúp DAG luôn sync với file gốc, không cần copy

## Cấu trúc thư mục

```
~/airflow/
├── dags/
│   └── ml_pipeline_dag.py -> /home/haminhchien/Documents/bigdata/final_project/dags/ml_pipeline_dag.py
├── airflow.cfg
└── ...

~/Documents/bigdata/final_project/
└── dags/
    └── ml_pipeline_dag.py (file gốc)
```

