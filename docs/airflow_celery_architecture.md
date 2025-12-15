# Airflow Celery Executor (v3.1.5) – Mô hình 3 máy

## Kiến trúc
- **Machine 1 – Airflow Master (192.168.80.148)**: Webserver, Scheduler, PostgreSQL (metadata), Redis (broker), Flower (monitor).
- **Machine 2 – Kafka Worker (192.168.80.127)**: Celery worker cho `kafka_queue`, Kafka + Zookeeper.
- **Machine 3 – Spark Worker (192.168.80.207)**: Celery worker cho `spark_queue`, Spark Master/Worker cho ML jobs.
- **Queues**: `kafka_queue` (Kafka tasks), `spark_queue` (Spark tasks), `default` (khác).

### Luồng Task
User → Airflow UI → Scheduler → Redis queue  
  └→ Workers nhận theo queue: Kafka (`kafka_queue`), Spark (`spark_queue`), Others (`default`)

## Chuẩn bị chung
- Biến môi trường tối thiểu (trên cả 3 máy, trỏ về Redis/Postgres của Master):
  - `AIRFLOW_HOME=/home/airflow/airflow` (ví dụ)
  - `AIRFLOW__CORE__EXECUTOR=CeleryExecutor`
  - `AIRFLOW__CELERY__BROKER_URL=redis://192.168.80.148:6379/0`
  - `AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow_user:airflow_pass123@192.168.80.148/airflow_db`
- Đồng bộ code DAGs/plugins: dùng git pull hoặc rsync/smb từ Master sang Workers.
- Đảm bảo version Python/venv và Airflow 3.1.5 đồng nhất trên cả 3 máy.

## Machine 1 – Airflow Master
Chạy các service nền (systemd/supervisor hoặc tmux):
```bash
airflow db upgrade
airflow webserver --port 8080
airflow scheduler
airflow celery flower --port 5555  # xem Workers/Queues
```

Kiểm tra cấu hình:
```bash
airflow config get-value core executor
airflow config get-value celery broker_url
airflow config get-value celery result_backend
```

Liệt kê worker đã đăng ký:
```bash
airflow celery list-workers
# Xem queue thực tế:
celery -A airflow.executors.celery_executor.app inspect active_queues
```

## Machine 2 – Kafka Worker (kafka_queue)
Khởi động Celery worker lắng nghe `kafka_queue` (kèm `default` nếu muốn fallback):
```bash
airflow celery worker --queues kafka_queue,default --loglevel INFO
```

Kiểm tra:
```bash
airflow celery list-workers
celery -A airflow.executors.celery_executor.app inspect active_queues
```

## Machine 3 – Spark Worker (spark_queue)
Khởi động Celery worker lắng nghe `spark_queue` (kèm `default` nếu muốn fallback):
```bash
airflow celery worker --queues spark_queue,default --loglevel INFO
```

Kiểm tra:
```bash
airflow celery list-workers
celery -A airflow.executors.celery_executor.app inspect active_queues
```

## Gợi ý vận hành DAG
- Các task Kafka đặt `queue='kafka_queue'`; task Spark đặt `queue='spark_queue'`; task chung dùng `default`.
- Flower (port 5555 trên Master) phải hiển thị mỗi worker kèm queue tương ứng; nếu không thấy queue, worker chưa chạy đúng.
- Khi thay đổi code DAG: git pull trên tất cả máy, rồi `airflow dags reserialize --include-plugins` (nếu dùng serialization) hoặc restart scheduler.
- Khi task bị “queued” mãi: kiểm tra `active_queues`; nếu thiếu, khởi động lại worker với `--queues ...`.

## Khắc phục sự cố thường gặp
- **Worker không nhận task**: sai broker/result_backend, hoặc worker không subscribe queue; xem lại env và lệnh `--queues`.
- **Không có lệnh `status/inspect` trong Airflow 3.1.5**: dùng `airflow celery list-workers` và `celery -A airflow.executors.celery_executor.app inspect active_queues`.
- **Redis/Postgres không truy cập được**: kiểm tra firewall, ping/telnet `192.168.80.148:6379` và cổng Postgres; xem log worker.
- **Phiên bản/venv lệch giữa các máy**: đồng bộ package (`pip freeze`), hoặc tạo venv chung và copy/clone requirements.

