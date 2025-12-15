# Hướng dẫn dựng máy worker

Tài liệu này giúp bạn bật các worker Celery cho hệ thống (cả worker chạy trong Docker của Airflow và worker chạy Python thuần với `mycelery/system_worker.py`).

## 1) Chuẩn bị chung
- Cài Python 3.10+ và pip; trên máy chạy orchestrator cần Docker + Docker Compose.
- Mở các cổng cho broker/backend (mặc định Redis 6379, Postgres 5432) giữa các host.
- Đồng bộ code repo vào cùng đường dẫn trên các host, ví dụ `/home/haminhchien/Documents/bigdata/final_project`.

## 2) Chọn và thống nhất broker/backend
Đã chuẩn hóa cấu hình để dùng service nội bộ Docker:
- Broker: `redis://:@redis:6379/0`
- Backend: `db+postgresql://airflow:airflow@postgres/airflow`

Nếu cần truy cập từ bên ngoài host, cổng host đang map: Redis `56379`, Postgres `55432`.

## 3) Biến môi trường đề xuất (dùng chung)
Tạo file `.env` (hoặc export trên shell/systemd):
```
CELERY_BROKER_URL=redis://:@redis:6379/0
CELERY_RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
AIRFLOW_UID=50000
AIRFLOW_GID=0
```
Nếu worker nằm ngoài Docker và cần gọi vào host, thay `redis` bằng IP host và cổng `56379`, thay `postgres` bằng IP host và cổng `55432`.

## 4) Orchestrator: dựng stack Airflow (docker-compose)
Thực hiện trên máy chạy `docker-compose.yaml`:
```
cd /home/haminhchien/Documents/bigdata/final_project
docker compose build
docker compose up airflow-init
docker compose up -d
```
Các service chính:
- Airflow API/UI: http://<host>:9090 (service `airflow-apiserver`, mặc định user/pass `airflow/airflow` nếu chưa đổi).
- `airflow-worker` trong Docker sẽ tự nối vào broker/backend đã cấu hình.
- Redis, Postgres chạy kèm theo compose.

Kiểm tra nhanh:
```
docker compose ps
docker compose logs -f airflow-apiserver airflow-scheduler
```

## 5) Worker Python thuần (ngoài Docker) với `mycelery/system_worker.py`
Thực hiện trên mỗi host worker riêng:
1. Cài đặt và tạo môi trường:
   ```
   sudo apt-get update
   sudo apt-get install -y python3-venv
   cd /home/haminhchien/Documents/bigdata/final_project
   python3 -m venv venv
   source venv/bin/activate
   pip install --upgrade pip
   pip install celery redis psycopg2-binary docker
   ```
2. Xuất biến môi trường (khớp broker/backend đã chọn):
   ```
export CELERY_BROKER_URL=redis://:@redis:6379/0
export CELERY_RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
   ```
3. Chạy worker:
   ```
   source venv/bin/activate
   celery -A mycelery.system_worker worker \
     -n worker1@%h \
     -Q system,kafka_queue,spark_queue \
     --loglevel=info
   ```
   - Nếu host chỉ xử lý một loại queue, rút gọn `-Q` (ví dụ chỉ `system`).

## 6) Chạy như systemd service (tùy chọn)
Ví dụ service tự khởi động:
```
sudo tee /etc/systemd/system/system-worker.service <<'EOF'
[Unit]
Description=Celery system_worker
After=network-online.target
Wants=network-online.target

[Service]
User=airflow
WorkingDirectory=/home/haminhchien/Documents/bigdata/final_project
Environment="CELERY_BROKER_URL=redis://:@redis:6379/0"
Environment="CELERY_RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow"
ExecStart=/home/haminhchien/Documents/bigdata/final_project/venv/bin/celery -A mycelery.system_worker worker -n worker1@%h -Q system,kafka_queue,spark_queue --loglevel=info
Restart=always

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable --now system-worker
```
Điều chỉnh user, đường dẫn, queue, IP cho phù hợp.

## 7) Kiểm thử sau khi dựng
- Kiểm tra worker thấy broker:
  ```
  celery -A mycelery.system_worker status
  ```
- Trigger một DAG trên Airflow dùng task của `mycelery.system_worker` (ví dụ `run_command`) và xem log worker nhận job.
- Nếu không nhận task: kiểm tra lại IP broker/backend, firewall, và danh sách queue `-Q`.

## 8) Ghi chú nhanh
- Dùng một IP duy nhất cho Redis/Postgres trong toàn cluster để tránh worker bị lệch.
- Nếu muốn đọc cấu hình từ env thay vì hardcode, chỉnh `system_worker.py` đọc `CELERY_BROKER_URL` và `CELERY_RESULT_BACKEND`.

