# Các bước tiếp theo sau khi tạo Spark Connection

## ⚠️ Quan trọng: Kiểm tra cấu hình Connection

Bạn đã tạo connection với:
- Host: `master:7077` (Spark standalone cluster)
- Port: `7077`

Nhưng DAG của bạn đang cấu hình để chạy ở **local mode** (`local[*]`).

### Có 2 lựa chọn:

---

## Lựa chọn 1: Dùng Local Mode (Đơn giản hơn - Khuyến nghị)

Nếu bạn chạy Spark trên cùng máy (local), cần **sửa connection**:

1. Click vào **icon Edit** (bút chì) của connection `spark_default`
2. Sửa các trường:
   - **Host**: `local[*]` (thay vì `master:7077`)
   - **Port**: Để **trống** (xóa `7077`)
3. Click **Sauvegarder** (Save)

✅ **Ưu điểm**: Không cần Spark cluster, chạy trực tiếp trên máy

---

## Lựa chọn 2: Dùng Spark Standalone Cluster

Nếu bạn có Spark standalone cluster đang chạy:

1. **Giữ nguyên connection** (Host: `master:7077`, Port: `7077`)
2. **Sửa DAG** để không override `spark.master`:
   - Mở `dags/ml_pipeline_dag.py`
   - Xóa hoặc comment dòng `'spark.master': 'local[*]'` trong `conf`

✅ **Ưu điểm**: Tận dụng cluster, xử lý nhanh hơn

---

## 📋 Các bước tiếp theo để chạy DAG

### Bước 1: Đảm bảo các services đang chạy

```bash
# Kiểm tra Kafka
docker ps | grep kafka

# Nếu chưa chạy, khởi động:
cd docker
docker-compose up -d
```

### Bước 2: Kiểm tra DAG trong Airflow

1. Vào Airflow UI: http://localhost:8080
2. Click vào tab **DAGs**
3. Tìm DAG: `ml_streaming_pipeline`
4. Kiểm tra:
   - ✅ DAG có màu xanh (enabled)
   - ✅ Không có lỗi (màu đỏ)

### Bước 3: Trigger DAG

1. Click vào DAG `ml_streaming_pipeline`
2. Click nút **▶️ Play** (Trigger DAG) ở góc trên bên phải
3. Chọn **Trigger DAG w/ config** (hoặc chỉ Trigger)
4. Click **Trigger**

### Bước 4: Theo dõi tiến trình

1. Click vào DAG để xem **Graph View**
2. Các task sẽ chuyển từ màu xám → vàng (running) → xanh (success)
3. Click vào từng task để xem logs nếu có lỗi

### Bước 5: Chạy Visualization (tùy chọn)

Sau khi DAG chạy xong, để xem kết quả trực quan:

1. Tìm DAG: `ml_streaming_visualization`
2. Trigger DAG này
3. Hoặc chạy thủ công:
   ```bash
   python visualization/kafka_consumer.py
   ```

---

## 🔍 Kiểm tra kết quả

### Xem logs của từng task:

1. Click vào task trong Graph View
2. Click **Log** để xem chi tiết

### Kiểm tra mô hình đã được tạo:

```bash
ls -la models/house_price_model/
```

### Kiểm tra Kafka topics:

```bash
# Vào container Kafka
docker exec -it kafka bash

# List topics
kafka-topics --bootstrap-server localhost:9092 --list

# Xem messages trong topic
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic house-prices-output --from-beginning
```

---

## 🐛 Troubleshooting

### Lỗi: "Connection spark_default not found"

- Kiểm tra Connection ID phải đúng: `spark_default`
- Refresh trang Airflow

### Lỗi: "Cannot connect to Spark master"

**Nếu dùng local mode:**
- Sửa connection: Host = `local[*]`, Port = trống
- Đảm bảo Spark đã được cài và `SPARK_HOME` được set

**Nếu dùng standalone:**
- Kiểm tra Spark cluster đang chạy: `jps | grep Master`
- Kiểm tra có thể kết nối: `telnet master 7077`

### Lỗi: "Model not found"

- Task `train_model` phải chạy thành công trước
- Kiểm tra file: `models/house_price_model/metadata/part-00000`

### Lỗi: "Kafka connection refused"

```bash
# Kiểm tra Kafka
docker ps | grep kafka
docker logs kafka

# Restart nếu cần
cd docker
docker-compose restart
```

---

## ✅ Checklist trước khi chạy

- [ ] Spark connection đã được tạo (`spark_default`)
- [ ] Kafka đang chạy (`docker ps | grep kafka`)
- [ ] DAG `ml_streaming_pipeline` đã được enable (màu xanh)
- [ ] DAG không có lỗi syntax (không có dấu X đỏ)
- [ ] Đã cài đặt dependencies: `pip install -r requirements.txt`
- [ ] Spark 4.0.0 đã được cài và `SPARK_HOME` được set

---

## 🎯 Thứ tự thực thi DAG

DAG sẽ chạy theo thứ tự:

1. `start_kafka` - Khởi động Kafka
2. `check_kafka_ready` - Kiểm tra Kafka
3. `prepare_data` - Chuẩn bị dữ liệu
4. `train_model` - Huấn luyện mô hình
5. `start_streaming_job` - Khởi động Spark Streaming
6. `send_streaming_data` - Gửi dữ liệu vào Kafka
7. `wait_for_streaming` - Đợi xử lý
8. `cleanup` - Dọn dẹp

Tổng thời gian: ~10-15 phút (tùy vào số lượng dữ liệu)

