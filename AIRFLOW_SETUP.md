# Hướng dẫn cấu hình Airflow Connection cho Spark

## Cấu hình Spark Connection trong Airflow

### Bước 1: Vào màn hình Connections

1. Mở Airflow UI: http://localhost:8080
2. Click vào **Admin** (menu trên cùng)
3. Chọn **Connections**

### Bước 2: Tạo Connection mới

1. Click nút **+** (Add a new record) hoặc **Ajouter une connexion**
2. Điền thông tin như sau:

#### Các trường bắt buộc:

- **Connection Id**: `spark_default`
  - ⚠️ Phải đúng tên này vì DAG sử dụng `conn_id='spark_default'`

- **Connection Type**: Chọn **`Spark`** từ dropdown
  - ⚠️ KHÔNG chọn "Package Index (Python)" hay các loại khác
  - Scroll trong dropdown để tìm "Spark"

#### Các trường cấu hình:

- **Host**: `local[*]`
  - Nếu chạy Spark local: `local[*]`
  - Nếu dùng Spark standalone cluster: `spark://master-hostname:7077`
  - Nếu dùng YARN: `yarn`

- **Port**: 
  - Để trống nếu dùng `local[*]`
  - Hoặc `7077` nếu dùng Spark standalone

- **Extra** (JSON format):
  ```json
  {
    "queue": "default",
    "deploy-mode": "client"
  }
  ```
  
  Hoặc đơn giản hơn:
  ```json
  {"queue": "default"}
  ```

- **Description** (tùy chọn): 
  - Ví dụ: "Spark connection for ML pipeline"

### Bước 3: Lưu

Click nút **Sauvegarder** (Save) để lưu connection.

---

## Kiểm tra Connection

Sau khi tạo, bạn sẽ thấy connection `spark_default` trong danh sách với:
- Connection Type: Spark
- Host: local[*]

---

## Troubleshooting

### Nếu không thấy "Spark" trong Connection Type dropdown:

1. Đảm bảo đã cài đặt Airflow Spark provider:
   ```bash
   pip install apache-airflow-providers-apache-spark==4.0.0
   ```

2. Restart Airflow webserver và scheduler:
   ```bash
   # Dừng Airflow (Ctrl+C)
   # Khởi động lại
   airflow webserver --port 8080
   airflow scheduler
   ```

### Nếu DAG vẫn báo lỗi connection:

1. Kiểm tra Connection ID phải đúng: `spark_default`
2. Kiểm tra trong DAG file: `conn_id='spark_default'`
3. Thử test connection trong Airflow UI (nếu có nút Test)

---

## Ví dụ cấu hình đầy đủ

```
Connection Id: spark_default
Connection Type: Spark
Host: local[*]
Port: (để trống)
Schema: (để trống)
Login: (để trống)
Password: (để trống)
Extra: {"queue": "default"}
```

---

## Lưu ý

- Connection ID `spark_default` là tên mặc định mà SparkSubmitOperator tìm kiếm
- Nếu muốn dùng tên khác, phải sửa trong DAG file: `conn_id='your_connection_name'`
- Với Spark local mode (`local[*]`), không cần điền Port

