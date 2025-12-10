# Hướng dẫn cài đặt và khởi động Airflow từ đầu

## 📋 Yêu cầu hệ thống

- **Airflow Version**: 3.1.3 (hoặc mới hơn)
- Python 3.9+ (khuyến nghị Python 3.10 hoặc 3.11)
- pip (Python package manager)
- Tối thiểu 4GB RAM
- Kết nối Internet để tải packages

**Lưu ý:** Airflow 3.x có một số thay đổi so với 2.x về cấu trúc và cách hoạt động.

---

## 🚀 Bước 1: Tạo Virtual Environment (Khuyến nghị)

### Tại sao cần Virtual Environment?
- Tách biệt dependencies của Airflow với hệ thống
- Tránh xung đột với các Python packages khác
- Dễ quản lý và dọn dẹp

### Cách tạo:

```bash
# Tạo thư mục cho Airflow
mkdir -p ~/airflow
cd ~/airflow

# Tạo virtual environment
python3 -m venv venv

# Kích hoạt virtual environment
source venv/bin/activate  # Linux/Mac
# hoặc
# venv\Scripts\activate  # Windows
```

**Lưu ý:** Mỗi lần làm việc với Airflow, bạn cần activate virtual environment:
```bash
cd ~/airflow
source venv/bin/activate
```

---

## 📦 Bước 2: Cài đặt Airflow

### 2.1. Cài đặt Airflow Core

```bash
# Đảm bảo đã activate venv
source ~/airflow/venv/bin/activate

# Cài đặt Airflow 3.1.3
pip install apache-airflow==3.1.3

# Cài đặt Spark provider (để submit Spark jobs)
# Kiểm tra version tương thích với Airflow 3.1.3
pip install apache-airflow-providers-apache-spark
```

**Lưu ý Airflow 3.x:**
- Airflow 3.x có cấu trúc khác với 2.x
- Một số operators đã được di chuyển sang `airflow.providers`
- Import paths có thể khác một chút

### 2.2. Cài đặt các Python dependencies cần thiết

```bash
# Cài đặt các packages cho ML pipeline
pip install pyspark==4.0.0
pip install pandas>=1.5.0
pip install scikit-learn>=1.0.0
pip install kafka-python>=2.0.0
pip install matplotlib>=3.5.0
pip install numpy>=1.21.0
```

**Hoặc cài từ requirements.txt:**
```bash
cd /home/haminhchien/Documents/bigdata/final_project
pip install -r requirements.txt
```

---

## ⚙️ Bước 3: Khởi tạo Airflow Database

### 3.1. Set AIRFLOW_HOME (tùy chọn)

```bash
export AIRFLOW_HOME=~/airflow
```

Hoặc thêm vào `~/.bashrc` hoặc `~/.zshrc`:
```bash
echo 'export AIRFLOW_HOME=~/airflow' >> ~/.bashrc
source ~/.bashrc
```

### 3.2. Khởi tạo database

```bash
# Đảm bảo đã activate venv
source ~/airflow/venv/bin/activate

# Khởi tạo Airflow database
airflow db init
```

**Kết quả:** Tạo các file và thư mục trong `~/airflow/`:
- `airflow.cfg` - File cấu hình
- `airflow.db` - SQLite database
- `logs/` - Thư mục logs
- `dags/` - Thư mục chứa DAGs

---

## 👤 Bước 4: Tạo User Admin

```bash
# Đảm bảo đã activate venv
source ~/airflow/venv/bin/activate

# Tạo user admin
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

**Thông tin đăng nhập:**
- Username: `admin`
- Password: `admin`

---

## 🔗 Bước 5: Kết nối DAGs từ Project

### 5.1. Tạo symlink từ project đến Airflow

```bash
# Tạo symlink để Airflow nhận DAGs từ project
ln -sf /home/haminhchien/Documents/bigdata/final_project/dags/ml_pipeline_dag.py \
    ~/airflow/dags/ml_pipeline_dag.py
```

### 5.2. Kiểm tra DAGs được nhận diện

```bash
source ~/airflow/venv/bin/activate
airflow dags list | grep ml_streaming
```

**Kết quả mong đợi:**
```
dag_id                    | fileloc      | owners  | is_paused
ml_streaming_pipeline     | ...          | airflow | True
ml_streaming_visualization| ...          | airflow | True
```

---

## 🎯 Bước 6: Khởi động Airflow

### 6.1. Khởi động Airflow Webserver (Terminal 1)

```bash
# Activate venv
cd ~/airflow
source venv/bin/activate

# Khởi động webserver
airflow webserver --port 8080
```

**Hoặc chạy background:**
```bash
nohup airflow webserver --port 8080 > ~/airflow/logs/webserver.log 2>&1 &
```

### 6.2. Khởi động Airflow Scheduler (Terminal 2)

```bash
# Activate venv
cd ~/airflow
source venv/bin/activate

# Khởi động scheduler
airflow scheduler
```

**Hoặc chạy background:**
```bash
nohup airflow scheduler > ~/airflow/logs/scheduler.log 2>&1 &
```

### 6.3. Hoặc dùng Airflow Standalone (Tất cả trong 1)

```bash
# Activate venv
cd ~/airflow
source venv/bin/activate

# Chạy standalone (webserver + scheduler)
airflow standalone
```

**Lưu ý:** Standalone tự động tạo user admin với password được in ra terminal.

---

## 🌐 Bước 7: Truy cập Airflow UI

1. Mở trình duyệt
2. Truy cập: **http://localhost:8080**
3. Đăng nhập:
   - Username: `admin`
   - Password: `admin` (hoặc password bạn đã set)

---

## ✅ Bước 8: Kiểm tra và Enable DAGs

### 8.1. Kiểm tra DAGs trong UI

1. Vào tab **DAGs**
2. Tìm DAGs:
   - `ml_streaming_pipeline`
   - `ml_streaming_visualization`

### 8.2. Enable DAGs

1. Tìm toggle switch bên trái tên DAG
2. Click để chuyển từ **OFF** → **ON** (màu xanh)

### 8.3. Kiểm tra DAG không có lỗi

- DAG không có dấu **X đỏ** = OK
- Nếu có X đỏ, click vào để xem lỗi

---

## 🔧 Bước 9: Cấu hình Spark Connection (Nếu cần)

### 9.1. Vào Connections

1. Airflow UI → **Admin** → **Connections**
2. Tìm hoặc tạo connection với ID: `spark_default`

### 9.2. Cấu hình Connection

**Nếu dùng Spark Standalone:**
- **Connection Type**: `Spark`
- **Host**: `192.168.1.19` (IP của Spark Master)
- **Port**: `7077`
- **Extra**: `{"queue": "default"}`

**Nếu dùng Local Mode:**
- **Connection Type**: `Spark`
- **Host**: `local[*]`
- **Port**: (để trống)
- **Extra**: `{"queue": "default"}`

**Lưu ý:** DAG hiện tại không dùng connection (đã set `conn_id=None`), nhưng có thể cần cho các DAG khác.

---

## 🛑 Bước 10: Dừng Airflow

### 10.1. Nếu chạy foreground (Ctrl+C)

```bash
# Trong terminal chạy webserver/scheduler
Ctrl+C
```

### 10.2. Nếu chạy background

```bash
# Tìm process
ps aux | grep airflow

# Kill process
pkill -f "airflow webserver"
pkill -f "airflow scheduler"
```

---

## 📝 Checklist hoàn chỉnh

- [ ] Python 3.9+ đã được cài
- [ ] Virtual environment đã được tạo và activate
- [ ] Airflow 2.7.0 đã được cài
- [ ] Spark provider đã được cài
- [ ] Python dependencies đã được cài (pyspark, pandas, kafka-python, matplotlib)
- [ ] Airflow database đã được init (`airflow db init`)
- [ ] User admin đã được tạo
- [ ] DAGs đã được symlink vào `~/airflow/dags/`
- [ ] Airflow webserver đang chạy (port 8080)
- [ ] Airflow scheduler đang chạy
- [ ] Có thể truy cập Airflow UI (http://localhost:8080)
- [ ] DAGs đã được enable trong UI
- [ ] Spark connection đã được cấu hình (nếu cần)

---

## 🐛 Troubleshooting

### Lỗi: "airflow: command not found"

**Nguyên nhân:** Chưa activate virtual environment hoặc Airflow chưa được cài

**Giải pháp:**
```bash
source ~/airflow/venv/bin/activate
which airflow  # Kiểm tra đường dẫn
```

### Lỗi: "Port 8080 already in use"

**Nguyên nhân:** Port 8080 đã được sử dụng

**Giải pháp:**
```bash
# Tìm process đang dùng port 8080
lsof -i :8080

# Kill process hoặc dùng port khác
airflow webserver --port 8081
```

### Lỗi: "DAG not found"

**Nguyên nhân:** DAG file chưa được symlink hoặc có lỗi syntax

**Giải pháp:**
```bash
# Kiểm tra symlink
ls -la ~/airflow/dags/

# Kiểm tra syntax
source ~/airflow/venv/bin/activate
python3 -c "import sys; sys.path.insert(0, '~/airflow/dags'); from ml_pipeline_dag import dag; print('OK')"
```

### Lỗi: "ModuleNotFoundError"

**Nguyên nhân:** Thiếu Python packages trong Airflow venv

**Giải pháp:**
```bash
source ~/airflow/venv/bin/activate
pip install <package_name>
```

---

## 📚 Tài liệu tham khảo

- [Airflow Official Documentation](https://airflow.apache.org/docs/)
- [Airflow Installation Guide](https://airflow.apache.org/docs/apache-airflow/stable/start.html)
- [Airflow Spark Provider](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/index.html)

---

## 🎯 Quick Start Commands (Airflow 3.1.3)

```bash
# 1. Tạo và activate venv
mkdir -p ~/airflow && cd ~/airflow
python3 -m venv venv
source venv/bin/activate

# 2. Cài đặt Airflow 3.1.3 và dependencies
pip install apache-airflow==3.1.3
pip install apache-airflow-providers-apache-spark
pip install pyspark==4.0.0 pandas scikit-learn kafka-python matplotlib numpy

# 3. Khởi tạo database
export AIRFLOW_HOME=~/airflow
airflow db init

# 4. Tạo user admin
airflow users create --username admin --firstname Admin --lastname User \
    --role Admin --email admin@example.com --password admin

# 5. Symlink DAGs
ln -sf /home/haminhchien/Documents/bigdata/final_project/dags/ml_pipeline_dag.py \
    ~/airflow/dags/ml_pipeline_dag.py

# 6. Khởi động Airflow
airflow standalone
# Hoặc riêng biệt:
# Terminal 1: airflow webserver --port 8080
# Terminal 2: airflow scheduler

# 7. Truy cập UI
# Mở browser: http://localhost:8080
# Login: admin/admin
```

## ⚠️ Lưu ý đặc biệt cho Airflow 3.1.3

### Thay đổi trong Airflow 3.x:
1. **Import paths**: Một số operators đã được di chuyển
   - `airflow.operators.bash` → `airflow.providers.standard.operators.bash`
   - `airflow.operators.python` → `airflow.providers.standard.operators.python`

2. **DAG structure**: Về cơ bản giống nhau nhưng có thể có một số thay đổi nhỏ

3. **Database**: Có thể cần migrate nếu upgrade từ 2.x

### Kiểm tra version:
```bash
source ~/airflow/venv/bin/activate
airflow version
# Kết quả mong đợi: 3.1.3
```

---

Chúc bạn thành công! 🎉

