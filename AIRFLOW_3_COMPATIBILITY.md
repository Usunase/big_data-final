# Tương thích với Airflow 3.1.3

## Thay đổi trong Airflow 3.x

### 1. Import Paths

**Airflow 2.x:**
```python
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
```

**Airflow 3.x (Khuyến nghị):**
```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
```

**DAG đã được cập nhật** để hỗ trợ cả 2.x và 3.x với try/except fallback.

### 2. Cài đặt

```bash
# Airflow 3.1.3
pip install apache-airflow==3.1.3

# Spark provider (tự động tương thích)
pip install apache-airflow-providers-apache-spark
```

### 3. Kiểm tra version

```bash
source ~/airflow/venv/bin/activate
airflow version
# Kết quả: 3.1.3
```

### 4. Các thay đổi khác trong Airflow 3.x

- **Database schema**: Có thể cần migrate nếu upgrade từ 2.x
- **Config format**: Về cơ bản giống nhau
- **CLI commands**: Giống nhau (`airflow db init`, `airflow webserver`, etc.)
- **UI**: Giao diện tương tự, có thể có một số cải tiến

### 5. Troubleshooting cho Airflow 3.1.3

#### Lỗi: Deprecated warnings về operators

Nếu thấy warnings:
```
DeprecatedImportWarning: The `airflow.operators.bash.BashOperator` attribute is deprecated
```

**Giải pháp:** DAG đã được cập nhật để tự động dùng import mới. Warnings này không ảnh hưởng chức năng nhưng có thể bỏ qua hoặc cập nhật import như trên.

#### Lỗi: Module not found

Nếu gặp lỗi import:
```bash
# Kiểm tra providers đã được cài
pip list | grep airflow-providers

# Cài lại nếu thiếu
pip install apache-airflow-providers-standard
pip install apache-airflow-providers-apache-spark
```

### 6. Migration từ Airflow 2.x sang 3.x

Nếu bạn đang upgrade từ 2.x:

```bash
# Backup database trước
cp ~/airflow/airflow.db ~/airflow/airflow.db.backup

# Upgrade Airflow
pip install --upgrade apache-airflow==3.1.3

# Migrate database
airflow db upgrade

# Kiểm tra DAGs
airflow dags list
```

### 7. Tài liệu tham khảo

- [Airflow 3.0 Release Notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html)
- [Airflow Providers](https://airflow.apache.org/docs/apache-airflow-providers/)


