# Hướng dẫn triển khai demo tối thiểu 3 máy (Spark / Kafka / Airflow + Producer)

## 1. Kiến trúc và vai trò
- **Máy Spark**: Spark Master + Workers, chạy train & streaming predict. Ví dụ IP: `192.168.x.S`.
- **Máy Kafka**: Kafka + Zookeeper (docker-compose). Ví dụ IP: `192.168.x.K`.
- **Máy Airflow**: Airflow scheduler + webserver; trigger DAG, chạy producer/consumer (có thể trùng máy demo).
- **Máy demo/producer**: chạy `kafka_producer.py` (có thể trùng Airflow nếu muốn).

## 2. Chuẩn bị mã nguồn
- Clone repo từ GitHub về cả 3 máy: `git clone <repo> final_project`
- Thống nhất branch/version và đường dẫn project.

## 3. Cấu hình IP trong code
Chỉnh các file sau để trỏ đúng IP Kafka/Spark:
- `streaming/kafka_producer.py`: `bootstrap_servers=['192.168.x.K:9092']`
- `spark_jobs/streaming_predict.py`: `.option("kafka.bootstrap.servers", "192.168.x.K:9092")`
- `visualization/kafka_consumer.py`: `bootstrap_servers=['192.168.x.K:9092']`
- `dags/ml_pipeline_dag.py`:
  - Spark master: `spark://192.168.x.S:7077` (train & streaming)
  - Nếu Kafka không phải localhost, cân nhắc chỉnh check_kafka_ready hoặc bỏ qua task này.

## 4. Chuẩn bị từng máy

### Máy Kafka
1) Cài Docker + Docker Compose.  
2) Chạy:
   ```
   cd final_project/docker
   docker compose up -d
   ```
3) Mở cổng: 9092 (Kafka), 2181 (ZK).  
4) Kiểm tra:
   ```
   docker ps
   docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```

### Máy Spark
1) Cài Spark 4.0.0 (Scala 2.13) + Java 17.  
2) Start Master:
   ```
   ./sbin/start-master.sh   # UI: 8080, master: 7077
   ```
3) Start Worker(s):
   ```
   ./sbin/start-worker.sh spark://192.168.x.S:7077
   ```
4) Mở cổng: 7077 (Spark), 8080 (Spark UI).  
5) Đảm bảo đọc file local: code đã set `spark.hadoop.fs.defaultFS=file:///` và đọc CSV với `file://...`.

### Máy Airflow
1) Cài Python 3.10+, Airflow 2.7+, provider Spark, PySpark 4.0.0, kafka-python, matplotlib, pandas, sklearn:
   ```
   pip install apache-airflow==2.7.0
   pip install apache-airflow-providers-apache-spark==4.0.0
   pip install pyspark==4.0.0 kafka-python matplotlib pandas scikit-learn numpy
   ```
2) Init Airflow:
   ```
   export AIRFLOW_HOME=~/airflow
   airflow db init
   airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
   airflow webserver --port 8080 &
   airflow scheduler &
   ```
3) Symlink DAG:
   ```
   ln -sf /path/to/final_project/dags/ml_pipeline_dag.py ~/airflow/dags/ml_pipeline_dag.py
   ```
4) (Tùy chọn) Sửa connection `spark_default` trong Airflow UI:
   - Conn Type: Spark
   - Host: `192.168.x.S`
   - Port: `7077`
   - Extra: `{"queue": "default"}`
   (Nếu để `conn_id=None` trong DAG thì không cần connection.)

### Máy demo/producer (có thể trùng Airflow)
- Cài Python + kafka-python, pandas.
- Đảm bảo truy cập Kafka IP `192.168.x.K:9092`.
- Chạy producer khi cần: `python streaming/kafka_producer.py 1 200`

## 5. Quy trình demo (tối thiểu 3 máy)

1) **Kafka (Machine Kafka)**: `docker compose up -d`  
2) **Spark (Machine Spark)**: Start Master + Worker(s).  
3) **Airflow (Machine Airflow)**:
   - Đảm bảo venv có đủ deps.
   - Airflow webserver + scheduler đang chạy.
   - Airflow UI: bật DAG `ml_streaming_pipeline`.
   - Trigger DAG (chạy tuần tự: prepare_data → train_model → start_streaming_job → send_streaming_data → wait → cleanup).
4) (Tùy chọn) Trigger DAG `ml_streaming_visualization` để chạy consumer (đọc Kafka output, hiển thị matplotlib).
5) Nếu muốn gửi thêm dữ liệu: chạy producer thủ công từ máy demo:
   ```
   python streaming/kafka_producer.py 1 200
   ```

## 6. Kiểm tra nhanh
- Spark UI: http://192.168.x.S:8080 (xem workers, apps)
- Kafka topics:
  ```
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
  docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
      --topic house-prices-output --from-beginning --max-messages 5
  ```
- Airflow UI: http://<Airflow_IP>:8080

## 7. Chú ý khi chạy đa máy
- Firewall mở port: 9092 (Kafka), 2181 (ZK), 7077 (Spark), 8080 (Spark UI), 8080 Airflow (nếu truy cập từ ngoài).
- Đảm bảo các file CSV nằm trên máy Spark nếu Spark job đọc local; đã set `file:///absolute/path`.
- Nếu Kafka không ở localhost, phải chỉnh `bootstrap_servers` ở producer/streaming/consumer.
- Không cần task start_kafka trong DAG nếu Kafka chạy trên máy riêng; có thể bỏ qua/disable task này hoặc chỉnh lệnh phù hợp.

## 8. Báo cáo & slide
- Mô tả kiến trúc 3 máy (IP, port).
- Luồng dữ liệu: prepare_data → train_model → streaming_predict → Kafka output → visualization.
- Kết quả mô hình: RMSE/MAE/R², kích thước dữ liệu, tham số RF.
- Demo checklist: Kafka up, Spark up, Airflow up, DAG chạy, Spark UI hiển thị jobs, Kafka output, Visualization hiển thị.


