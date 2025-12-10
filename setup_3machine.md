  cd /home/.../final_project/docker
  docker compose up -d
  ```
- Mở cổng: 9092 (Kafka), 2181 (ZK). IP máy Kafka: ví dụ `192.168.x.K`.
- Kiểm tra:
  ```
  docker ps
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
  ```

### Máy Spark (Machine 1)
- Cài Spark 4.0.0 (Scala 2.13) + Java 17.
- Chạy Spark Master:
  ```
  ./sbin/start-master.sh  # mặc định 7077, UI 8080
  ```
- Chạy ít nhất 1 worker:
  ```
  ./sbin/start-worker.sh spark://<SPARK_MASTER_IP>:7077
  ```
- Mở cổng: 7077 (Spark), 8080 (Spark UI). IP máy Spark: ví dụ `192.168.x.S`.
- Đảm bảo đọc file từ local path hoặc shared storage. Trong code đã chỉnh `spark.hadoop.fs.defaultFS=file:///` và đọc CSV với `file://...`.

### Máy Airflow (Machine 3)
- Cài Python 3.10+, Airflow 2.7+, provider Spark, pyspark==4.0.0, kafka-python, matplotlib, pandas, sklearn.
  ```
  pip install apache-airflow==2.7.0
  pip install apache-airflow-providers-apache-spark==4.0.0
  pip install pyspark==4.0.0 kafka-python matplotlib pandas scikit-learn numpy
  ```
- Set `AIRFLOW_HOME`, init DB, tạo user admin, chạy webserver + scheduler.
- Symlink DAG:
  ```
  ln -sf /path/to/final_project/dags/ml_pipeline_dag.py ~/airflow/dags/
  ```
- Trong DAG, đã cấu hình Spark master `spark://192.168.x.S:7077`. Nếu cần, chỉnh `dags/ml_pipeline_dag.py`.
- Kafka bootstrap servers: sửa trong `streaming_predict.py` và `kafka_producer.py` nếu Kafka ở IP khác (hiện mặc định localhost). Đặt thành `192.168.x.K:9092`.
- Kiểm tra DAG load: Airflow UI -> DAGs.

### Máy demo/producer (có thể là Airflow máy hoặc máy riêng)
- Cài Python + kafka-python, pandas.
- Đảm bảo truy cập được Kafka IP `192.168.x.K:9092`.
- Chạy producer:
  ```
  python streaming/kafka_producer.py 1 200
  ```

## 4. Các điểm cần chỉnh trong code cho đa máy
- Kafka bootstrap servers:
  - `streaming/kafka_producer.py`: `bootstrap_servers=['192.168.x.K:9092']`
  - `spark_jobs/streaming_predict.py`: `.option("kafka.bootstrap.servers", "192.168.x.K:9092")`
  - `visualization/kafka_consumer.py`: `bootstrap_servers=['192.168.x.K:9092']`
- Spark master:
  - `dags/ml_pipeline_dag.py` (train_model, start_streaming_job): `spark://192.168.x.S:7077`
- Đường dẫn dữ liệu:
  - Đọc CSV dạng `file:///absolute/path/...` (đã chỉnh trong `train_model.py`).
- Topic:
  - Input: `house-prices-input`
  - Output: `house-prices-output`

## 5. Quy trình demo (tối thiểu 3 máy)
Trên Kafka (Machine 2):
1. `docker compose up -d`
2. Kiểm tra Kafka port 9092.

Trên Spark (Machine 1):
1. Start Master + Worker(s):
 