"""
Airflow DAG để điều khiển toàn bộ ML pipeline
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import time
import subprocess
import os

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_kafka_ready(**kwargs):
    """Kiểm tra Kafka đã sẵn sàng chưa"""
    import socket
    max_retries = 30
    for i in range(max_retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', 9092))
            sock.close()
            if result == 0:
                print(f"✓ Kafka đã sẵn sàng!")
                return True
            else:
                print(f"⏳ Đang chờ Kafka... (thử lần {i+1}/{max_retries})")
                time.sleep(10)
        except Exception as e:
            print(f"❌ Lỗi khi kiểm tra Kafka: {e}")
            time.sleep(10)
    raise Exception("Kafka không sẵn sàng sau 5 phút")

def wait_for_streaming_complete(**kwargs):
    """Đợi streaming hoàn thành (hoặc timeout)"""
    print("⏳ Đợi 5 phút để streaming xử lý dữ liệu...")
    time.sleep(300)  # 5 phút
    print("✓ Hoàn thành thời gian streaming")

# Tạo DAG
with DAG(
    'ml_streaming_pipeline',
    default_args=default_args,
    description='End-to-end ML pipeline with Kafka and Spark',
    schedule=None,  # Chạy manual (schedule_interval deprecated in Airflow 2.4+)
    catchup=False,
    tags=['machine-learning', 'kafka', 'spark', 'streaming'],
) as dag:
    
    # Task 1: Khởi động Kafka với Docker Compose
    start_kafka = BashOperator(
        task_id='start_kafka',
        bash_command="""
        set -e
        export PATH=/usr/bin:$PATH
        cd {{ params.project_dir }}/docker
        echo "Current directory: $(pwd)"
        echo "Docker version: $(docker --version)"
        echo "Docker compose version: $(docker compose version || echo 'docker compose not found, trying docker-compose')"
        docker compose down || docker-compose down || true
        docker compose up -d || docker-compose up -d
        sleep 5
        docker ps | grep -E "kafka|zookeeper" || echo "Warning: Containers may not be running"
        echo "✓ Đã khởi động Kafka container"
        """,
        params={'project_dir': '/home/haminhchien/Documents/bigdata/final_project'}
    )
    
    # Task 2: Kiểm tra Kafka đã sẵn sàng
    check_kafka = PythonOperator(
        task_id='check_kafka_ready',
        python_callable=check_kafka_ready,
    )
    
    # Task 3: Chuẩn bị dữ liệu (nếu chưa có)
    prepare_data = BashOperator(
        task_id='prepare_data',
        bash_command="""
        cd {{ params.project_dir }} && \
        if [ ! -f data/train_data.csv ]; then
            echo "📊 Đang chuẩn bị dữ liệu..."
            python data/prepare_data.py
        else
            echo "✓ Dữ liệu đã có sẵn"
        fi
        """,
        params={'project_dir': '/home/haminhchien/Documents/bigdata/final_project'}
    )
    
    # Task 4: Huấn luyện mô hình với Spark (dùng bash để kiểm soát --master)
    train_model = BashOperator(
        task_id='train_model',
        bash_command="""
        cd {{ params.project_dir }} && \
        spark-submit \
            --master spark://192.168.1.19:7077 \
            --conf spark.hadoop.fs.defaultFS=file:/// \
            --conf spark.local.dir=/tmp/spark_local \
            --driver-memory 4g \
            --executor-memory 4g \
            --num-executors 2 \
            --executor-cores 2 \
            spark_jobs/train_model.py
        """,
        params={'project_dir': '/home/haminhchien/Documents/bigdata/final_project'}
    )
    
    # Task 5: Khởi động Spark Streaming job (background)
    start_streaming_job = BashOperator(
        task_id='start_streaming_job',
        bash_command="""
        cd {{ params.project_dir }} && \
        nohup spark-submit \
            --master spark://192.168.1.19:7077 \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
            --driver-memory 4g \
            --executor-memory 4g \
            --num-executors 2 \
            --executor-cores 2 \
            spark_jobs/streaming_predict.py > /tmp/spark_streaming.log 2>&1 &
        echo $! > /tmp/spark_streaming.pid
        echo "✓ Đã khởi động Spark Streaming job (PID: $(cat /tmp/spark_streaming.pid))"
        sleep 20
        """,
        params={'project_dir': '/home/haminhchien/Documents/bigdata/final_project'}
    )
    
    # Task 6: Gửi dữ liệu streaming vào Kafka
    send_streaming_data = BashOperator(
        task_id='send_streaming_data',
        bash_command="""
        cd {{ params.project_dir }} && \
        echo "📤 Đang gửi dữ liệu streaming vào Kafka..." && \
        python streaming/kafka_producer.py 1 200
        """,
        params={'project_dir': '/home/haminhchien/Documents/bigdata/final_project'}
    )
    
    # Task 7: Đợi streaming xử lý xong
    wait_processing = PythonOperator(
        task_id='wait_for_streaming',
        python_callable=wait_for_streaming_complete,
    )
    
    # Task 8: Dọn dẹp (optional - dừng streaming job)
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command="""
        if [ -f /tmp/spark_streaming.pid ]; then
            PID=$(cat /tmp/spark_streaming.pid)
            echo "🛑 Đang dừng Spark Streaming job (PID: $PID)"
            kill $PID 2>/dev/null || echo "Process đã dừng"
            rm /tmp/spark_streaming.pid
        fi
        echo "✓ Hoàn thành pipeline"
        """,
        trigger_rule='all_done'  # Chạy dù task trước thành công hay thất bại
    )
    
    # Định nghĩa thứ tự thực thi
    start_kafka >> check_kafka >> prepare_data >> train_model >> start_streaming_job >> send_streaming_data >> wait_processing >> cleanup


# DAG riêng để chạy visualization
with DAG(
    'ml_streaming_visualization',
    default_args=default_args,
    description='Run visualization consumer',
    schedule=None,  # Chạy manual
    catchup=False,
    tags=['visualization', 'kafka'],
) as dag_viz:
    
    run_visualization = BashOperator(
        task_id='run_visualization',
        bash_command="""
        cd {{ params.project_dir }} && \
        python visualization/kafka_consumer.py
        """,
        params={'project_dir': '/home/haminhchien/Documents/bigdata/final_project'}
    )