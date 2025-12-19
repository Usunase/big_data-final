"""
Airflow DAG cho quá trình dự đoán streaming
Sử dụng RabbitMQ/Celery để điều khiển các node từ xa (không dùng SSH)
Kiến trúc:
- Máy Airflow (localhost): Chạy Airflow, RabbitMQ
- Máy Kafka (192.168.80.127): Chạy Kafka (Celery worker queue: node_57)
- Máy Spark (192.168.80.207): Chạy Spark (Celery worker queue: spark)
- Máy Hadoop (192.168.80.127): Chạy HDFS
"""
from airflow import DAG
try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import time
import socket
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.rabbitmq_client import get_rabbitmq_client
from mycelery.system_worker import docker_compose_up, run_command

# ========================================
# CẤU HÌNH HỆ THỐNG PHÂN TÁN
# ========================================
# Theo CLUSTER_NODES trong system_worker.py
KAFKA_HOST, KAFKA_PORT = "192.168.80.127", 9092  # kafka node
SPARK_HOST = "192.168.80.207"  # spark-master, spark-worker
SPARK_MASTER = f"spark://{SPARK_HOST}:7077"
PROJECT_DIR = "/home/haminhchien/Documents/bigdata/final_project"

# RabbitMQ chạy trên máy Airflow (cùng máy) - không cần IP, dùng localhost
RABBITMQ_HOST = "localhost"  # hoặc "127.0.0.1" - cùng máy nên không cần IP
RABBITMQ_PORT = 5672

# Queue mapping (khớp với CLUSTER_NODES trong system_worker.py)
KAFKA_QUEUE = "node_57"
SPARK_QUEUE = "spark"

# Cấu hình HDFS
HDFS_NAMENODE = "hdfs://192.168.80.127:9000"
HDFS_MODEL_DIR = "/bigdata/house_prices/models"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_service_ready(host, port, name, max_retries=10, delay=5):
    """Kiểm tra service TCP (Kafka/Spark/RabbitMQ) đã sẵn sàng"""
    for i in range(max_retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                if sock.connect_ex((host, port)) == 0:
                    print(f"✓ {name} đã sẵn sàng tại {host}:{port}!")
                    return True
            print(f"⏳ Chờ {name}... (lần {i+1}/{max_retries})")
        except Exception as e:
            print(f"❌ Lỗi khi kiểm tra {name}: {e}")
        time.sleep(delay)
    raise Exception(f"{name} không sẵn sàng tại {host}:{port}")


def check_kafka_ready(**_):
    """Kiểm tra Kafka đã sẵn sàng"""
    return check_service_ready(KAFKA_HOST, KAFKA_PORT, "Kafka", max_retries=30, delay=10)


def check_spark_ready(**_):
    """Kiểm tra Spark đã sẵn sàng"""
    return check_service_ready(SPARK_HOST, 7077, "Spark Master", max_retries=20, delay=10)


def check_rabbitmq_ready(**_):
    """Kiểm tra RabbitMQ đã sẵn sàng"""
    return check_service_ready(RABBITMQ_HOST, RABBITMQ_PORT, "RabbitMQ", max_retries=10, delay=5)


def wait_for_celery_result(result, timeout=600, poll_interval=5):
    """Đợi Celery task hoàn thành qua RabbitMQ"""
    elapsed = 0
    while elapsed < timeout:
        if result.ready():
            if result.successful():
                return result.result
            raise Exception(f"Celery task failed: {result.result}")
        time.sleep(poll_interval)
        elapsed += poll_interval
    raise TimeoutError(f"Celery task {result.id} timed out sau {timeout} giây")


def start_kafka_via_celery(**context):
    """
    Khởi động Kafka cluster trên node Kafka thông qua Celery/RabbitMQ
    Giả định có docker-compose kafka trên node, ví dụ: ~/kafka-cluster/docker-compose.yml
    """
    compose_path = "~/kafka-cluster/docker-compose.yml"
    print(f"🚀 Gửi lệnh docker-compose up Kafka tới queue '{KAFKA_QUEUE}' qua RabbitMQ")

    result = docker_compose_up.apply_async(
        args=[compose_path],
        kwargs={
            "services": None,   # tất cả services trong compose
            "detach": True,
            "build": False,
            "force_recreate": False,
        },
        queue=KAFKA_QUEUE,
    )

    output = wait_for_celery_result(result, timeout=600)
    print("✓ Kafka cluster đã được start qua Celery/RabbitMQ")
    return {
        "task_id": result.id,
        "queue": KAFKA_QUEUE,
        "compose_path": compose_path,
        "output": output,
    }


def ensure_kafka_topics_via_celery(**context):
    """
    Đảm bảo Kafka topics tồn tại bằng cách chạy lệnh trên node Kafka qua Celery
    """
    cmd = (
        "docker exec kafka kafka-topics --bootstrap-server localhost:9092 "
        "--create --if-not-exists --topic house-prices-input --replication-factor 1 --partitions 1 && "
        "docker exec kafka kafka-topics --bootstrap-server localhost:9092 "
        "--create --if-not-exists --topic house-prices-output --replication-factor 1 --partitions 1 && "
        "docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list"
    )
    print(f"🚀 Gửi lệnh tạo Kafka topics tới queue '{KAFKA_QUEUE}' qua RabbitMQ")

    result = run_command.apply_async(
        args=[cmd],
        kwargs={},
        queue=KAFKA_QUEUE,
    )

    output = wait_for_celery_result(result, timeout=300)
    print("✓ Kafka topics đã được đảm bảo qua Celery/RabbitMQ")
    return {
        "task_id": result.id,
        "queue": KAFKA_QUEUE,
        "command": cmd,
        "output": output,
    }


def start_spark_via_celery(**context):
    """
    Khởi động Spark master/worker trên node Spark thông qua Celery/RabbitMQ
    Giả định có docker-compose Spark trên node, ví dụ: ~/docker-spark/docker-compose.yml
    """
    compose_path = "~/docker-spark/docker-compose.yml"
    print(f"🚀 Gửi lệnh docker-compose up Spark tới queue '{SPARK_QUEUE}' qua RabbitMQ")

    result = docker_compose_up.apply_async(
        args=[compose_path],
        kwargs={
            "services": ["spark-master", "spark-worker"],
            "detach": True,
            "build": False,
            "force_recreate": False,
        },
        queue=SPARK_QUEUE,
    )

    output = wait_for_celery_result(result, timeout=600)
    print("✓ Spark cluster đã được start qua Celery/RabbitMQ")
    return {
        "task_id": result.id,
        "queue": SPARK_QUEUE,
        "compose_path": compose_path,
        "output": output,
    }


def send_prediction_start_message(**_):
    """Gửi message bắt đầu prediction qua RabbitMQ"""
    try:
        client = get_rabbitmq_client()
        client.connect()
        client.publish_message(
            queue_name='prediction_status',
            message={
                'status': 'started',
                'timestamp': datetime.now().isoformat(),
                'stage': 'prediction'
            }
        )
        client.close()
        print("✓ Đã gửi message bắt đầu prediction")
    except Exception as e:
        print(f"⚠️  Không thể gửi message: {e}")


def send_prediction_complete_message(**_):
    """Gửi message hoàn thành prediction qua RabbitMQ"""
    try:
        client = get_rabbitmq_client()
        client.connect()
        client.publish_message(
            queue_name='prediction_status',
            message={
                'status': 'completed',
                'timestamp': datetime.now().isoformat(),
                'stage': 'prediction'
            }
        )
        client.close()
        print("✓ Đã gửi message hoàn thành prediction")
    except Exception as e:
        print(f"⚠️  Không thể gửi message: {e}")


def wait_for_streaming_complete(**kwargs):
    """Đợi streaming hoàn thành"""
    print("⏳ Đợi 5 phút để streaming xử lý dữ liệu...")
    time.sleep(300)  # 5 phút
    print("✓ Hoàn thành thời gian streaming")


# ========================================
# DAG: PREDICT STREAMING
# ========================================
with DAG(
    'predict_streaming_pipeline',
    default_args=default_args,
    description='Streaming prediction pipeline với HDFS và RabbitMQ (không dùng SSH)',
    schedule=None,
    catchup=False,
    tags=['prediction', 'streaming', 'kafka', 'spark', 'rabbitmq'],
) as dag:

    # Task 1: Kiểm tra các service sẵn sàng
    check_rabbitmq = PythonOperator(
        task_id='check_rabbitmq',
        python_callable=check_rabbitmq_ready,
    )

    check_kafka = PythonOperator(
        task_id='check_kafka',
        python_callable=check_kafka_ready,
    )

    check_spark = PythonOperator(
        task_id='check_spark',
        python_callable=check_spark_ready,
    )

    # Task 2: Khởi động Kafka cluster qua Celery/RabbitMQ
    start_kafka_remote = PythonOperator(
        task_id='start_kafka_via_celery',
        python_callable=start_kafka_via_celery,
    )

    # Task 3: Đảm bảo Kafka topics tồn tại (qua Celery/RabbitMQ)
    ensure_kafka_topics = PythonOperator(
        task_id='ensure_kafka_topics_via_celery',
        python_callable=ensure_kafka_topics_via_celery,
    )

    # Task 4: Khởi động Spark cluster qua Celery/RabbitMQ
    start_spark_remote = PythonOperator(
        task_id='start_spark_via_celery',
        python_callable=start_spark_via_celery,
    )

    # Task 5: Gửi message bắt đầu prediction
    notify_prediction_start = PythonOperator(
        task_id='notify_prediction_start',
        python_callable=send_prediction_start_message,
    )

    # Task 6: Gửi dữ liệu streaming vào Kafka remote
    send_streaming_data = BashOperator(
        task_id='send_data_to_remote_kafka',
        bash_command=f"""
        cd {PROJECT_DIR}
        echo "📤 Gửi dữ liệu vào Kafka: {KAFKA_HOST}:{KAFKA_PORT}"
        python3 streaming/kafka_producer.py 1 200
        echo "✓ Đã gửi 200 records vào Kafka"
        """,
    )

    # Task 7: Khởi động Spark Streaming job (chạy trên node Spark hoặc local tùy cấu hình Spark master)
    start_streaming_job = BashOperator(
        task_id='start_streaming_job',
        bash_command=f"""
        cd {PROJECT_DIR}
        echo "🚀 Khởi động Spark Streaming job..."
        echo "HDFS Namenode: {HDFS_NAMENODE}"
        echo "HDFS Model Dir: {HDFS_MODEL_DIR}"
        echo "Kafka Bootstrap: {KAFKA_HOST}:{KAFKA_PORT}"

        # Xóa checkpoint cũ để đọc lại từ đầu
        rm -rf /tmp/checkpoint /tmp/checkpoint-house-prices-output

        # Chạy streaming job
        spark-submit \\
            --master {SPARK_MASTER} \\
            --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \\
            --driver-memory 4g \\
            --executor-memory 4g \\
            --num-executors 2 \\
            --executor-cores 2 \\
            --conf spark.hadoop.fs.defaultFS={HDFS_NAMENODE} \\
            --conf spark.hadoop.fs.hdfs.impl=org.apache.hadoop.hdfs.DistributedFileSystem \\
            spark_jobs/streaming_predict.py

        echo "✓ Streaming job đã hoàn thành"
        """,
        env={
            'HDFS_NAMENODE': HDFS_NAMENODE,
            'HDFS_MODEL_DIR': HDFS_MODEL_DIR,
            'KAFKA_BOOTSTRAP_SERVERS': f"{KAFKA_HOST}:{KAFKA_PORT}",
        },
        execution_timeout=timedelta(minutes=5)
    )

    # Task 8: Đợi streaming xử lý
    wait_processing = PythonOperator(
        task_id='wait_for_streaming',
        python_callable=wait_for_streaming_complete,
    )

    # Task 9: Gửi message hoàn thành prediction
    notify_prediction_complete = PythonOperator(
        task_id='notify_prediction_complete',
        python_callable=send_prediction_complete_message,
    )

    # Task 10: Cleanup (local)
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command="""
        if [ -f /tmp/spark_streaming.pid ]; then
            PID=$(cat /tmp/spark_streaming.pid)
            echo "🛑 Đang dừng Spark Streaming job (PID: $PID)"
            kill $PID 2>/dev/null || echo "Process đã dừng"
            rm -rf /tmp/checkpoint /tmp/checkpoint-house-prices-output
        fi
        echo "✓ Hoàn thành pipeline"
        """,
        trigger_rule='all_done'
    )

    # Định nghĩa dependencies
    [check_rabbitmq, check_kafka, check_spark] >> start_kafka_remote >> ensure_kafka_topics
    [check_spark] >> start_spark_remote
    [ensure_kafka_topics, start_spark_remote] >> notify_prediction_start >> send_streaming_data >> start_streaming_job >> wait_processing >> notify_prediction_complete >> cleanup

