"""
Airflow DAG cho quá trình huấn luyện mô hình
Sử dụng RabbitMQ để giao tiếp giữa các service
Kiến trúc:
- Máy Airflow (localhost): Chạy Airflow, RabbitMQ
- Máy Hadoop (192.168.80.127): Chạy HDFS
- Máy Spark (192.168.80.207): Chạy Spark
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

# ========================================
# CẤU HÌNH HỆ THỐNG PHÂN TÁN
# ========================================
# Theo CLUSTER_NODES trong system_worker.py
HADOOP_HOST = "192.168.80.127"  # hadoop-namenode, hadoop-datanode
SPARK_HOST, SPARK_USER = "192.168.80.207", "nindang"  # spark-master, spark-worker
SPARK_MASTER = f"spark://{SPARK_HOST}:7077"
PROJECT_DIR = "/home/haminhchien/Documents/bigdata/final_project"

# RabbitMQ chạy trên máy Airflow (cùng máy) - không cần IP, dùng localhost
RABBITMQ_HOST = "localhost"  # hoặc "127.0.0.1" - cùng máy nên không cần IP
RABBITMQ_PORT = 5672

# Cấu hình HDFS
HDFS_NAMENODE = "hdfs://192.168.80.127:9000"
HDFS_DATA_DIR = "/bigdata/house_prices"
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
    """Kiểm tra service đã sẵn sàng"""
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

def check_hadoop_ready(**_):
    """Kiểm tra Hadoop HDFS đã sẵn sàng"""
    return check_service_ready(HADOOP_HOST, 9000, "Hadoop NameNode", max_retries=20, delay=10)

def check_spark_ready(**_):
    """Kiểm tra Spark đã sẵn sàng"""
    return check_service_ready(SPARK_HOST, 7077, "Spark Master", max_retries=20, delay=10)

def check_rabbitmq_ready(**_):
    """Kiểm tra RabbitMQ đã sẵn sàng"""
    return check_service_ready(RABBITMQ_HOST, RABBITMQ_PORT, "RabbitMQ", max_retries=10, delay=5)

def send_training_start_message(**_):
    """Gửi message bắt đầu training qua RabbitMQ"""
    try:
        # RabbitMQ chạy trên cùng máy nên không cần truyền host/port (dùng default localhost)
        client = get_rabbitmq_client()
        client.connect()
        client.publish_message(
            queue_name='training_status',
            message={
                'status': 'started',
                'timestamp': datetime.now().isoformat(),
                'stage': 'training'
            }
        )
        client.close()
        print("✓ Đã gửi message bắt đầu training")
    except Exception as e:
        print(f"⚠️  Không thể gửi message: {e}")

def send_training_complete_message(**_):
    """Gửi message hoàn thành training qua RabbitMQ"""
    try:
        # RabbitMQ chạy trên cùng máy nên không cần truyền host/port (dùng default localhost)
        client = get_rabbitmq_client()
        client.connect()
        client.publish_message(
            queue_name='training_status',
            message={
                'status': 'completed',
                'timestamp': datetime.now().isoformat(),
                'stage': 'training',
                'model_path': f"{HDFS_MODEL_DIR}/house_price_model"
            }
        )
        client.close()
        print("✓ Đã gửi message hoàn thành training")
    except Exception as e:
        print(f"⚠️  Không thể gửi message: {e}")

# ========================================
# DAG: TRAIN MODEL
# ========================================
with DAG(
    'train_model_pipeline',
    default_args=default_args,
    description='Train model pipeline với HDFS và RabbitMQ',
    schedule=None,
    catchup=False,
    tags=['training', 'hdfs', 'spark', 'rabbitmq'],
) as dag:
    
    # Task 1: Kiểm tra các service sẵn sàng
    check_rabbitmq = PythonOperator(
        task_id='check_rabbitmq',
        python_callable=check_rabbitmq_ready,
    )
    
    check_hadoop = PythonOperator(
        task_id='check_hadoop',
        python_callable=check_hadoop_ready,
    )
    
    check_spark = PythonOperator(
        task_id='check_spark',
        python_callable=check_spark_ready,
    )
    
    # Task 2: Chuẩn bị dữ liệu local
    prepare_data = BashOperator(
        task_id='prepare_data',
        bash_command=f"""
        cd {PROJECT_DIR}
        if [ -f data/train_data.csv ]; then
            echo "✓ Dữ liệu đã có sẵn"
        else
            echo "📊 Đang chuẩn bị dữ liệu..."
            python data/prepare_data.py
        fi
        """,
    )
    
    # Task 3: Upload dữ liệu lên HDFS
    upload_to_hdfs = BashOperator(
        task_id='upload_to_hdfs',
        bash_command=f"""
        cd {PROJECT_DIR}
        echo "📤 Đang upload dữ liệu lên HDFS..."
        python data/upload_to_hdfs.py
        echo "✓ Đã upload dữ liệu lên HDFS"
        """,
    )
    
    # Task 4: Gửi message bắt đầu training
    notify_training_start = PythonOperator(
        task_id='notify_training_start',
        python_callable=send_training_start_message,
    )
    
    # Task 5: Huấn luyện mô hình trên Spark cluster
    train_model = BashOperator(
        task_id='train_model',
        bash_command=f"""
        cd {PROJECT_DIR}
        echo "🚀 Gửi training job đến Spark: {SPARK_MASTER}"
        echo "HDFS Namenode: {HDFS_NAMENODE}"
        echo "HDFS Data Dir: {HDFS_DATA_DIR}"
        echo "HDFS Model Dir: {HDFS_MODEL_DIR}"
        
        spark-submit --master {SPARK_MASTER} \\
            --conf spark.hadoop.fs.defaultFS={HDFS_NAMENODE} \\
            --conf spark.local.dir=/tmp/spark_local \\
            --driver-memory 4g \\
            --executor-memory 4g \\
            --num-executors 2 \\
            --executor-cores 2 \\
            --conf spark.hadoop.fs.hdfs.impl=org.apache.hadoop.hdfs.DistributedFileSystem \\
            spark_jobs/train_model.py
        echo "✓ Training hoàn thành"
        """,
        env={
            'HDFS_NAMENODE': HDFS_NAMENODE,
            'HDFS_DATA_DIR': HDFS_DATA_DIR,
            'HDFS_MODEL_DIR': HDFS_MODEL_DIR,
        }
    )
    
    # Task 6: Gửi message hoàn thành training
    notify_training_complete = PythonOperator(
        task_id='notify_training_complete',
        python_callable=send_training_complete_message,
    )
    
    # Định nghĩa dependencies
    [check_rabbitmq, check_hadoop, check_spark] >> prepare_data >> upload_to_hdfs >> notify_training_start >> train_model >> notify_training_complete

