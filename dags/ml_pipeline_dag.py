"""
Airflow DAG cho hệ thống phân tán (FIXED VERSION)
- Machine 1 (192.168.80.148): Airflow orchestrator
- Machine 2 (192.168.80.127): Kafka cluster
- Machine 3 (192.168.80.207): Spark cluster
"""
from airflow import DAG
try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import time, socket

# ========================================
# CẤU HÌNH HỆ THỐNG PHÂN TÁN
# ========================================
KAFKA_HOST, KAFKA_PORT, KAFKA_USER = "192.168.80.127", 9092, "nindang"
SPARK_HOST, SPARK_USER = "192.168.80.207", "nindang"
SPARK_MASTER = f"spark://{SPARK_HOST}:7077"
PROJECT_DIR = "/home/haminhchien/Documents/bigdata/final_project"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Hàm kiểm tra sẵn sàng cho một service bất kỳ
def check_remote_ready(host, port, name, max_retries=10, delay=5):
    print(f"🔍 Kiểm tra {name} tại {host}:{port}")
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
    return check_remote_ready(KAFKA_HOST, KAFKA_PORT, "Kafka", max_retries=30, delay=10)

def check_spark_ready(**_):
    return check_remote_ready(SPARK_HOST, 7077, "Spark Master")
def wait_for_streaming_complete(**kwargs):
    """Đợi streaming hoàn thành (hoặc timeout)"""
    print("⏳ Đợi 5 phút để streaming xử lý dữ liệu...")
    time.sleep(300)  # 5 phút
    print("✓ Hoàn thành thời gian streaming")
# ========================================
# DAG CHÍNH
# ========================================
with DAG(
    'ml_streaming_pipeline_distributed',
    default_args=default_args,
    description='Distributed ML pipeline: Airflow -> Kafka (remote) -> Spark (remote)',
    schedule=None,
    catchup=False,
    tags=['distributed', 'machine-learning', 'kafka', 'spark'],
) as dag:
    
    # Task 1: Khởi động Kafka trên máy remote
    start_kafka_remote = BashOperator(
        task_id='start_kafka_remote',
        bash_command=f"""
        echo "=========================================="
        echo "KHỞI ĐỘNG KAFKA CLUSTER - {KAFKA_HOST}"
        echo "=========================================="
        ssh {KAFKA_USER}@{KAFKA_HOST} '
        cd ~/Documents/bigdata/final_project/docker && \
        (docker compose down || docker-compose down || true) && \
        (docker compose up -d || docker-compose up -d) && \
        sleep 10 && docker ps | grep -E "kafka|zookeeper"
        '
        echo "✓ Kafka remote đã khởi động"
        """
    )
    
    # Task 2: Kiểm tra Kafka đã sẵn sàng
    check_kafka = PythonOperator(
        task_id='check_kafka_remote',
        python_callable=check_kafka_ready,
    )
    # Task 2b: Đảm bảo Kafka topic output tồn tại
    ensure_kafka_output_topic = BashOperator(
        task_id='ensure_kafka_output_topic',
        bash_command=f"""
        echo "=========================================="
        echo "ĐẢM BẢO TOPIC house-prices-output TỒN TẠI"
        echo "=========================================="
        ssh {KAFKA_USER}@{KAFKA_HOST} '
        set -e
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 \\
            --create --if-not-exists \\
            --topic house-prices-output \\
            --replication-factor 1 \\
            --partitions 1
        echo ""
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic house-prices-output
        '
        echo "✓ Kafka topic house-prices-output đã sẵn sàng"
        """
    )
    # Task 3: Khởi động Spark cluster trên máy remote
    start_spark_remote = BashOperator(
        task_id='start_spark_remote',
        bash_command=f"""
        echo "=========================================="
        echo "KHỞI ĐỘNG SPARK CLUSTER - {SPARK_HOST}"
        echo "=========================================="
        ssh {SPARK_USER}@{SPARK_HOST} '
        export SPARK_HOME=/home/nindang/spark-4.0.0-bin-hadoop3
        [ ! -d "$SPARK_HOME" ] && echo "❌ SPARK_HOME not found" && exit 1
        $SPARK_HOME/sbin/stop-worker.sh 2>/dev/null || true
        $SPARK_HOME/sbin/stop-master.sh 2>/dev/null || true
        sleep 5
        $SPARK_HOME/sbin/start-master.sh && sleep 5
        $SPARK_HOME/sbin/start-worker.sh {SPARK_MASTER} && sleep 5
        jps | grep -E "Master|Worker"
        '
        echo "✓ Spark remote đã khởi động"
        """
    )
    
    # Task 4: Kiểm tra Spark đã sẵn sàng
    check_spark = PythonOperator(
        task_id='check_spark_remote',
        python_callable=check_spark_ready,
    )
    
    # Task 5: Chuẩn bị dữ liệu (local - trên máy Airflow)
    prepare_data = BashOperator(
        task_id='prepare_data',
        bash_command="""
        cd {{ params.project_dir }}
        if [ -f data/train_data.csv ]; then
            echo "✓ Dữ liệu đã có sẵn"
        else
            echo "📊 Đang chuẩn bị dữ liệu..."
            python data/prepare_data.py
        fi
        """,
        params={'project_dir': PROJECT_DIR}
    )
    
    # Task 6: Huấn luyện mô hình trên Spark cluster remote
    train_model = BashOperator(
        task_id='train_model',
        bash_command="""
        cd {{ params.project_dir }}
        echo "🚀 Gửi training job đến Spark: {{ params.spark_master }}"
        spark-submit \
            --master {{ params.spark_master }} \
            --conf spark.hadoop.fs.defaultFS=file:/// \
            --conf spark.local.dir=/tmp/spark_local \
            --driver-memory 4g \
            --executor-memory 4g \
            --num-executors 2 \
            --executor-cores 2 \
            spark_jobs/train_model.py
        echo "✓ Training hoàn thành"
        """,
        params={'project_dir': PROJECT_DIR, 'spark_master': SPARK_MASTER}
    )
    
    # Task 7: Gửi dữ liệu streaming vào Kafka remote
    send_streaming_data = BashOperator(
        task_id='send_data_to_remote_kafka',
        bash_command=f"""
        cd {PROJECT_DIR}
        echo "📤 Gửi dữ liệu vào Kafka: {KAFKA_HOST}:{KAFKA_PORT}"
        python streaming/kafka_producer.py 1 200
        echo "✓ Đã gửi 200 records vào Kafka"
        """
    )
    
    # Task 8: Khởi động Spark Streaming job (tự động dừng sau 2 phút)
    # LƯU Ý: streaming_predict.py phải có timeout logic (xem artifact streaming_predict_fixed)
    start_streaming_job = BashOperator(
        task_id='start_streaming_job',
        bash_command="""
        cd {{ params.project_dir }}
        echo "🚀 Khởi động Spark Streaming job..."

        # Xóa checkpoint cũ để đọc lại từ đầu (tránh giữ offset cũ làm trống output)
        rm -rf /tmp/checkpoint /tmp/checkpoint-house-prices-output
        
        # Chạy streaming job (foreground - đợi nó tự dừng)
        spark-submit \
            --master {{ params.spark_master }} \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
            --driver-memory 4g \
            --executor-memory 4g \
            --num-executors 2 \
            --executor-cores 2 \
            spark_jobs/streaming_predict.py
        
        echo "✓ Streaming job đã hoàn thành"
        """,
        params={
            'project_dir': PROJECT_DIR,
            'spark_master': SPARK_MASTER,
        },
        execution_timeout=timedelta(minutes=5)  # Timeout sau 5 phút
    )
    wait_processing = PythonOperator(
        task_id='wait_for_streaming',
        python_callable=wait_for_streaming_complete,
    )
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
        trigger_rule='all_done'  # Chạy dù task trước thành công hay thất bại
    )
    
    # Định nghĩa dependencies
    start_kafka_remote >> check_kafka >> ensure_kafka_output_topic
    start_spark_remote >> check_spark
    [ensure_kafka_output_topic, check_spark] >> prepare_data >> train_model >> send_streaming_data >> start_streaming_job >> wait_processing >> cleanup


# ========================================
# DAG VISUALIZATION
# ========================================
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
        params={'project_dir': PROJECT_DIR}
    )