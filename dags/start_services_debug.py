"""
DAG phụ để khởi động và kiểm tra Kafka/Spark riêng lẻ,
sử dụng Celery task trong `mycelery.system_worker` (chạy trên
queue kafka_queue / spark_queue) thay vì BashOperator cục bộ.
"""
from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import time

from mycelery.system_worker import run_command

SPARK_MASTER = "spark://192.168.80.207:7077"
SPARK_HOME = "/home/nindang/spark-4.0.0-bin-hadoop3"


def wait_for_celery_result(result, timeout=300, poll_interval=3):
    """Poll Celery AsyncResult với timeout."""
    elapsed = 0
    while elapsed < timeout:
        if result.ready():
            if result.successful():
                return result.result
            raise Exception(f"Celery task failed: {result.result}")
        time.sleep(poll_interval)
        elapsed += poll_interval
    raise TimeoutError(f"Celery task {result.id} timed out after {timeout}s")


def start_kafka_task(**_):
    """Khởi động Kafka/ZK qua docker compose trên kafka_queue."""
    cmd = r"""
    set -e
    cd ~/kafka-cluster
    docker compose down || true
    sleep 2
    docker compose up -d
    # Wait ~30s for services
    for i in $(seq 1 15); do
        if docker ps | grep -E "kafka|zookeeper" | grep -q "Up"; then
            break
        fi
        sleep 2
    done
    docker ps | grep -E "kafka|zookeeper"
    """
    async_result = run_command.apply_async(args=[cmd], queue="kafka_queue")
    return wait_for_celery_result(async_result, timeout=180)


def start_spark_task(**_):
    """Khởi động Spark Master/Worker trên spark_queue."""
    cmd = fr"""
    set -e
    export SPARK_HOME={SPARK_HOME}
    if [ ! -d "$SPARK_HOME" ]; then
      echo "SPARK_HOME not found: $SPARK_HOME"; exit 1;
    fi
    $SPARK_HOME/sbin/stop-worker.sh 2>/dev/null || true
    $SPARK_HOME/sbin/stop-master.sh 2>/dev/null || true
    pkill -9 -f "org.apache.spark.deploy" 2>/dev/null || true
    sleep 2
    $SPARK_HOME/sbin/start-master.sh
    sleep 5
    jps | grep Master
    $SPARK_HOME/sbin/start-worker.sh {SPARK_MASTER}
    sleep 5
    jps | grep Worker
    """
    async_result = run_command.apply_async(args=[cmd], queue="spark_queue")
    return wait_for_celery_result(async_result, timeout=240)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "start_services_debug",
    default_args=default_args,
    description="Start Kafka/Spark separately for debugging (Celery remote)",
    schedule=None,
    catchup=False,
    tags=["debug", "kafka", "spark"],
) as dag:
    start_kafka_only = PythonOperator(
        task_id="start_kafka_only",
        python_callable=start_kafka_task,
    )

    start_spark_only = PythonOperator(
        task_id="start_spark_only",
        python_callable=start_spark_task,
    )

    # Không đặt dependency để có thể chạy độc lập từ UI