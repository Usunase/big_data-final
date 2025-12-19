#!/bin/bash
# Script để start Celery worker cho Kafka trên máy Kafka
# Usage: ./start_kafka_worker.sh <HOSTNAME_AIRFLOW>

if [ $# -ne 1 ]; then
    echo "Usage: $0 <HOSTNAME_AIRFLOW>"
    echo "Example: $0 airflow-master"
    exit 1
fi

HOSTNAME_AIRFLOW=$1
# Tự động detect project directory từ script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "Start Celery Worker cho Kafka"
echo "=========================================="
echo "RabbitMQ Host: $HOSTNAME_AIRFLOW"
echo "Queue: node_57"
echo ""

# Set broker URL
export CELERY_BROKER_URL="amqp://guest:guest@${HOSTNAME_AIRFLOW}:5672//"

# Test kết nối
echo "Test kết nối tới RabbitMQ..."
if ! timeout 5 bash -c "echo > /dev/tcp/${HOSTNAME_AIRFLOW}/5672" 2>/dev/null; then
    echo "❌ Không thể kết nối tới $HOSTNAME_AIRFLOW:5672"
    echo "Kiểm tra:"
    echo "  1. /etc/hosts đã có entry cho $HOSTNAME_AIRFLOW chưa?"
    echo "  2. RabbitMQ đang chạy trên máy Airflow chưa?"
    echo "  3. Firewall đã mở port 5672 chưa?"
    exit 1
fi

echo "✓ Kết nối thành công!"

# Chuyển đến thư mục project
cd "$PROJECT_DIR" || {
    echo "❌ Không tìm thấy thư mục: $PROJECT_DIR"
    echo "Vui lòng sửa PROJECT_DIR trong script này"
    exit 1
}

# Start worker
echo ""
echo "Đang start Celery worker..."
celery -A mycelery.system_worker worker \
  -Q node_57 \
  -n kafka@%h \
  --loglevel=INFO \
  --broker="$CELERY_BROKER_URL"

