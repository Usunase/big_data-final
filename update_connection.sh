#!/bin/bash
# Script tự động cập nhật địa chỉ Kafka trong các file Python
# Chạy trên máy Airflow (192.168.80.148)

PROJECT_DIR="/home/haminhchien/Documents/bigdata/final_project"
KAFKA_HOST="192.168.80.127:9092"

echo "=========================================="
echo "CẬP NHẬT KAFKA CONNECTIONS"
echo "=========================================="
echo "Project: $PROJECT_DIR"
echo "Kafka: $KAFKA_HOST"
echo ""

cd $PROJECT_DIR

# Backup files trước
echo "📦 Backup files..."
cp streaming/kafka_producer.py streaming/kafka_producer.py.backup
cp spark_jobs/streaming_predict.py spark_jobs/streaming_predict.py.backup
cp visualization/kafka_consumer.py visualization/kafka_consumer.py.backup

# 1. Update kafka_producer.py
echo "🔧 Cập nhật streaming/kafka_producer.py..."
sed -i "s/bootstrap_servers=\['localhost:9092'\]/bootstrap_servers=['$KAFKA_HOST']/g" streaming/kafka_producer.py
sed -i "s/bootstrap_servers=\[\"localhost:9092\"\]/bootstrap_servers=['$KAFKA_HOST']/g" streaming/kafka_producer.py
sed -i "s/bootstrap_servers=\['192.168.80.127:9092'\]/bootstrap_servers=['$KAFKA_HOST']/g" streaming/kafka_producer.py

# 2. Update streaming_predict.py
echo "🔧 Cập nhật spark_jobs/streaming_predict.py..."
sed -i 's/option("kafka.bootstrap.servers", "localhost:9092")/option("kafka.bootstrap.servers", "'"$KAFKA_HOST"'")/g' spark_jobs/streaming_predict.py
sed -i 's/option("kafka.bootstrap.servers", "192.168.80.127:9092")/option("kafka.bootstrap.servers", "'"$KAFKA_HOST"'")/g' spark_jobs/streaming_predict.py

# 3. Update kafka_consumer.py
echo "🔧 Cập nhật visualization/kafka_consumer.py..."
sed -i "s/bootstrap_servers=\['localhost:9092'\]/bootstrap_servers=['$KAFKA_HOST']/g" visualization/kafka_consumer.py
sed -i "s/bootstrap_servers=\[\"localhost:9092\"\]/bootstrap_servers=['$KAFKA_HOST']/g" visualization/kafka_consumer.py
sed -i "s/bootstrap_servers=\['192.168.80.127:9092'\]/bootstrap_servers=['$KAFKA_HOST']/g" visualization/kafka_consumer.py

echo ""
echo "✓ Đã cập nhật tất cả file"
echo ""
echo "Kiểm tra thay đổi:"
echo "-------------------------------------------"
echo "kafka_producer.py:"
grep -n "bootstrap_servers" streaming/kafka_producer.py | head -1

echo ""
echo "streaming_predict.py:"
grep -n "kafka.bootstrap.servers" spark_jobs/streaming_predict.py | head -2

echo ""
echo "kafka_consumer.py:"
grep -n "bootstrap_servers" visualization/kafka_consumer.py | head -1

echo ""
echo "=========================================="
echo "✓✓✓ HOÀN THÀNH CẬP NHẬT ✓✓✓"
echo "=========================================="
echo ""
echo "📝 Backup files tại:"
echo "  - streaming/kafka_producer.py.backup"
echo "  - spark_jobs/streaming_predict.py.backup"
echo "  - visualization/kafka_consumer.py.backup"