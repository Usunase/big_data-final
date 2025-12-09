#!/bin/bash
# Script để chạy pipeline thủ công (không dùng Airflow)

PROJECT_DIR="/home/haminhchien/Documents/bigdata/final_project"
cd "$PROJECT_DIR"

echo "=========================================="
echo "ML STREAMING PIPELINE - CHẠY THỦ CÔNG"
echo "=========================================="
echo ""

# Màu sắc cho output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Bước 1: Khởi động Kafka
echo -e "${YELLOW}[Bước 1/6]${NC} Khởi động Kafka..."
cd docker
docker-compose up -d
cd ..
echo -e "${GREEN}✓${NC} Kafka đã khởi động"
echo "Đợi 10 giây để Kafka sẵn sàng..."
sleep 10

# Bước 2: Chuẩn bị dữ liệu
echo ""
echo -e "${YELLOW}[Bước 2/6]${NC} Chuẩn bị dữ liệu..."
if [ ! -f "data/train_data.csv" ]; then
    python data/prepare_data.py
    echo -e "${GREEN}✓${NC} Dữ liệu đã được chuẩn bị"
else
    echo -e "${GREEN}✓${NC} Dữ liệu đã có sẵn"
fi

# Bước 3: Huấn luyện mô hình
echo ""
echo -e "${YELLOW}[Bước 3/6]${NC} Huấn luyện mô hình Spark ML..."
spark-submit \
    --master local[*] \
    --driver-memory 4g \
    --executor-memory 4g \
    spark_jobs/train_model.py

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC} Mô hình đã được huấn luyện"
else
    echo -e "${RED}✗${NC} Lỗi khi huấn luyện mô hình"
    exit 1
fi

# Bước 4: Khởi động Spark Streaming (background)
echo ""
echo -e "${YELLOW}[Bước 4/6]${NC} Khởi động Spark Streaming job..."
nohup spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
    --driver-memory 4g \
    --executor-memory 4g \
    spark_jobs/streaming_predict.py > /tmp/spark_streaming.log 2>&1 &

SPARK_PID=$!
echo $SPARK_PID > /tmp/spark_streaming.pid
echo -e "${GREEN}✓${NC} Spark Streaming đã khởi động (PID: $SPARK_PID)"
echo "Đợi 20 giây để Spark Streaming sẵn sàng..."
sleep 20

# Bước 5: Gửi dữ liệu streaming
echo ""
echo -e "${YELLOW}[Bước 5/6]${NC} Gửi dữ liệu streaming vào Kafka..."
echo "Tham số: interval=1s, num_records=200"
python streaming/kafka_producer.py 1 200
echo -e "${GREEN}✓${NC} Đã gửi dữ liệu streaming"

# Bước 6: Hướng dẫn visualization
echo ""
echo -e "${YELLOW}[Bước 6/6]${NC} Trực quan hóa kết quả"
echo ""
echo "Để xem kết quả trực quan, chạy lệnh sau trong terminal khác:"
echo -e "${GREEN}python visualization/kafka_consumer.py${NC}"
echo ""
echo "Hoặc đợi 5 phút để xem logs:"
echo "tail -f /tmp/spark_streaming.log"
echo ""
echo "=========================================="
echo -e "${GREEN}PIPELINE ĐÃ HOÀN THÀNH!${NC}"
echo "=========================================="
echo ""
echo "Để dừng Spark Streaming job:"
echo "kill \$(cat /tmp/spark_streaming.pid)"
echo ""
echo "Để dừng Kafka:"
echo "cd docker && docker-compose down"

