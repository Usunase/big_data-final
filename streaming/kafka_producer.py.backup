"""
Chương trình mô phỏng streaming - gửi dữ liệu vào Kafka
"""
import json
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import sys

def create_producer(max_retries=10):
    """Tạo Kafka producer với retry logic"""
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['192.168.80.127:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                api_version=(2, 5, 0)
            )
            print("✓ Đã kết nối đến Kafka broker")
            return producer
        except NoBrokersAvailable:
            if i < max_retries - 1:
                print(f"⏳ Đang chờ Kafka khởi động... (thử lần {i+1}/{max_retries})")
                time.sleep(5)
            else:
                print("❌ Không thể kết nối đến Kafka sau nhiều lần thử")
                raise

def send_streaming_data(interval=2, num_records=None):
    """
    Gửi dữ liệu streaming vào Kafka
    
    Args:
        interval: Khoảng thời gian giữa các lần gửi (giây)
        num_records: Số lượng bản ghi gửi (None = gửi tất cả)
    """
    # Đọc dữ liệu streaming
    df = pd.read_csv('data/streaming_data.csv')
    
    if num_records:
        df = df.head(num_records)
    
    print("=" * 60)
    print("KAFKA PRODUCER - MÔ PHỎNG STREAMING")
    print("=" * 60)
    print(f"Số lượng bản ghi sẽ gửi: {len(df)}")
    print(f"Khoảng thời gian: {interval} giây/bản ghi")
    print(f"Topic: house-prices-input")
    print("=" * 60)
    
    # Tạo producer
    producer = create_producer()
    
    try:
        for idx, row in df.iterrows():
            # Tạo message (không bao gồm target - để mô hình dự đoán)
            message = {
                'id': idx,
                'MedInc': float(row['MedInc']),
                'HouseAge': float(row['HouseAge']),
                'AveRooms': float(row['AveRooms']),
                'AveBedrms': float(row['AveBedrms']),
                'Population': float(row['Population']),
                'AveOccup': float(row['AveOccup']),
                'Latitude': float(row['Latitude']),
                'Longitude': float(row['Longitude']),
                'actual_price': float(row['target'])  # Giá thực tế để so sánh
            }
            
            # Gửi vào Kafka
            producer.send('house-prices-input', value=message)
            
            print(f"📤 Đã gửi bản ghi {idx+1}/{len(df)} | "
                  f"MedInc={message['MedInc']:.2f} | "
                  f"Actual Price=${message['actual_price']*100:.2f}K")
            
            time.sleep(interval)
        
        producer.flush()
        print("\n✓ Đã gửi tất cả dữ liệu!")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Đã dừng streaming")
    finally:
        producer.close()
        print("✓ Đã đóng producer")

if __name__ == "__main__":
    # Có thể truyền tham số từ command line
    interval = float(sys.argv[1]) if len(sys.argv) > 1 else 2
    num_records = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    send_streaming_data(interval=interval, num_records=num_records)