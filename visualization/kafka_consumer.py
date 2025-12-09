"""
Consumer để đọc kết quả từ Kafka và trực quan hóa
"""
import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import numpy as np
from collections import deque
import time

class RealtimeVisualizer:
    def __init__(self, max_points=50):
        self.max_points = max_points
        self.ids = deque(maxlen=max_points)
        self.actual_prices = deque(maxlen=max_points)
        self.predicted_prices = deque(maxlen=max_points)
        self.errors = deque(maxlen=max_points)
        
        # Tạo figure với 2 subplots
        self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(12, 8))
        self.fig.suptitle('Real-time House Price Prediction', fontsize=16, fontweight='bold')
        
        # Setup consumer
        self.consumer = KafkaConsumer(
            'house-prices-output',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print("=" * 60)
        print("KAFKA CONSUMER - TRỰC QUAN HÓA KẾT QUẢ")
        print("=" * 60)
        print("✓ Đã kết nối đến Kafka topic: house-prices-output")
        print("📊 Đang hiển thị biểu đồ real-time...")
        print("=" * 60)
    
    def consume_messages(self, timeout_ms=100):
        """Đọc message từ Kafka"""
        messages = self.consumer.poll(timeout_ms=timeout_ms)
        
        for topic_partition, records in messages.items():
            for record in records:
                data = record.value
                
                self.ids.append(data['id'])
                self.actual_prices.append(data['actual_price'] * 100)  # Chuyển sang K$
                self.predicted_prices.append(data['predicted_price'] * 100)
                self.errors.append(abs(data['error']) * 100)
                
                # In thông tin
                print(f"📊 ID: {data['id']:4d} | "
                      f"Actual: ${data['actual_price']*100:6.2f}K | "
                      f"Predicted: ${data['predicted_price']*100:6.2f}K | "
                      f"Error: {data['error_percentage']:6.2f}%")
    
    def update_plot(self, frame):
        """Update biểu đồ"""
        self.consume_messages()
        
        if len(self.ids) == 0:
            return
        
        # Clear axes
        self.ax1.clear()
        self.ax2.clear()
        
        # Plot 1: So sánh giá thực tế vs dự đoán
        x = list(range(len(self.ids)))
        self.ax1.plot(x, self.actual_prices, 'bo-', label='Actual Price', linewidth=2, markersize=6)
        self.ax1.plot(x, self.predicted_prices, 'rs-', label='Predicted Price', linewidth=2, markersize=6)
        self.ax1.set_xlabel('Sample Index', fontsize=11)
        self.ax1.set_ylabel('Price ($1000s)', fontsize=11)
        self.ax1.set_title('Actual vs Predicted House Prices', fontsize=12, fontweight='bold')
        self.ax1.legend(loc='upper left')
        self.ax1.grid(True, alpha=0.3)
        
        # Plot 2: Sai số tuyệt đối
        self.ax2.bar(x, self.errors, color='coral', alpha=0.7)
        self.ax2.set_xlabel('Sample Index', fontsize=11)
        self.ax2.set_ylabel('Absolute Error ($1000s)', fontsize=11)
        self.ax2.set_title('Prediction Error', fontsize=12, fontweight='bold')
        self.ax2.grid(True, alpha=0.3, axis='y')
        
        # Tính toán metrics
        if len(self.errors) > 0:
            mae = np.mean(self.errors)
            rmse = np.sqrt(np.mean(np.array(self.errors) ** 2))
            
            # Hiển thị metrics
            metrics_text = f'MAE: ${mae:.2f}K | RMSE: ${rmse:.2f}K | Samples: {len(self.ids)}'
            self.fig.text(0.5, 0.02, metrics_text, ha='center', fontsize=11, 
                         bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout(rect=[0, 0.03, 1, 0.96])
    
    def run(self):
        """Chạy visualization"""
        ani = FuncAnimation(self.fig, self.update_plot, interval=1000, cache_frame_data=False)
        plt.show()

if __name__ == "__main__":
    try:
        visualizer = RealtimeVisualizer(max_points=100)
        visualizer.run()
    except KeyboardInterrupt:
        print("\n\n✓ Đã dừng consumer")
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")