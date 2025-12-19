"""
RabbitMQ client để giao tiếp giữa các service
"""
import pika
import json
import logging
from typing import Dict, Any, Optional, Callable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RabbitMQClient:
    """Client để giao tiếp với RabbitMQ"""
    
    def __init__(self, host='localhost', port=5672, username='guest', password='guest'):
        """
        Khởi tạo RabbitMQ client
        
        Args:
            host: RabbitMQ host
            port: RabbitMQ port
            username: Username
            password: Password
        """
        self.host = host
        self.port = port
        self.credentials = pika.PlainCredentials(username, password)
        self.connection = None
        self.channel = None
    
    def connect(self):
        """Kết nối đến RabbitMQ"""
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    credentials=self.credentials
                )
            )
            self.channel = self.connection.channel()
            logger.info(f"✓ Đã kết nối đến RabbitMQ tại {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi khi kết nối RabbitMQ: {e}")
            return False
    
    def close(self):
        """Đóng kết nối"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("✓ Đã đóng kết nối RabbitMQ")
    
    def declare_queue(self, queue_name: str, durable: bool = True):
        """Khai báo queue"""
        if not self.channel:
            self.connect()
        
        self.channel.queue_declare(queue=queue_name, durable=durable)
        logger.info(f"✓ Đã khai báo queue: {queue_name}")
    
    def publish_message(self, queue_name: str, message: Dict[Any, Any], persistent: bool = True):
        """
        Gửi message vào queue
        
        Args:
            queue_name: Tên queue
            message: Dictionary chứa message
            persistent: Message có persistent không
        """
        if not self.channel:
            self.connect()
        
        self.declare_queue(queue_name)
        
        properties = pika.BasicProperties(
            delivery_mode=2 if persistent else 1  # 2 = persistent
        )
        
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=properties
        )
        logger.info(f"✓ Đã gửi message vào queue {queue_name}")
    
    def consume_messages(self, queue_name: str, callback: Callable, auto_ack: bool = False):
        """
        Consume messages từ queue
        
        Args:
            queue_name: Tên queue
            callback: Hàm xử lý message (ch_method, ch_properties, ch_body)
            auto_ack: Tự động acknowledge
        """
        if not self.channel:
            self.connect()
        
        self.declare_queue(queue_name)
        
        def message_handler(ch, method, properties, body):
            try:
                message = json.loads(body.decode('utf-8'))
                callback(ch, method, properties, message)
                if not auto_ack:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error(f"❌ Lỗi khi xử lý message: {e}")
                if not auto_ack:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=message_handler,
            auto_ack=auto_ack
        )
        
        logger.info(f"⏳ Đang chờ messages từ queue {queue_name}...")
        self.channel.start_consuming()
    
    def stop_consuming(self):
        """Dừng consume messages"""
        if self.channel:
            self.channel.stop_consuming()
            logger.info("✓ Đã dừng consume messages")

# Singleton instance
_client = None

def get_rabbitmq_client() -> RabbitMQClient:
    """Lấy singleton instance của RabbitMQ client"""
    global _client
    if _client is None:
        _client = RabbitMQClient()
    return _client

