"""
Service để load Spark ML model và thực hiện dự đoán giá nhà
"""
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql import Row
import os

class HousePricePredictor:
    def __init__(self, model_path="models/house_price_model"):
        self.model_path = model_path
        self.spark = None
        self.model = None
        self._initialize()
    
    def _initialize(self):
        """Khởi tạo Spark session và load model"""
        try:
            # Khởi tạo Spark Session với cấu hình tối ưu cho web service
            self.spark = SparkSession.builder \
                .appName("HousePricePredictionService") \
                .config("spark.hadoop.fs.defaultFS", "file:///") \
                .config("spark.local.dir", "/tmp/spark_local") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("ERROR")
            
            # Load model
            if not os.path.exists(self.model_path):
                raise FileNotFoundError(f"Model không tồn tại tại: {self.model_path}")
            
            self.model = PipelineModel.load(self.model_path)
            print(f"✓ Đã tải model thành công từ: {self.model_path}")
            
        except Exception as e:
            print(f"❌ Lỗi khi khởi tạo predictor: {e}")
            raise
    
    def predict(self, med_inc, house_age, ave_rooms, ave_bedrms, 
                population, ave_occup, latitude, longitude):
        """
        Dự đoán giá nhà dựa trên các đặc trưng
        
        Args:
            med_inc: Thu nhập trung bình
            house_age: Tuổi nhà
            ave_rooms: Số phòng trung bình
            ave_bedrms: Số phòng ngủ trung bình
            population: Dân số
            ave_occup: Mật độ cư trú trung bình
            latitude: Vĩ độ
            longitude: Kinh độ
        
        Returns:
            float: Giá nhà dự đoán (đơn vị: trăm nghìn USD)
        """
        try:
            # Tạo DataFrame từ dữ liệu đầu vào
            data = Row(
                MedInc=float(med_inc),
                HouseAge=float(house_age),
                AveRooms=float(ave_rooms),
                AveBedrms=float(ave_bedrms),
                Population=float(population),
                AveOccup=float(ave_occup),
                Latitude=float(latitude),
                Longitude=float(longitude)
            )
            
            df = self.spark.createDataFrame([data])
            
            # Thực hiện dự đoán
            predictions = self.model.transform(df)
            
            # Lấy kết quả dự đoán
            result = predictions.select("prediction").collect()[0][0]
            
            return float(result)
            
        except Exception as e:
            print(f"❌ Lỗi khi dự đoán: {e}")
            raise
    
    def predict_batch(self, data_list):
        """
        Dự đoán hàng loạt
        
        Args:
            data_list: List of dicts, mỗi dict chứa các features
        
        Returns:
            List of predictions
        """
        try:
            rows = []
            for data in data_list:
                rows.append(Row(
                    MedInc=float(data['MedInc']),
                    HouseAge=float(data['HouseAge']),
                    AveRooms=float(data['AveRooms']),
                    AveBedrms=float(data['AveBedrms']),
                    Population=float(data['Population']),
                    AveOccup=float(data['AveOccup']),
                    Latitude=float(data['Latitude']),
                    Longitude=float(data['Longitude'])
                ))
            
            df = self.spark.createDataFrame(rows)
            predictions = self.model.transform(df)
            
            results = [float(row.prediction) for row in predictions.select("prediction").collect()]
            return results
            
        except Exception as e:
            print(f"❌ Lỗi khi dự đoán hàng loạt: {e}")
            raise
    
    def close(self):
        """Đóng Spark session"""
        if self.spark:
            self.spark.stop()
            self.spark = None

# Global predictor instance
_predictor = None

def get_predictor():
    """Lấy singleton instance của predictor"""
    global _predictor
    if _predictor is None:
        _predictor = HousePricePredictor()
    return _predictor

