# Hướng Dẫn Sử Dụng Web UI Dự Đoán Giá Nhà

## Tổng Quan

Web UI này cho phép người dùng nhập các thông tin về ngôi nhà và nhận được dự đoán giá nhà từ mô hình Random Forest đã được huấn luyện bằng Spark ML.

## Yêu Cầu

1. Python 3.7+
2. Spark đã được cài đặt
3. Mô hình đã được huấn luyện và lưu tại `models/house_price_model`

## Cài Đặt

1. Cài đặt các dependencies:
```bash
pip install -r requirements.txt
```

## Chạy Web UI

1. Đảm bảo mô hình đã được huấn luyện:
```bash
python spark_jobs/train_model.py
```

2. Chạy web server:
```bash
python ui.py
```

3. Mở trình duyệt và truy cập:
```
http://localhost:5000
```

## Sử Dụng

1. Điền đầy đủ các thông tin vào form:
   - **MedInc**: Thu nhập trung bình (ví dụ: 3.5)
   - **HouseAge**: Tuổi nhà (ví dụ: 30.0)
   - **AveRooms**: Số phòng trung bình (ví dụ: 5.5)
   - **AveBedrms**: Số phòng ngủ trung bình (ví dụ: 1.0)
   - **Population**: Dân số (ví dụ: 2000)
   - **AveOccup**: Mật độ cư trú trung bình (ví dụ: 3.0)
   - **Latitude**: Vĩ độ (ví dụ: 34.05)
   - **Longitude**: Kinh độ (ví dụ: -118.24)

2. Nhấn nút "🔮 Dự Đoán Giá Nhà"

3. Kết quả sẽ hiển thị giá nhà dự đoán (đơn vị: USD)

## API Endpoints

### POST /predict
Dự đoán giá nhà từ dữ liệu đầu vào.

**Request Body (JSON):**
```json
{
  "MedInc": 3.5,
  "HouseAge": 30.0,
  "AveRooms": 5.5,
  "AveBedrms": 1.0,
  "Population": 2000,
  "AveOccup": 3.0,
  "Latitude": 34.05,
  "Longitude": -118.24
}
```

**Response (Success):**
```json
{
  "success": true,
  "predicted_price": 2.3456,
  "predicted_price_usd": 234560.0,
  "formatted_price": "$234,560.00"
}
```

**Response (Error):**
```json
{
  "success": false,
  "error": "Error message"
}
```

### GET /health
Kiểm tra trạng thái của service.

**Response:**
```json
{
  "status": "healthy",
  "model_loaded": true
}
```

## Cấu Trúc Files

- `ui.py`: Flask application chính
- `predict_service.py`: Service để load model và thực hiện dự đoán
- `templates/index.html`: Giao diện web UI
- `models/house_price_model/`: Thư mục chứa mô hình đã huấn luyện

## Lưu Ý

- Đảm bảo Spark đã được cài đặt và có thể truy cập được
- Mô hình cần được huấn luyện trước khi sử dụng web UI
- Giá dự đoán được tính bằng đơn vị trăm nghìn USD trong model, nhưng UI hiển thị bằng USD




