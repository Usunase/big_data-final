"""
Flask Web UI cho dự đoán giá nhà
"""
from flask import Flask, render_template, request, jsonify
from predict_service import get_predictor
import os

app = Flask(__name__)
app.config['SECRET_KEY'] = 'house-price-prediction-secret-key'

# Khởi tạo predictor khi app start
predictor = None

def initialize_predictor():
    """Khởi tạo predictor khi app khởi động"""
    global predictor
    try:
        predictor = get_predictor()
        print("✓ Predictor đã được khởi tạo")
    except Exception as e:
        print(f"⚠️  Không thể khởi tạo predictor: {e}")
        predictor = None

@app.route('/')
def index():
    """Trang chủ - Form nhập liệu"""
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    """API endpoint để dự đoán giá nhà"""
    try:
        if predictor is None:
            return jsonify({
                'success': False,
                'error': 'Model chưa được tải. Vui lòng kiểm tra lại.'
            }), 500
        
        # Lấy dữ liệu từ form
        data = request.get_json() if request.is_json else request.form
        
        med_inc = float(data.get('MedInc', 0))
        house_age = float(data.get('HouseAge', 0))
        ave_rooms = float(data.get('AveRooms', 0))
        ave_bedrms = float(data.get('AveBedrms', 0))
        population = float(data.get('Population', 0))
        ave_occup = float(data.get('AveOccup', 0))
        latitude = float(data.get('Latitude', 0))
        longitude = float(data.get('Longitude', 0))
        
        # Validate input
        if not all([med_inc, house_age, ave_rooms, ave_bedrms, 
                   population, ave_occup, latitude, longitude]):
            return jsonify({
                'success': False,
                'error': 'Vui lòng điền đầy đủ tất cả các trường'
            }), 400
        
        # Thực hiện dự đoán
        predicted_price = predictor.predict(
            med_inc=med_inc,
            house_age=house_age,
            ave_rooms=ave_rooms,
            ave_bedrms=ave_bedrms,
            population=population,
            ave_occup=ave_occup,
            latitude=latitude,
            longitude=longitude
        )
        
        # Chuyển đổi đơn vị: từ trăm nghìn USD sang USD
        price_usd = predicted_price * 100000
        
        return jsonify({
            'success': True,
            'predicted_price': round(predicted_price, 4),
            'predicted_price_usd': round(price_usd, 2),
            'formatted_price': f"${price_usd:,.2f}"
        })
        
    except ValueError as e:
        return jsonify({
            'success': False,
            'error': f'Dữ liệu đầu vào không hợp lệ: {str(e)}'
        }), 400
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Lỗi khi dự đoán: {str(e)}'
        }), 500

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'model_loaded': predictor is not None
    })

if __name__ == '__main__':
    # Khởi tạo predictor trước khi chạy app
    try:
        predictor = get_predictor()
        print("=" * 60)
        print("✓ Web UI đã sẵn sàng!")
        print("=" * 60)
        print("🌐 Truy cập: http://localhost:5000")
        print("=" * 60)
    except Exception as e:
        print(f"⚠️  Cảnh báo: Không thể tải model: {e}")
        print("⚠️  App vẫn chạy nhưng không thể dự đoán")
    
    app.run(host='0.0.0.0', port=5000, debug=True)

