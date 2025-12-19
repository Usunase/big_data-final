"""
Script để upload dữ liệu lên HDFS
"""
import subprocess
import os
import sys

# Cấu hình HDFS
HDFS_NAMENODE = "hdfs://192.168.80.148:9000"  # Thay đổi theo cấu hình của bạn
HDFS_DATA_DIR = "/bigdata/house_prices"
LOCAL_DATA_DIR = "data"

def check_hdfs_available():
    """Kiểm tra HDFS có sẵn sàng không"""
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-ls", "/"],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Không thể kết nối đến HDFS: {e}")
        return False

def upload_to_hdfs(local_path, hdfs_path):
    """Upload file lên HDFS"""
    try:
        # Tạo thư mục trên HDFS nếu chưa có
        subprocess.run(
            ["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(hdfs_path)],
            check=True,
            timeout=30
        )
        
        # Upload file
        print(f"📤 Đang upload {local_path} -> {hdfs_path}")
        result = subprocess.run(
            ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            print(f"✓ Đã upload thành công: {hdfs_path}")
            return True
        else:
            print(f"❌ Lỗi khi upload: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"❌ Timeout khi upload {local_path}")
        return False
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return False

def main():
    print("=" * 60)
    print("UPLOAD DỮ LIỆU LÊN HDFS")
    print("=" * 60)
    
    # Kiểm tra HDFS
    if not check_hdfs_available():
        print("❌ HDFS không sẵn sàng. Vui lòng kiểm tra lại.")
        sys.exit(1)
    
    print("✓ HDFS đã sẵn sàng")
    
    # Kiểm tra file local
    train_file = os.path.join(LOCAL_DATA_DIR, "train_data.csv")
    streaming_file = os.path.join(LOCAL_DATA_DIR, "streaming_data.csv")
    
    if not os.path.exists(train_file):
        print(f"❌ Không tìm thấy file: {train_file}")
        print("💡 Chạy prepare_data.py trước để tạo dữ liệu")
        sys.exit(1)
    
    if not os.path.exists(streaming_file):
        print(f"❌ Không tìm thấy file: {streaming_file}")
        print("💡 Chạy prepare_data.py trước để tạo dữ liệu")
        sys.exit(1)
    
    # Upload train_data.csv
    hdfs_train_path = f"{HDFS_DATA_DIR}/train_data.csv"
    if not upload_to_hdfs(train_file, hdfs_train_path):
        sys.exit(1)
    
    # Upload streaming_data.csv
    hdfs_streaming_path = f"{HDFS_DATA_DIR}/streaming_data.csv"
    if not upload_to_hdfs(streaming_file, hdfs_streaming_path):
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("✓ HOÀN THÀNH UPLOAD DỮ LIỆU")
    print("=" * 60)
    print(f"Train data: {hdfs_train_path}")
    print(f"Streaming data: {hdfs_streaming_path}")

if __name__ == "__main__":
    main()

