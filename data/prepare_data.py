"""
Script để tải và chia dữ liệu
Sử dụng California Housing dataset (có sẵn trong sklearn)
"""
import pandas as pd
from sklearn.datasets import fetch_california_housing
from sklearn.model_selection import train_test_split
import os

def prepare_data():
    # Tạo thư mục data nếu chưa có
    os.makedirs('data', exist_ok=True)
    
    # Tải dataset California Housing
    housing = fetch_california_housing()
    df = pd.DataFrame(housing.data, columns=housing.feature_names)
    df['target'] = housing.target  # Giá nhà (đơn vị: $100,000)
    
    print(f"Tổng số mẫu: {len(df)}")
    print(f"Các đặc trưng: {housing.feature_names}")
    print(f"\nMô tả dữ liệu:")
    print(df.describe())
    
    # Chia 80% cho huấn luyện, 20% cho streaming
    train_df, streaming_df = train_test_split(df, test_size=0.2, random_state=42)
    
    # Lưu dữ liệu
    train_df.to_csv('data/train_data.csv', index=False)
    streaming_df.to_csv('data/streaming_data.csv', index=False)
    
    print(f"\n✓ Đã lưu {len(train_df)} mẫu huấn luyện vào data/train_data.csv")
    print(f"✓ Đã lưu {len(streaming_df)} mẫu streaming vào data/streaming_data.csv")
    
    # Tạo file README
    with open('data/README.md', 'w', encoding='utf-8') as f:
        f.write("# Dữ liệu California Housing\n\n")
        f.write("## Các đặc trưng:\n")
        for feat in housing.feature_names:
            f.write(f"- {feat}\n")
        f.write("\n## Target:\n")
        f.write("- target: Giá nhà trung bình (đơn vị: $100,000)\n")
        f.write(f"\n## Thống kê:\n")
        f.write(f"- Tổng số mẫu: {len(df)}\n")
        f.write(f"- Mẫu huấn luyện: {len(train_df)}\n")
        f.write(f"- Mẫu streaming: {len(streaming_df)}\n")

if __name__ == "__main__":
    prepare_data()