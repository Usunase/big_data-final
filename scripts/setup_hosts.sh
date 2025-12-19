#!/bin/bash
# Script để setup /etc/hosts trên các máy Kafka/Spark
# Usage: ./setup_hosts.sh <IP_AIRFLOW> <HOSTNAME_AIRFLOW>

if [ $# -ne 2 ]; then
    echo "Usage: $0 <IP_AIRFLOW> <HOSTNAME_AIRFLOW>"
    echo "Example: $0 192.168.1.7 airflow-master"
    exit 1
fi

IP_AIRFLOW=$1
HOSTNAME_AIRFLOW=$2

echo "=========================================="
echo "Setup /etc/hosts để map hostname máy Airflow"
echo "=========================================="
echo "IP Airflow: $IP_AIRFLOW"
echo "Hostname Airflow: $HOSTNAME_AIRFLOW"
echo ""

# Kiểm tra đã có entry chưa
if grep -q "$HOSTNAME_AIRFLOW" /etc/hosts; then
    echo "⚠️  Entry đã tồn tại trong /etc/hosts:"
    grep "$HOSTNAME_AIRFLOW" /etc/hosts
    read -p "Bạn có muốn cập nhật? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Bỏ qua."
        exit 0
    fi
    # Xóa entry cũ
    sudo sed -i "/$HOSTNAME_AIRFLOW/d" /etc/hosts
fi

# Thêm entry mới
echo "$IP_AIRFLOW  $HOSTNAME_AIRFLOW" | sudo tee -a /etc/hosts

echo ""
echo "✓ Đã thêm vào /etc/hosts:"
grep "$HOSTNAME_AIRFLOW" /etc/hosts

echo ""
echo "Test kết nối:"
ping -c 2 $HOSTNAME_AIRFLOW

echo ""
echo "✓ Hoàn thành!"

