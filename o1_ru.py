import requests
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions
import datetime

# InfluxDB 配置
INFLUXDB_URL = "http://192.168.0.39:30001"  # 替换为你的 InfluxDB URL
BUCKET = "o1_performance"
ORG = "influxdata"  # 替换为你的组织名称
TOKEN = "bA4f9O4UZcg2IgCLndENMmhuAT110mdU"  # 替换为你的 InfluxDB 访问令牌

# 初始化 InfluxDB 客户端
client = InfluxDBClient(url=INFLUXDB_URL, token=TOKEN, org=ORG)
write_api = client.write_api(write_options=WriteOptions(batch_size=1))

# API 地址
api_url = 'https://192.168.135.102:16000/api/mplane-proxy/oran-mp/ru/info/software'

def fetch_ru_info():
    """调用 API 获取 RU 信息"""
    try:
        response = requests.get(api_url, verify=False)
        if response.status_code == 200:
            data = response.json()
            # 获取 tx_attenuation 的值
            tx_attenuation = float(data['msg']['ru1']['tx_attenuation'])
            return tx_attenuation
        else:
            print(f"Failed to fetch data from API: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error while fetching RU info: {e}")
        return None

def write_to_influxdb(tx_attenuation):
    """将数据写入 InfluxDB"""
    timestamp = datetime.datetime.utcnow().isoformat()
    # 添加 DeviceId 字段，值为 2001
    point = Point("RU01001").field("tx_attenuation", tx_attenuation).field("DeviceId", 2001).time(timestamp, WritePrecision.NS)
    write_api.write(bucket=BUCKET, org=ORG, record=point)
    print(f"Successfully wrote tx_attenuation: {tx_attenuation} and DeviceId: 2001 to InfluxDB")

def main():
    while True:
        tx_attenuation = fetch_ru_info()
        if tx_attenuation is not None:
            write_to_influxdb(tx_attenuation)
        time.sleep(60)  # 每分钟调用一次 API

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        write_api.close()  # 确保在关闭客户端之前完成所有写入操作
        client.close()
        print("Script terminated by user.")
