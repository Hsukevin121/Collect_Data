import time
import datetime
import requests
import pymysql
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions

# InfluxDB 2.0 設定
INFLUXDB_URL = "http://192.168.0.39:30001"
BUCKET = "o1_performance"
ORG = "influxdata"
TOKEN = "bzgBwlyDG8ZWBo0LP2hpbJ48I9zhZtMR"

# 初始化 InfluxDB 客戶端
client = InfluxDBClient(url=INFLUXDB_URL, token=TOKEN, org=ORG)
write_api = client.write_api(write_options=WriteOptions(batch_size=1))

# 從 MySQL 中抓取所有 RU 裝置資料
def fetch_ru_devices_from_db():
    connection = pymysql.connect(
        host='192.168.0.39',
        user='root',
        password='ubuntu',
        database='devicelist'
    )
    devices = []

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT IP, Port, Greigns_id, devicename
                FROM RUdevicelist
                WHERE IP IS NOT NULL AND Port IS NOT NULL AND Greigns_id IS NOT NULL
            """)
            rows = cursor.fetchall()
            for row in rows:
                devices.append({
                    "ip": row[0],
                    "port": row[1],
                    "greigns_id": row[2],
                    "devicename": row[3]
                })
    finally:
        connection.close()

    return devices

# 根據裝置資訊發送 API 並取得 tx_attenuation

def fetch_ru_info(ip, port, greigns_id):
    api_url = f'https://{ip}:{port}/api/mplane-proxy/oran-mp/ru/info/software'
    try:
        response = requests.get(api_url, verify=False, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if greigns_id in data["msg"] and "tx_attenuation" in data["msg"][greigns_id]:
                return float(data["msg"][greigns_id]["tx_attenuation"])
            else:
                print(f"tx_attenuation not found for {greigns_id} from {ip}:{port}")
        else:
            print(f"Failed to fetch data from {ip}:{port}, status code: {response.status_code}")
    except Exception as e:
        print(f"Error while fetching RU info from {ip}:{port}: {e}")
    return None

# 寫入 InfluxDB（measurement = devicename，field = tx_attenuation）
def write_to_influxdb(devicename, tx_attenuation):
    timestamp = datetime.datetime.utcnow().isoformat()
    point = Point(str(devicename)) \
        .field("tx_attenuation", tx_attenuation) \
        .time(timestamp, WritePrecision.NS)

    write_api.write(bucket=BUCKET, org=ORG, record=point)
    print(f"[Device {devicename}] Wrote tx_attenuation: {tx_attenuation} to InfluxDB")

# 主邏輯輪詢執行
def main():
    while True:
        devices = fetch_ru_devices_from_db()
        for device in devices:
            tx_att = fetch_ru_info(device["ip"], device["port"], device["greigns_id"])
            if tx_att is not None:
                write_to_influxdb(device["devicename"], tx_att)
        time.sleep(60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        write_api.close()
        client.close()
        print("Script terminated by user.")
