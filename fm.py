import json
import requests
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import random
import string
import pymysql
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions

# InfluxDB 設定
INFLUXDB_URL = "http://192.168.0.39:30001"
INFLUXDB_TOKEN = "bzgBwlyDG8ZWBo0LP2hpbJ48I9zhZtMR"
INFLUXDB_ORG = "influxdata"
INFLUXDB_BUCKET = "o1_fault_event"

influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=WriteOptions(batch_size=1))

# 故障ID與裝置類型對應
fault_to_device_mapping = {
    5: ('DU', 3), 6: ('DU', 3), 7: ('DU', 3), 8: ('DU', 3),
    12: ('RU', 3), 13: ('RU', 3), 14: ('RU', 3), 16: ('CU', 3),
    17: ('DU', 3), 18: ('RU', 3), 40: ('CU', 3), 41: ('CU', 3),
    42: ('CU', 3), 55: ('DU', 3), 56: ('DU', 3), 185: ('RU', 3),
    186: ('RU', 3), 187: ('RU', 3), 188: ('RU', 3), 189: ('RU', 3),
    190: ('RU', 3), 191: ('RU', 3), 192: ('RU', 3), 193: ('RU_DU', 3),
    194: ('RU_DU', 3), 195: ('RU_DU', 3), 196: ('DU_CU', 3), 197: ('DU', 3),
    198: ('DU', 3), 199: ('CU', 3), 183: ('DU', 3), 184: ('DU', 3)  # 新增未知告警ID分類
}

# 產生事件 ID
def generate_event_id(prefix='fault_', length=10):
    return prefix + ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

# 從資料庫查詢裝置資訊

def get_device_info(device_type, fault_id, ran_id=None, greigns_id=None):
    table_map = {
        'RU': 'RUdevicelist',
        'DU': 'DUdevicelist',
        'CU': 'CUdevicelist'
    }
    table = table_map.get(device_type)
    if not table:
        return None

    query = ""
    param = ()

    if ran_id:
        query = f"SELECT devicename, deviceid FROM {table} WHERE ran_id = %s"
        param = (ran_id,)
    else:
        query = f"SELECT devicename, deviceid FROM {table} LIMIT 1"

    conn = pymysql.connect(
        host='192.168.0.39',
        user='root',
        password='ubuntu',
        database='devicelist'
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, param)
            result = cursor.fetchone()
            if result:
                return {"devicename": result[0], "deviceid": result[1]}
    finally:
        conn.close()
    return None

# 建立告警資料格式

def generate_alarm_data(fault_id, mqtt_data, is_cleared):
    return {
        "AlarmId": fault_id,
        "EventTime": (datetime.now() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S"),
        "EventSeverity": mqtt_data['notification']['alarm-notif']["fault-severity"],
        "SystemDN": "greigns",
        "ProbableCause": mqtt_data['notification']['alarm-notif']["fault-text"],
        "IsCleared": "Idle" if is_cleared == "false" else "Active"
    }

# 寫入 InfluxDB

def write_fault_to_influx(devicename, deviceid, alarm_data):
    point = Point(devicename)
    point.field("DeviceId", deviceid)
    point.field("AlarmId", alarm_data["AlarmId"])
    point.field("EventSeverity", alarm_data["EventSeverity"])
    point.field("ProbableCause", alarm_data["ProbableCause"])
    point.field("IsCleared", alarm_data["IsCleared"])
    point.time(datetime.utcnow(), WritePrecision.NS)

    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    print(f"[InfluxDB] Wrote fault event for {devicename}")

# 告警上報到上層 API（如需）
def post_fault_data(devicename, deviceid, device_type, alarm_data):
    url = "http://192.168.0.40/api/v1/ORAN/O1Fault?serverid=10001"
    headers = {'Content-Type': 'application/json'}

    payload = {
        "DeviceId": deviceid,
        "DeviceType": devicename,
        "AlarmId": alarm_data["AlarmId"],
        "EventTime": alarm_data["EventTime"],
        "EventSeverity": alarm_data["EventSeverity"],
        "SystemDN": alarm_data["SystemDN"],
        "AlarmType": "oam_Alarms",
        "ProbableCause": alarm_data["ProbableCause"],
        "IsCleared": alarm_data["IsCleared"]
    }

    print("POST Payload:", payload)
    try:
        response = requests.post(url, headers=headers, json=payload)
        print("Response:", response.status_code, response.text)
    except Exception as e:
        print("POST error:", e)

# 更新裝置狀態

def update_device_status(fault_id, device_type, is_cleared, devicename):
    db_connection = pymysql.connect(
        host='192.168.0.39',
        user='root',
        password='ubuntu',
        database='devicelist'
    )
    try:
        with db_connection.cursor() as cursor:
            message = f"Fault detected, Fault ID: {fault_id}" if is_cleared == "false" else "The device is healthy"
            new_status = 3 if is_cleared == "false" else 1
            table_map = {
                'RU': 'RUdevicelist',
                'DU': 'DUdevicelist',
                'CU': 'CUdevicelist'
            }
            table = table_map.get(device_type)
            if table:
                cursor.execute(f"SELECT status FROM {table} WHERE devicename = %s", (devicename,))
                current = cursor.fetchone()
                if current and current[0] != 2:
                    cursor.execute(
                        f"UPDATE {table} SET status = %s, message = %s WHERE devicename = %s",
                        (new_status, message, devicename)
                    )
        db_connection.commit()
    except Exception as e:
        print("Database update error:", e)
    finally:
        db_connection.close()

# 告警處理流程

def handle_fault(fault_id, is_cleared, mqtt_data):
    if fault_id not in fault_to_device_mapping:
        print(f"Unknown fault ID: {fault_id} — Adding default classification as RU")
        device_type = 'RU'
    else:
        device_type, _ = fault_to_device_mapping[fault_id]

    alarm_data = generate_alarm_data(fault_id, mqtt_data, is_cleared)
    ran_id = mqtt_data['notification']['alarm-notif'].get('ran-id')

    types = device_type.split('_') if '_' in device_type else [device_type]

    for dtype in types:
        device = get_device_info(dtype, fault_id, ran_id=ran_id)
        if device:
            post_fault_data(device["devicename"], device["deviceid"], dtype, alarm_data)
            update_device_status(fault_id, dtype, is_cleared, device["devicename"])
            write_fault_to_influx(device["devicename"], device["deviceid"], alarm_data)
        else:
            print(f"No matching device found for type {dtype}, ran_id={ran_id}")

# MQTT 消息處理

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")
        print("Received MQTT:", payload_str)

        mqtt_data = json.loads(payload_str)
        fault_id = int(mqtt_data['notification']['alarm-notif']["fault-id"])
        is_cleared = mqtt_data['notification']['alarm-notif']['is-cleared']

        handle_fault(fault_id, is_cleared, mqtt_data)

    except Exception as e:
        print('MQTT processing error:', e)

# 設定 MQTT
client = mqtt.Client(protocol=mqtt.MQTTv311)
client.on_message = on_message
client.connect('192.168.135.102', 1883)
client.subscribe('netconf-proxy/oran-o1/fm')
client.loop_forever()
