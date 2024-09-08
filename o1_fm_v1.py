import json
import requests
import paho.mqtt.client as mqtt
from datetime import datetime
import random
import string
import pymysql

# 获取当前时间的时间戳（精确到微秒）
timestamp = datetime.utcnow().timestamp()
# 将时间戳转换为秒
time_in_s = int(timestamp)
# 获取毫秒部分
time_ms = int((timestamp - time_in_s) * 1000000)
# 格式化时间
event_time = datetime.utcfromtimestamp(time_in_s).strftime('%Y-%m-%dT%H:%M:%S') + f'.{time_ms:06d}Z'

# 取整到最近的 900 秒的倍数
collection_end_time = int(timestamp - (timestamp % 900))
# 计算 collectionEndTime 前 900 秒的时间戳
collection_start_time = collection_end_time - 900
# 将 collectionStartTime 和 collectionEndTime 转换成微秒级别
collection_start_time_micros = collection_start_time * 1000000
collection_end_time_micros = collection_end_time * 1000000
# 将 collectionStartTime 和 collectionEndTime 格式化成可读的时间字符串
interval_start_time = datetime.utcfromtimestamp(collection_start_time).strftime('%a, %d %b %Y %H:%M:%S GMT')
interval_end_time = datetime.utcfromtimestamp(collection_end_time).strftime('%a, %d %b %Y %H:%M:%S GMT')


def generate_event_id(prefix='fault_', length=10):
    random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return prefix + random_string

event_id = generate_event_id()

# Fault ID to Device Type and Status Mapping
fault_to_device_mapping = {
    5: ('RU', 3),
    6: ('RU', 3),
    7: ('RU', 3),
    8: ('RU', 3),
    12: ('RU', 3),
    13: ('RU', 3),
    14: ('RU', 3),
    16: ('CU', 3),
    17: ('DU', 3),
    18: ('RU', 3),
    41: ('CU', 3),
    44: ('CU', 3),
    49: ('CU', 3),
    52: ('DU', 3),
    55: ('DU', 3),
    60: ('DU', 3),
    186: ('RU', 3),
    193: ('RU_DU', 3),
    194: ('RU_DU', 3),
    195: ('RU_DU', 3),
    196: ('DU_CU', 3),
    197: ('DU', 3),
    198: ('DU', 3),
    199: ('CU', 3),
    # 添加更多映射...
}

# Update MySQL Device Status Based on Fault ID
def update_device_status(fault_id, device_type, new_status, is_cleared):
    db_connection = pymysql.connect(
        host='192.168.0.39',
        user='root',
        password='rtlab666',
        database='devicelist'
    )

    try:
        with db_connection.cursor() as cursor:
            # 如果故障未清除 (is_cleared == "false")，将 message 设置为 fault_id
            if is_cleared == "false":
                message = f"Fault detected, Fault ID: {fault_id}"
                new_status = 3
            else:
                message = "The device is healthy"
                new_status = 1

            if device_type == 'RU_DU':
                # 更新 RU 和 DU 设备的状态、fault_id 和 message
                cursor.execute(
                    "UPDATE devicelist SET status = %s, message = %s WHERE devicename LIKE 'CU%%'", 
                    (new_status, message)
                )
                cursor.execute(
                    "UPDATE devicelist SET status = %s, message = %s WHERE devicename LIKE 'CU%%'", 
                    (new_status, message)
                )
            elif device_type == 'DU_CU':
                # 更新 DU 和 CU 设备的状态、fault_id 和 message
                cursor.execute(
                    "UPDATE devicelist SET status = %s, message = %s WHERE devicename LIKE 'CU%%'", 
                    (new_status, message)
                )
                cursor.execute(
                    "UPDATE devicelist SET status = %s, message = %s WHERE devicename LIKE 'CU%%'", 
                    (new_status, message)
                )
            else:
                # 更新指定设备类型的状态、fault_id 和 message
                cursor.execute(
                    "UPDATE devicelist SET status = %s, message = %s WHERE devicename LIKE %s", 
                    (new_status, message, f'{device_type}%')
                )

        db_connection.commit()
    except Exception as e:
        print("Error occurred while updating the database:", e)
    finally:
        db_connection.close()

# Generate JSON for Sending
def generate_json(mqtt_data):
    event_id = generate_event_id()

    a = mqtt_data['notification']['alarm-notif']['is-cleared']
    if a == "false":
        a = "Idle"
    elif a == "true":
        a = "Active"

    template = {
        "event": {
            "commonEventHeader": {
                "domain": "fault",
                "eventId": event_id,
                "eventName": mqtt_data['notification']['alarm-notif']['fault-text'],
                "eventType": "oam_Alarms",
                "sequence": 0,
                "priority": "High",
                "reportingEntityId": "",
                "reportingEntityName": mqtt_data['notification']['alarm-notif']['ran-id'],
                "sourceId": "",
                "sourceName": mqtt_data['notification']['alarm-notif']['ran-id'],
                "startEpochMicrosec": collection_start_time_micros,
                "lastEpochMicrosec": collection_end_time_micros,
                "nfNamingCode": "oam",
                "nfVendorName": "gregins",
                "timeZoneOffset": "+00:00",
                "version": "4.1",
                "vesEventListenerVersion": "7.2.1"
            },
            "faultFields": {
                "faultFieldsVersion": "4.0",
                "eventSeverity": mqtt_data['notification']['alarm-notif']["fault-severity"],
                "eventSourceType": "fault",
                "eventCategory": "network",
                "alarmCondition": mqtt_data['notification']['alarm-notif']["fault-id"],
                "specificProblem": mqtt_data['notification']['alarm-notif']["fault-text"],
                "vfStatus": a,
                "alarmInterfaceA": "N/A"
            }
        }
    }

    print("Generated JSON template:", template)
    return template

# Send HTTP POST Request
def send_http_request(payload):
    try:
        # 设置目标地址
        url = 'http://192.168.0.39:30417/eventListener/v7'

        # 设置用户名和密码
        username1 = 'sample1'
        password1 = 'sample1'

        headers = {'content-type': 'application/json'}
        verify = False

        # 发送 HTTP POST 请求
        response = requests.post(url, json=payload, auth=(username1, password1), headers=headers, verify=verify)

        # 检查响应状态码
        if response.status_code >= 200 and response.status_code <= 300:
            print(payload)
            print('Data sent successfully')
        else:
            print('Failed to send data:', response.text)
    except Exception as e:
        print('Error occurred while sending data:', e)

# MQTT 消息回调函数
def on_message(client, userdata, msg):
    try:
        # 转换消息为 UTF-8 编码的字符串，并打印
        payload_str = msg.payload.decode("utf-8")
        print("Received message:", payload_str)

        # 將 JSON 字串解析為 Python 物件
        mqtt_data = json.loads(payload_str)
        fault_id = int(mqtt_data['notification']['alarm-notif']["fault-id"])
        is_cleared = mqtt_data['notification']['alarm-notif']['is-cleared']

        # Update device status based on fault_id and is-cleared
        if fault_id in fault_to_device_mapping:
            device_type, status_to_update = fault_to_device_mapping[fault_id]
            
            # If is_cleared is "false", set status to 3 (Idle state)
            if is_cleared == "false":
                update_device_status(fault_id, device_type, 3, is_cleared)
            # If is_cleared is "true", set status to 1 (Active state) and message to "The device is healthy"
            elif is_cleared == "true":
                update_device_status(fault_id, device_type, 1, is_cleared)

        # 生成要發送的 JSON 資料
        json_payload = generate_json(mqtt_data)
        # 调用发送 HTTP POST 请求的函数
        send_http_request(json_payload)
    except Exception as e:
        print('Error occurred while processing message:', e)

# 设置 MQTT 客户端
client = mqtt.Client(protocol=mqtt.MQTTv311)
client.on_message = on_message

# 连接到 MQTT 代理
broker_address = '192.168.135.102'
broker_port = 1883
client.connect(broker_address, broker_port)

# 订阅主题
client.subscribe('netconf-proxy/oran-o1/fm')

# 开始循环
client.loop_forever()
