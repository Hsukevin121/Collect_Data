import json
import requests
import paho.mqtt.client as mqtt
from datetime import datetime
import threading
import time
import random
import string

# 全局变量
latest_mqtt_data = None
buffer_lock = threading.Lock()

# 获取时间和事件ID的生成逻辑
def generate_event_id(prefix='other_', length=10):
    random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return prefix + random_string

event_id = generate_event_id()
timestamp = datetime.utcnow().timestamp()
time_in_s = int(timestamp)
time_ms = int((timestamp - time_in_s) * 1000000)
event_time = datetime.utcfromtimestamp(time_in_s).strftime('%Y-%m-%dT%H:%M:%S') + f'.{time_ms:06d}Z'

collection_end_time = int(timestamp - (timestamp % 900))
collection_start_time = collection_end_time - 900
collection_start_time_micros = collection_start_time * 1000000
collection_end_time_micros = collection_end_time * 1000000
interval_start_time = datetime.utcfromtimestamp(collection_start_time).strftime('%a, %d %b %Y %H:%M:%S GMT')
interval_end_time = datetime.utcfromtimestamp(collection_end_time).strftime('%a, %d %b %Y %H:%M:%S GMT')

# 生成 JSON1 数据
def generate_json1(mqtt_data):
    template = {
        "event": {
            "commonEventHeader": {
                "domain": "other",
                "eventId": event_id,
                "eventName": "ue_info",
                "eventType": "ue_pm",
                "sequence": 0,
                "priority": "Low",
                "reportingEntityId": "",
                "reportingEntityName": "node1.cluster.local",
                "sourceId": "",
                "sourceName": "BBU_greigns",
                "startEpochMicrosec": collection_start_time_micros,
                "lastEpochMicrosec": collection_end_time_micros,
                "internalHeaderFields": {
                    "intervalStartTime": interval_start_time,
                    "intervalEndTime": interval_end_time
                },
                "version": "4.1",
                "vesEventListenerVersion": "7.2.1"
            },
            "otherFields": {
                "otherFieldsVersion": "3.0",
                "arrayOfNamedHashMap": [
                    {
                        "name": "total",
                        "hashMap": {
                            "num_of_ue": str(mqtt_data['num_of_ue']),
                            "total_ul_tp": str(mqtt_data['total_ul_tp']),
                            "total_ul_pkt": str(mqtt_data['total_ul_pkt']),
                            "total_dl_tp": str(mqtt_data['total_dl_tp']),
                            "total_dl_pkt": str(mqtt_data['total_dl_pkt'])
                        }
                    }
                ]
            }
        }
    }
    return template

# 生成 JSON2 数据
def generate_json2(mqtt_data):
    ue_array = []
    for i in range(mqtt_data['num_of_ue']):
        ue_info = mqtt_data['ue'][i]
        ue_payload = {
            "name": "personal",
            "hashMap": {
                "ue_id": str(ue_info['ue_id']),
                "ul_tp": str(ue_info['ul_tp']),
                "ul_pkt": str(ue_info['ul_pkt']),
                "dl_tp": str(ue_info['dl_tp']),
                "dl_pkt": str(ue_info['dl_pkt'])
            }
        }
        ue_array.append(ue_payload)

    template = {
        "event": {
            "commonEventHeader": {
                "domain": "other",
                "eventId": event_id,
                "eventName": "ue_info",
                "eventType": "ue_pm",
                "sequence": 0,
                "priority": "Low",
                "reportingEntityId": "",
                "reportingEntityName": "node1.cluster.local",
                "sourceId": "",
                "sourceName": "BBU_greigns",
                "startEpochMicrosec": collection_start_time_micros,
                "lastEpochMicrosec": collection_end_time_micros,
                "internalHeaderFields": {
                    "intervalStartTime": interval_start_time,
                    "intervalEndTime": interval_end_time
                },
                "version": "4.1",
                "vesEventListenerVersion": "7.2.1"
            },
            "otherFields": {
                "otherFieldsVersion": "3.0",
                "arrayOfNamedHashMap": ue_array
            }
        }
    }
    return template

# 发送 HTTP 请求
def send_http_request(payload):
    try:
        url = 'http://192.168.0.39:30417/eventListener/v7'
        username1 = 'sample1'
        password1 = 'sample1'
        headers = {'content-type': 'application/json'}
        response = requests.post(url, json=payload, auth=(username1, password1), headers=headers)
        if response.status_code >= 200 and response.status_code <= 300:
            print('Data sent successfully')
        else:
            print('Failed to send data:', response.text)
    except Exception as e:
        print('Error occurred while sending data:', e)

# 定时发送最新的 MQTT 数据
def process_and_send_data():
    while True:
        time.sleep(60)  # 每 60 秒处理一次

        with buffer_lock:
            if latest_mqtt_data:
                json_payload1 = generate_json1(latest_mqtt_data)
                json_payload2 = generate_json2(latest_mqtt_data)
                send_http_request(json_payload1)
                send_http_request(json_payload2)
                print("Sent data for the latest MQTT message")

# MQTT 消息回调函数
def on_message(client, userdata, msg):
    global latest_mqtt_data
    try:
        payload_str = msg.payload.decode("utf-8")
        mqtt_data = json.loads(payload_str)

        # 锁定并更新最新的 MQTT 数据
        with buffer_lock:
            latest_mqtt_data = mqtt_data

        print("New MQTT data received and stored.")
    except Exception as e:
        print('Error occurred while receiving data:', e)

# 设置 MQTT 客户端
client = mqtt.Client(protocol=mqtt.MQTTv5)
client.on_message = on_message

# 连接到 MQTT 代理
broker_address = '192.168.135.102'
broker_port = 1883
client.connect(broker_address, broker_port)

# 订阅主题
client.subscribe('ran1/pdcp_tp')

# 启动定时处理线程
threading.Thread(target=process_and_send_data, daemon=True).start()

# 开始循环
client.loop_forever()
