import json
import requests
import paho.mqtt.client as mqtt
from datetime import datetime
import random
import string

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

# 设置 granularity
granularity = "PM1min"

def generate_event_id(prefix='PM_', length=10):
    random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return prefix + random_string

event_id = generate_event_id()

def generate_json(mqtt_data):
    event_id = generate_event_id()
    template = {
        "event": {
            "commonEventHeader": {
                "domain": "measurement",
                "eventId": event_id,
                "eventName": "oran_pm",
                "eventType": "PM",
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
            "measurementFields": {
                "additionalFields": {
                    "ran_id": "ran1",
                    "src_id": "BBU"
                },
                "additionalMeasurements": [
                    {
                        "name": "BBU_instantaneous_power",
                        "hashMap": {
                            "instantaneous_power": mqtt_data["instantaneous_power"]
                        }
                    }
                ],
                "measurementInterval": 15,
                "measurementFieldsVersion": "4.0"
            }
        }
    }

    return template

def send_http_request(payload):
    try:
        # 设置目标地址
        url = 'http://192.168.0.19:30417/eventListener/v7'

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

        # 将 JSON 字符串解析为 Python 对象
        mqtt_data = json.loads(payload_str)

        # 生成要发送的 JSON 数据
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

print(f"Connecting to MQTT broker at {broker_address}:{broker_port}")
try:
    client.connect(broker_address, broker_port)
    print("Connected successfully")
except Exception as e:
    print(f"Failed to connect to MQTT broker: {e}")

# 订阅主题
client.subscribe('bbu/instantaneous-power')

# 开始循环
client.loop_forever()
