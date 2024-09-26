import json
import requests
import paho.mqtt.client as mqtt
from datetime import datetime
import random
import string


# 获取当前时间的时间戳（精确到微秒）
timestamp = datetime.utcnow().timestamp()
time_in_s = int(timestamp)
time_ms = int((timestamp - time_in_s) * 1000000)
event_time = datetime.utcfromtimestamp(time_in_s).strftime('%Y-%m-%dT%H:%M:%S') + f'.{time_ms:06d}Z'


# 取整到最近的 900 秒的倍数
collection_end_time = int(timestamp - (timestamp % 900))
collection_start_time = collection_end_time - 900
collection_start_time_micros = collection_start_time * 1000000
collection_end_time_micros = collection_end_time * 1000000
interval_start_time = datetime.utcfromtimestamp(collection_start_time).strftime('%a, %d %b %Y %H:%M:%S GMT')
interval_end_time = datetime.utcfromtimestamp(collection_end_time).strftime('%a, %d %b %Y %H:%M:%S GMT')

# 设置 granularity
granularity = "PM1min"

def generate_event_id(prefix='performance_', length=10):
    random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return prefix + random_string

event_id = generate_event_id()


def generate_json(mqtt_data):
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
                "sourceId": mqtt_data["src_id"],
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
                    "ran_id": mqtt_data["ran_id"]
                },
                "additionalMeasurements": [
                    {
                        "name": "pm_data",
                        "hashMap": mqtt_data["pm_data"]
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
        url = 'http://192.168.0.39:30417/eventListener/v7'
        username1 = 'sample1'
        password1 = 'sample1'
        headers = {'content-type': 'application/json'}
        verify = False

        response = requests.post(url, json=payload, auth=(username1, password1), headers=headers, verify=verify)

        if response.status_code >= 200 and response.status_code <= 300:
            print(payload)
            print('Data sent successfully')
        else:
            print('Failed to send data:', response.text)
    except Exception as e:
        print('Error occurred while sending data:', e)


def filter_du_parameters(mqtt_data):
    filtered_data = {}
    keys_to_keep = [
        "RRU.PrbTotDl", "RRU.PrbAvailDl", "RRU.PrbTotUl", "RRU.PrbAvailUl",
        "RRU.PrbTotDlDist.BinBelow50Percentage", "RRU.PrbTotDlDist.Bin50To60Percentage",
        "RRU.PrbTotDlDist.Bin61To70Percentage", "RRU.PrbTotDlDist.Bin71To80Percentage",
        "RRU.PrbTotDlDist.Bin81To85Percentage", "RRU.PrbTotDlDist.Bin86To90Percentage",
        "RRU.PrbTotDlDist.Bin91To93Percentage", "RRU.PrbTotDlDist.Bin94To96Percentage",
        "RRU.PrbTotDlDist.Bin97To98Percentage", "RRU.PrbTotDlDist.BinAbove98Percentage",
        "RRU.PrbTotUlDist.BinBelow50Percentage", "RRU.PrbTotUlDist.Bin50To60Percentage",
        "RRU.PrbTotUlDist.Bin61To70Percentage", "RRU.PrbTotUlDist.Bin71To80Percentage",
        "RRU.PrbTotUlDist.Bin81To85Percentage", "RRU.PrbTotUlDist.Bin86To90Percentage",
        "RRU.PrbTotUlDist.Bin91To93Percentage", "RRU.PrbTotUlDist.Bin94To96Percentage",
        "RRU.PrbTotUlDist.Bin97To98Percentage", "RRU.PrbTotUlDist.BinAbove98Percentage",
        "L1M.PHR1.BinLessThanMinus32dBm", "L1M.PHR1.BinMinus32ToMinus26dBm",
        "L1M.PHR1.BinMinus25ToMinus19dBm", "L1M.PHR1.BinMinus18ToMinus12dBm",
        "L1M.PHR1.BinMinus11ToMinus5dBm", "L1M.PHR1.BinMinus4To2dBm",
        "L1M.PHR1.Bin3To9dBm", "L1M.PHR1.Bin10To16dBm", "L1M.PHR1.Bin17To23dBm",
        "L1M.PHR1.Bin24To31dBm", "L1M.PHR1.Bin32To37dBm", "L1M.PHR1.BinGreaterThan38",
        "RACH.PreambleDedCell", "RACH.PreambleACell", "RACH.PreambleBCell",
        "RACH.PreambleDed.0", "RACH.PreambleA.0", "RACH.PreambleB.0"
    ]

    for key in keys_to_keep:
        if key in mqtt_data["pm_data"]:
            filtered_data[key] = mqtt_data["pm_data"][key]
    
    return filtered_data

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")
        print("Received message:", payload_str)
        mqtt_data = json.loads(payload_str)

        if mqtt_data["src_id"] == "gNB_DU":
            mqtt_data["pm_data"] = filter_du_parameters(mqtt_data)
            json_payload_du = generate_json(mqtt_data)
            send_http_request(json_payload_du)
        elif mqtt_data["src_id"] == "gNB_CU":
            json_payload_cu = generate_json(mqtt_data)
            send_http_request(json_payload_cu)

    except Exception as e:
        print('Error occurred while processing data:', e)


client = mqtt.Client(protocol=mqtt.MQTTv311)
client.on_message = on_message

broker_address = '192.168.135.102'
broker_port = 1883
client.connect(broker_address, broker_port)

client.subscribe('netconf-proxy/oran-o1/pm')

client.loop_forever()
