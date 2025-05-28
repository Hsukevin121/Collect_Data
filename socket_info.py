import time
import datetime
import pymysql
import requests
from pysnmp.hlapi import SnmpEngine, CommunityData, UdpTransportTarget, ContextData, ObjectType, ObjectIdentity, nextCmd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions

# InfluxDB 設定
INFLUXDB_URL = "http://192.168.0.39:30001"
BUCKET = "socket_info"
ORG = "influxdata"
TOKEN = "bzgBwlyDG8ZWBo0LP2hpbJ48I9zhZtMR"

# 初始化 InfluxDB 客戶端
client = InfluxDBClient(url=INFLUXDB_URL, token=TOKEN, org=ORG)
write_api = client.write_api(write_options=WriteOptions(batch_size=1))

# OID 和欄位名稱對應表
OIDS = {
    "outVoltage": ".1.3.6.1.4.1.26104.3.3.3.1.6",
    "outCurrent": ".1.3.6.1.4.1.26104.3.3.3.1.7",
    "outPowerLoad": ".1.3.6.1.4.1.26104.3.3.3.1.9",
    "inFeedPowerEnergy": ".1.3.6.1.4.1.26104.3.3.2.1.8",
    "inFeedCurrent": ".1.3.6.1.4.1.26104.3.3.2.1.5",
    "inFeedVoltage": ".1.3.6.1.4.1.26104.3.3.2.1.3",
    "inFeedPowerLoad": ".1.3.6.1.4.1.26104.3.3.2.1.6"
}

# 從 MySQL 中抓取所有 PDU 裝置資料
def fetch_pdu_devices_from_db():
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
                SELECT IP, Port, devicename, deviceid
                FROM PDUdevicelist
                WHERE IP IS NOT NULL AND Port IS NOT NULL
            """)
            rows = cursor.fetchall()
            for row in rows:
                devices.append({
                    "ip": row[0],
                    "port": row[1],
                    "devicename": row[2],
                    "deviceid": row[3]
                })
    finally:
        connection.close()

    return devices

# SNMP Walk 函式
def snmp_walk(ip, oid):
    results = []
    for (errorIndication, errorStatus, errorIndex, varBinds) in nextCmd(
        SnmpEngine(),
        CommunityData("public"),
        UdpTransportTarget((ip, 161)),
        ContextData(),
        ObjectType(ObjectIdentity(oid)),
        lexicographicMode=False
    ):
        if errorIndication:
            print(f"[SNMP Error] {ip}: {errorIndication}")
            break
        elif errorStatus:
            print('%s at %s' % (errorStatus.prettyPrint(),
                                errorIndex and varBinds[int(errorIndex) - 1][0] or '?'))
            break
        else:
            for varBind in varBinds:
                results.append(varBind)
    return results

# 主程式邏輯
def main():
    while True:
        devices = fetch_pdu_devices_from_db()
        for device in devices:
            point = Point(device["devicename"])
            point.field("DeviceId", device["deviceid"])

            for measurement, oid in OIDS.items():
                varBinds = snmp_walk(device["ip"], oid)
                for varBind in varBinds:
                    oid_index = str(varBind[0]).split('.')[-1]
                    try:
                        value = float(varBind[1])
                        point.field(f"{measurement}_index_{oid_index}", value)
                    except ValueError:
                        print(f"[Warning] Cannot convert value to float: {varBind[1]}")

            timestamp = datetime.datetime.utcnow().isoformat()
            point.time(timestamp, WritePrecision.NS)
            write_api.write(bucket=BUCKET, org=ORG, record=point)
            print(f"Written to InfluxDB: {device['devicename']}, with fields: {point._fields}")

        write_api.flush()
        time.sleep(60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        write_api.close()
        client.close()
        print("Script terminated by user.")
