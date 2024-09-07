from pysnmp.hlapi import SnmpEngine, CommunityData, UdpTransportTarget, ContextData, ObjectType, ObjectIdentity, nextCmd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions
import datetime
import time

# 參數設置
INFLUXDB_URL = "http://192.168.0.39:30001"
BUCKET = "socket_info"
ORG = "influxdata"
TOKEN = "l2yrVMPtDQW6Zl9KEVRI2o3LqloJcZue"

# 目標 SNMP 伺服器
SNMP_HOST = "192.168.1.10"
COMMUNITY = "public"

# OID 和欄位名稱對應表
OIDS = {
    "outVoltage": ".1.3.6.1.4.1.26104.3.3.3.1.6",
    "outCurrent": ".1.3.6.1.4.1.26104.3.3.3.1.7",
    "outPowerLoad": ".1.3.6.1.4.1.26104.3.3.3.1.9",
    "inFeedPowerEnergy": ".1.3.6.1.4.1.26104.3.3.2.1.8"
}

# 初始化InfluxDB客戶端
client = InfluxDBClient(url=INFLUXDB_URL, token=TOKEN, org=ORG)
write_api = client.write_api(write_options=WriteOptions(batch_size=1))

def snmp_walk(oid):
    results = []
    for (errorIndication, errorStatus, errorIndex, varBinds) in nextCmd(
        SnmpEngine(),
        CommunityData(COMMUNITY),
        UdpTransportTarget((SNMP_HOST, 161)),
        ContextData(),
        ObjectType(ObjectIdentity(oid)),
        lexicographicMode=False
    ):
        if errorIndication:
            print(errorIndication)
            break
        elif errorStatus:
            print('%s at %s' % (errorStatus.prettyPrint(),
                                errorIndex and varBinds[int(errorIndex) - 1][0] or '?'))
            break
        else:
            for varBind in varBinds:
                results.append(varBind)
    return results

def main():
    while True:
        for measurement, oid in OIDS.items():
            varBinds = snmp_walk(oid)
            for varBind in varBinds:
                oid_index = str(varBind[0]).split('.')[-1]
                value = float(varBind[1])
                timestamp = datetime.datetime.utcnow().isoformat()  # 生成唯一的时间戳
                point = Point(measurement).tag("index", oid_index).field("value", value).time(timestamp, WritePrecision.NS)
                write_api.write(bucket=BUCKET, org=ORG, record=point)
        write_api.flush()  # 确保所有待处理的写入操作完成
        time.sleep(60)  # 等待3秒

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        write_api.close()  # 确保在关闭客户端之前完成所有写入操作
        client.close()
        print("Script terminated by user.")
