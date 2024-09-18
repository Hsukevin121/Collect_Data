from pysnmp.hlapi import SnmpEngine, CommunityData, UdpTransportTarget, ContextData, ObjectType, ObjectIdentity, nextCmd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions  # 正确导入 WriteOptions
import datetime
import time

# 參數設置
INFLUXDB_URL = "http://192.168.0.39:30001"
BUCKET = "socket_info"
ORG = "influxdata"
TOKEN = "pDDWqgH1csy4LYVTPKmsoXfAalFgd4pi"

# 目標 SNMP 伺服器
SNMP_HOST = "192.168.1.10"
COMMUNITY = "public"

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
        point = Point("PDU01001")  # 固定測量點為 PDU01001
        point.field("DeviceId", 5001)  # 添加 DeviceId 字段
        
        for measurement, oid in OIDS.items():
            varBinds = snmp_walk(oid)
            for varBind in varBinds:
                # 获取 OID 的索引
                oid_index = str(varBind[0]).split('.')[-1]
                value = float(varBind[1])
                
                # 将每个 measurement 的值作为字段添加到 point 中
                point.field(f"{measurement}_index_{oid_index}", value)

        # 添加时间戳
        timestamp = datetime.datetime.utcnow().isoformat()  # 生成唯一的时间戳
        point.time(timestamp, WritePrecision.NS)

        # 写入 InfluxDB
        write_api.write(bucket=BUCKET, org=ORG, record=point)
        print(f"Written to InfluxDB: PDU01001, with fields: {point._fields}")

        write_api.flush()  # 确保所有待处理的写入操作完成
        time.sleep(60)  # 每隔60秒循环一次

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        write_api.close()  # 确保在关闭客户端之前完成所有写入操作
        client.close()
        print("Script terminated by user.")
