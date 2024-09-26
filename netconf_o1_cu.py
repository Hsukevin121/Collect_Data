import os
import xml.etree.ElementTree as ET
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# 设置InfluxDB连接
bucket = "o1_performance"
org = "influxdata"
token = "bA4f9O4UZcg2IgCLndENMmhuAT110mdU"
url = "http://192.168.0.39:30001"
client = InfluxDBClient(url=url, token=token)
write_api = client.write_api(write_options=SYNCHRONOUS)

# 解析 CU 的 XML 文件并写入 InfluxDB
def process_cu_xml(file_path):
    try:
        tree = ET.parse(file_path)
    except ET.ParseError:
        print(f"Error: Unable to parse the CU XML file: {file_path}")
        return

    root = tree.getroot()

    # 创建一个字典来存储 measType 的 p 值和参数名的对应关系
    meas_types = {}
    for measType in root.findall('.//{http://www.3gpp.org/ftp/specs/archive/28_series/28.532#measData}measType'):
        p_value = measType.attrib.get('p')
        meas_types[p_value] = measType.text

    # 遍历 XML 中的 measValue 标签
    for measValue in root.findall('.//{http://www.3gpp.org/ftp/specs/archive/28_series/28.532#measData}measValue'):
        meas_obj = measValue.attrib.get('measObjLdn')

        # 获取指标数据
        fields = {}
        for r in measValue.findall('.//{http://www.3gpp.org/ftp/specs/archive/28_series/28.532#measData}r'):
            p_value = r.attrib.get('p')
            meas_name = meas_types.get(p_value, f"metric_{p_value}")  # 获取参数名称
            fields[meas_name] = float(r.text)

        # 打印 fields 确认其内容是字典
        print(fields)

        # 逐个插入每个字段
        for meas_name, value in fields.items():
            point = Point("CU01001").tag("measObjLdn", meas_obj).field(meas_name, value)
            write_api.write(bucket=bucket, org=org, record=point)

# 解析 DU 的 XML 文件并写入 InfluxDB
def process_du_xml(file_path):
    try:
        tree = ET.parse(file_path)
    except ET.ParseError:
        print(f"Error: Unable to parse the DU XML file: {file_path}")
        return

    root = tree.getroot()

    # 创建一个字典来存储 measType 的 p 值和参数名的对应关系
    meas_types = {}
    for measType in root.findall('.//{http://www.3gpp.org/ftp/specs/archive/28_series/28.532#measData}measType'):
        p_value = measType.attrib.get('p')
        meas_types[p_value] = measType.text

    # 遍历 XML 中的 measValue 标签
    for measValue in root.findall('.//{http://www.3gpp.org/ftp/specs/archive/28_series/28.532#measData}measValue'):
        meas_obj = measValue.attrib.get('measObjLdn')

        # 获取指标数据
        fields = {}
        for r in measValue.findall('.//{http://www.3gpp.org/ftp/specs/archive/28_series/28.532#measData}r'):
            p_value = r.attrib.get('p')
            meas_name = meas_types.get(p_value, f"metric_{p_value}")  # 获取参数名称
            fields[meas_name] = float(r.text)

        # 打印 fields 确认其内容是字典
        print(fields)

        # 逐个插入每个字段
        for meas_name, value in fields.items():
            point = Point("DU01001").tag("measObjLdn", meas_obj).field(meas_name, value)
            write_api.write(bucket=bucket, org=org, record=point)

# 监控新文件的创建
class NewFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(".xml"):
            print(f"New XML file detected: {event.src_path}")
            # 根据路径选择不同的处理函数
            if "/CU/" in event.src_path:
                process_cu_xml(event.src_path)
            elif "/DU/" in event.src_path:
                process_du_xml(event.src_path)

# 监控 CU 和 DU 文件夹的变更
def monitor_directories():
    event_handler = NewFileHandler()
    observer = Observer()

    # 监控 CU 文件夹
    observer.schedule(event_handler, path='/home/sftpuser/CU', recursive=False)

    # 监控 DU 文件夹
    observer.schedule(event_handler, path='/home/sftpuser/DU', recursive=False)

    observer.start()
    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

# 开始监控 CU 和 DU 目录
monitor_directories()
