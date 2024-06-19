import paho.mqtt.client as mqtt
import socket
import struct
import codecs
from datetime import datetime, timedelta
import time
import yaml

with open("config.yml", "r") as yml:
    config = yaml.safe_load(yml)

mqtt_host = str(config["mqtt"]["host"])
mqtt_port = int(config["mqtt"]["port"])
mqtt_topic = str(config["mqtt"]["topic"])
modbus_host = str(config["modbus"]["host"])
modbus_port = int(config["modbus"]["port"])
modbus_unitid = int(config["modbus"]["unitid"])
BUFFER_SIZE = 512

current_hour_energy = 0

def send_data(host, port, data):
    try:
        # TCP/IPソケットを作成
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # サーバーに接続
            sock.connect((host, port))
            print("サーバーに接続しました。")

            # データをサーバーに送信
            sock.send(data)
            print("TX: {0}".format(codecs.encode(data, 'hex_codec')))

            # データの受信
            response = sock.recv(BUFFER_SIZE)
            print("RX: {0}".format(codecs.encode(response, 'hex_codec')))
            return response

    except socket.error as e:
        print(f"ソケットエラーが発生しました: {e}")
        return None
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
        return None

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected disconnection.")

def on_publish(client, userdata, mid):
    print("Message published.")

def main():
    # データ構造
    transactionId = 0
    protocolId = 0
    length = 6
    unitId = modbus_unitid
    functionCode = 3
    startRegister = 0x0200
    data = 0x0002
    req = struct.pack('>HHHBBHH', int(transactionId), int(protocolId), int(length), int(unitId), int(functionCode), int(startRegister), int(data))
    response = send_data(modbus_host, modbus_port, req)
    if response:
        start_index = 8
        end_index = start_index + 8
        extracted_hex = response[-8:-4]
        extracted_hex_string = extracted_hex.hex()
        correct_response = "070" + str(modbus_unitid) + "0304"
        if extracted_hex_string == correct_response:
            current_hour_energy = int.from_bytes(response[-4:], byteorder='big', signed=False)
            client.publish(mqtt_topic, current_hour_energy)
            print(f"Current hour Energy: {current_hour_energy}")
        else:
            print(extracted_hex)
            print(correct_response)
            print("Invalid response.")

if __name__ == '__main__':
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish

    client.connect(mqtt_host, mqtt_port, 60)
    transactionId = 0
    protocolId = 0
    length = 6
    unitId = modbus_unitid
    functionCode = 3
    startRegister = 0x0200
    data = 0x0002
    req = struct.pack('>HHHBBHH', int(transactionId), int(protocolId), int(length), int(unitId), int(functionCode), int(startRegister), int(data))
    response = send_data(modbus_host, modbus_port, req)
    if response:
        current_hour_energy = int.from_bytes(response[-4:], byteorder='big', signed=False)
    try:
        client.loop_start()
        while True:
            now = datetime.now()
            if now.minute == 11:
                main()
            elif now.minute == 21:
                main()
            elif now.minute == 31:
                main()
            elif now.minute == 41:
                main()
            elif now.minute == 51:
                main()
            elif now.hour != 0 and now.minute == 1:
                main()
            elif now.hour == 0 and now.minute == 1:
                transactionId = 0
                protocolId = 0
                length = 6
                unitId = modbus_unitid
                functionCode = 6
                startRegister = 0xFFFF
                data = 0x0300
                req = struct.pack('>HHHBBHH', int(transactionId), int(protocolId), int(length), int(unitId), int(functionCode), int(startRegister), int(data))
                send_data(modbus_host, modbus_port, req)
            next_minute = now.replace(second=0) + timedelta(minutes=1)
            sleep_time = (next_minute - now).total_seconds()
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        client.loop_stop()
