#!/usr/bin/python3

import bluepy
import ruamel.yaml as yaml
import influxdb

sensors_config = 'sensors.yaml'

def load_config():
    with open(sensors_config) as f:
        conf = yaml.safe_load(f)
        return conf
        
def c2temp(char):
    return int.from_bytes(char[0:2], 'little')/100.0

def c2hum(char):
    return int.from_bytes(char[2:4], 'little')/100.0

def b2bat(char):
    return int.from_bytes(char[0:2], 'little')/100.0

def post_influxdb(client, mac, te, hu, ba):
    body = [{
        'measurement': 'sensor',
        'fields': {'hu': hu, 'te': te, 'ba': ba,},
        'tags' : { 'address':  mac },
    }]
    return client.write_points(body, time_precision='s', database="sensor")

def get_data(mac):
    temp_handle = 0x24
    battery_handle = 0x26
    device = bluepy.btle.Peripheral(mac)
    c = device.readCharacteristic(temp_handle)
    ba = b2bat(device.readCharacteristic(battery_handle))
    device.disconnect()
    return (c2temp(c), c2hum(c), ba)

if __name__ == '__main__':
    conf = load_config()
    clients = list(map(lambda h: influxdb.InfluxDBClient(host=h), conf['hosts']))
    for (mac, client) in [(mac, client) for mac in conf['sensors'] for client in clients]:
        try:
            (te, hu, ba) = get_data(mac)
            post_influxdb(client, mac, te, hu, ba)
        except Exception as e:
            # pass
            print(e)

