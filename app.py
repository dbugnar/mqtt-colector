import paho.mqtt.client as mqtt
from mongodb import db_get_topics, db_add_topic, db_add_message
from broker import update_broker_info
from message import Message
import time
import os

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # client.subscribe("#", 0)
    # client.subscribe("#", 1)
    client.subscribe("#")
    client.subscribe("$SYS/#")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    topics = db_get_topics()
    # print(msg.retain)
    if msg.topic not in topics:
        db_add_topic(msg.topic)
        print('Adding new topic' + msg.topic + '.')
    if msg.topic.startswith('$SYS/broker'):
        p = "".join(msg.topic.split('broker', 1)[1].replace('/', " ")[1:]).replace(' ','_')
        update_broker_info(p, msg.payload.decode('utf-8'))
    else:
        message = Message(msg.payload.decode('utf-8'), msg.qos, int(time.time()))
        db_add_message(msg.topic, message)
    # print(msg.topic+" "+str(msg.payload))


if __name__ == '__main__':
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    ip = os.getenv('MQTT_IP')
    port = os.getenv('MQTT_PORT')
    if ip is None or port is None:
        print('MQTT_IP or MQTT_PORT not found as a environment variable')
        exit(1)
    client.connect(ip, int(port), 60)
    client.loop_forever()