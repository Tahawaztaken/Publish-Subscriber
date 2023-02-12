import paho.mqtt.client as mqtt
import sys
import json

def on_connect(a, b, c, d):
    print("Connected to broker.")

def on_message(a, b, msg):
    message = msg.payload.decode()
    message = message.replace("'", "\"")
    if len(message) < 10:
        print("Discarding retainer message.")
    else:
        message = json.loads(message)
        topic_message = f"{message['time']} {message[list(message)[-1]]}"
        print(f"{message['time']} {list(message)[-1]} payload processed.")
        file = open(f"{list(message)[-1]}.txt", "a")
        file.write(f"{topic_message}\n")
        file.close()
        global temperature_monitor
        global windspeed_monitor
        global rain_monitor
        if list(message)[-1] == "temperature":
            if message[list(message)[-1]] < 10:
                temperature_monitor += 1
                if temperature_monitor == 6:
                    print("Warning! Temperature has been below 10 degrees for 30 mins.")
                    temperature_monitor = 0
            else:
                temperature_monitor = 0
        elif list(message)[-1] == "windspeed":
            if message[list(message)[-1]] > 20:
                windspeed_monitor += 1
                if windspeed_monitor == 2:
                    print("Warning! High wind speeds!")
                    windspeed_monitor = 0
            else:
                windspeed_monitor = 0
        elif list(message)[-1] == "raining":
            if message[list(message)[-1]] == "True":
                rain_monitor += 1
                if rain_monitor == 12:
                    print("Warning! Heavy rain for the past hour!")
                    rain_monitor = 0
            else:
                rain_monitor = 0

temperature_monitor = 0
windspeed_monitor = 0
rain_monitor = 0

broker = "broker.hivemq.com"
port = 1883
topic = sys.argv[1]

client = mqtt.Client()
client.connect(broker, port)
client.subscribe(topic)
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()











