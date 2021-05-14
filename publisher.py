import paho.mqtt.client as mqtt # pip install paho.mqtt
import time
import datetime
import numpy as np

print("creating new instance")
client = mqtt.Client("P1")     # create new instance (the ID, in this case "P1", must be unique)

#broker_address = "localhost" # Use your own MQTT Server IP Adress (or domain name) here, or ...
broker_address = "test.mosquitto.org" # ... use the Mosquitto test server during development
client.username_pw_set("admin", "password")
# Use exception handling (try...except in Python)
try:
    print("connecting to broker")
    client.connect(broker_address) # connect to broker
    client.loop_start()            # start the event processing loop

    print("Subscribing to topic: teds20/group10/pressure")
    client.subscribe("teds20/group10/pressure", qos=2) # subscribe

    for i in range(10):
        mu, sigma = 1200.00, 1.0
        reading = f'{round(np.random.normal(mu, sigma), 2):.2f}'        
        dt = datetime.datetime.now()
        dt = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        message = f'{reading}|{dt}'
        print("Publishing message to topic: teds20/group10/pressure")
        client.publish(topic="teds20/group10/pressure", payload=message, qos=2) # publish
        time.sleep(1)

    print("Unsubscribing from topic: teds20/group10/pressure")
    client.unsubscribe("teds20/group10/pressure") # unsubscribe

    time.sleep(4)       # wait 4 seconds before stopping the event processing loop (so all pending events are processed)
    client.loop_stop()  # stop the event processing loop

    print("\ndisconnecting from broker")
    client.disconnect() # disconnect from broker
except Exception as e:
    # if we receive an exception (error) in the "try" block,
    # handle it here, by printing out the error message
    print(f"connection error: {e}")
