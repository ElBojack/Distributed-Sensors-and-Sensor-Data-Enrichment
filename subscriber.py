from os import write
import paho.mqtt.client as mqtt # pip install paho.mqtt
import time
import datetime
from rdflib import Graph, URIRef, BNode, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD, SOSA, TIME
import numpy as np

g = Graph()

BASE = Namespace("http://example.org/data/")
QUDT11 = Namespace("http://qudt.org/1.1/schema/qudt#")
QUDTU11 = Namespace("http://qudt.org/1.1/vocab/unit#")
CDT = Namespace("http://w3id.org/lindt/custom_datatypes#")

g.bind('rdf', RDF)
g.bind('rdfs', RDFS)
g.bind('xsd', XSD)
g.bind('sosa', SOSA)
g.bind('time', TIME)
g.bind('qudt-1-1', QUDT11)
g.bind('qudt-unit-1-1', QUDTU11)
g.bind('cdt', CDT)
##### Ressources #####
# earthAtmosphere ressources
earthAtmosphere = URIRef('earthAtmosphere')
earthAtmosphere_label = Literal("Atmosphere of Earth", lang="en")

# Iphone ressources
iphone7 = URIRef('iphone7/35-207306-844818-0')
iphone7_label = Literal("IPhone 7 - IMEI 35-207306-844818-0", lang="en")
iphone7_comment = Literal("IPhone 7 - IMEI 35-207306-844818-0 - John Doe", lang="en")

# sensor ressources
sensor = URIRef('sensor/35-207306-844818-0/BMP282')
sensor_obs = URIRef('sensor/35-207306-844818-0/BMP282/atmosphericPressure')
sensor_label = Literal("Bosch Sensortec BMP282", lang="en")

# Observation 346345 ressources
observation_346345 = URIRef('Observation/346345')
###################

##### RDF Triples #####
# earthAtmosphere
g.add((earthAtmosphere, RDF.type, SOSA.FeatureOfInterest))
g.add((earthAtmosphere, RDFS.label, earthAtmosphere_label))

# iphone 7
g.add((iphone7, RDF.type, SOSA.Platform))
g.add((iphone7, RDFS.label, iphone7_label))
g.add((iphone7, RDFS.comment, iphone7_comment))
g.add((iphone7, SOSA.hosts, sensor))

# sensor
g.add((sensor, RDF.type, SOSA.Observation))
g.add((sensor, RDFS.label, sensor_label))
g.add((sensor, SOSA.observes, sensor_obs))
# Observation 346345
g.add((observation_346345, RDF.type, SOSA.Observation))
g.add((observation_346345, SOSA.observedProperty, sensor_obs))
g.add((observation_346345, SOSA.hasFeatureOfInterest, earthAtmosphere))
g.add((observation_346345, SOSA.madeBySensor, sensor_obs))
observation_346345_result = BNode()
g.add((observation_346345, SOSA.hasResult, observation_346345_result))
g.add((observation_346345_result, RDF.type, QUDT11.QuantityValue))
g.add((observation_346345_result, QUDT11.numericValue, Literal("101936", datatype=XSD.double)))
g.add((observation_346345_result, QUDT11.unit, QUDTU11.Pascal))
observation_346345_resultTime = BNode()
g.add((observation_346345, SOSA.resultTime, observation_346345_resultTime))
g.add((observation_346345_resultTime, RDF.type, TIME.Instant))
g.add((observation_346345_resultTime, TIME.inXSDDateTimeStamp, Literal("2017-06-06T12:36:13+00:00", datatype=XSD.dateTimeStamp)))

########################

def print_graph(gg):
    print(gg.serialize(format='ttl', base=BASE).decode('u8'))

########################

def on_message(client, userdata, message):
    [reading, dt, id] = message.payload.decode('utf-8').split('|')    
    new_obs = URIRef("Observation/" + id)

    print("Message received")

    g.add((new_obs, SOSA.observedProperty, sensor_obs))
    g.add((new_obs, SOSA.hasFeatureOfInterest, earthAtmosphere))
    g.add((new_obs, SOSA.madeBySensor, sensor))
    g.add((new_obs, SOSA.hasSimpleResult, Literal(reading, datatype=CDT.ucum)))
    g.add((new_obs, SOSA.resultTime, Literal(dt, datatype=XSD.dateTime)))

print("creating new instance")
client = mqtt.Client("P1")     # create new instance (the ID, in this case "P1", must be unique)
client.on_message = on_message # attach "on_message" callback function (event handler) to "on_message" event

#broker_address = "localhost" # Use your own MQTT Server IP Adress (or domain name) here, or ...
broker_address = "test.mosquitto.org" # ... use the Mosquitto test server during development

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
        message = f'{reading}|{dt}|{str(i + 1)}'
        print("Publishing message to topic: teds20/group10/pressure")
        client.publish(topic="teds20/group10/pressure", payload=message, qos=2) # publish
        time.sleep(1)

    print("Unsubscribing from topic: teds20/group10/pressure")
    client.unsubscribe("teds20/group10/pressure") # unsubscribe

    time.sleep(4)       # wait 4 seconds before stopping the event processing loop (so all pending events are processed)
    client.loop_stop()  # stop the event processing loop

    print("\ndisconnecting from broker\n")
    client.disconnect() # disconnect from broker
except Exception as e:
    # if we receive an exception (error) in the "try" block,
    # handle it here, by printing out the error message
    print(f"connection error: {e}")

print_graph(g)

with open("publisher.ttf", "w") as f:
    f.write(g.serialize(format='ttl', base=BASE).decode('u8'))