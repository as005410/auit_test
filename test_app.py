# Zaprojektować, napisać w języku Python 3 i uruchomić prototyp aplikacji przetwarzającej dane pomiarowe pochodzące z brokera komunikatów MQTT. Broker jest zasilany asynchronicznie.

# Uzasadnienie podjętych decyzji projektowych pominiętych w opisie 

# Dla podlączenia bazy danych PostgreSQL użyt moduł psycopg2
# Stworzony odzielny plik konfiguracyjny .ini dla przechowywania informacji o bazie danych oraz plik config.py z funlcją odzytywania .ini pliku.
# Dla komunikacji z MQTT brokerem użyt moduł paho-mqtt
# Dla obsługi asynchronicznego działania MQTT brokera użyto moduł asyncio
# Moduł statistics pozwala na obliczanie agregatów (min, max, median, average) dla danych pomiarowych

# Opis ograniczeń zaproponowanego rozwiązania

# Z powodu braku połączenia z brokerem aplikacja nie może odzytać dane  oraz ich przetwarzyć. Ale jeżeli załozuć że połączenie z brokerem istnee i aplikacja działa jak oczekiwano to dane pomiarowe (gdzie powinno być N pomiarów w T sekund) nie będą agregowane.


import random
import psycopg2
from config import config
import statistics
import argparse
import paho.mqtt.client as mqtt
import json
import asyncio


#connect do db
# read connection parameters
params = config()

print('Connecting to the PostgreSQL database...')
conn = psycopg2.connect(**params)		
cur = conn.cursor()
  
#creating table if doesn't exist 
cur.execute(
    "CREATE TABLE IF NOT EXISTS measurements_data (period_start TIMESTAMP, period_end TIMESTAMP, N INTEGER, min REAL, max REAL, median REAL, average REAL)"
)
conn.commit()
print('Creating table to store measurements data')

def aggregate_data(data):
    return {
        "period_start": data[0]["time"],
        "period_end": data[-1]["time"],
        "N": len(data),
        "min": min(x["value"] for x in data),
        "max": max(x["value"] for x in data),
        "median": statistics.median(x["value"] for x in data),
        "average": statistics.mean(x["value"] for x in data),
    }

def insert_aggregated_data(aggregate):
    print('Inserting data to database')
    cur.execute(
        "INSERT INTO measurements_data (period_start, period_end, N, min, max, median, average) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (
            aggregate["period_start"],
            aggregate["period_end"],
            aggregate["N"],
            aggregate["min"],
            aggregate["max"],
            aggregate["median"],
            aggregate["average"],
        ),
    )
    conn.commit()
#getting arguments from console
parser = argparse.ArgumentParser()
parser.add_argument("--T", type=int, required=True, help="Okres agregacji (w sekundach)")
parser.add_argument("--N", type=int, required=True, help="Liczba pomiarów dla okresu agregacji")
args = parser.parse_args()
T = args.T
N = args.N

#array for data from mqtt
measurements = []

#function for read and agregate data from mqtt
def on_message(client, userdata, msg):
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    #convert json to object
    print('Converting reseived data')
    data = json.loads(m_decode)
    measurements.append(data)

    if len(measurements) == N:
        # Agregate data and insert to database
        aggregate = aggregate_data(measurements)
        insert_aggregated_data(aggregate)
        print('Inserting data to database')
        #clear array with data
        measurements.clear()
    else:
        # Send measurements back to mqtt with ignored status
        client.publish("ignored", json.dumps(data))

# example broker data
broker = 'broker1'
port = 1111
topic = "measurements"
client_id = f'python-mqtt-{random.randint(0, 1000)}'

# Connecting mqtt broker
async def connect_mqtt():
    client = mqtt.Client(client_id)
    client.connect(broker, port)
    client.subscribe(topic)
    client.on_message = on_message
    client.loop_start()

# Setting time period for data agregation
async def wait_for_aggregator():
    while True:
        #read data in T periods
        await asyncio.sleep(T)
        if measurements:
            aggregate = aggregate_data(measurements)
            insert_aggregated_data(aggregate)
            measurements.clear()
    
async def main():
    #group awaitables
    await asyncio.gather(connect_mqtt(), wait_for_aggregator())

asyncio.run(main())