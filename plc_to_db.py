# import common modules
import json
import threading
import queue
import time
import getpass
import schedule
import pytz
import configparser
from datetime import datetime

# import mqtt modules
import paho.mqtt.client as mqtt

# import database modules
import psycopg2
import psycopg2.pool

#import siemens plc modules
import snap7

"""
Setting basic information
"""
config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')

# Database info
DB_HOST = config.get('Database info', 'DB_HOST')
DB_PORT = config.get('Database info', 'DB_PORT')
DB_USER = config.get('Database info', 'DB_USER')
DB_NAME = config.get('Database info', 'DB_NAME')
DB_PASSWORD = config.get('Database info', 'DB_PASSWORD')

# MQTT info
MQTT_BROKER = config.get('MQTT info', 'MQTT_BROKER')
MQTT_PORT = int(config.get('MQTT info', 'MQTT_PORT'))
MQTT_TOPIC = config.get('MQTT info', 'MQTT_TOPIC')

# PLC info
PLC_IP = config.get('PLC info', 'PLC_IP')
PLC_RACK = 0
PLC_SLOT = 1
DB_NUMBER = 15

# Sensor id information
sensor_id_list = [
    "RTD-01",
    "RTD-02",
    "RTD-03",
    "RTD-04",
    "RTD-05",
    "RTD-06",
    "RTD-07",
    "RTD-08",
    "DMT-01",
    "DMT-02",
    "Chiller-01",
    "Chiller-T",
    "Chiller-PrevT"
]

# workers initialize
num_workers = 8
workers = []

# Set up a queue for the database connection
msg_queue = queue.Queue()

# Global PLC client and prcessing lock
plc_client = None
plc_lock = threading.Lock()

# Set up threaded operation
def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

"""
set up PLC connection
"""
def init_plc():
    global plc_client
    client = snap7.client.Client()
    client.connect(PLC_IP, PLC_RACK, PLC_SLOT)
    plc_client = client
    print("PLC connection established")

def on_connect(client, userdata, flags, rc, properties):
    # print(f"Connected with result code {rc}")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Connected with result code {rc}")
    client.subscribe(MQTT_TOPIC, qos=1)

def on_message(client, userdata, msg):
    try:
        msg_queue.put(msg.payload.decode("utf-8"))
        print(f"Received message: {msg.payload}")
    except Exception as e:
        print(f"Error occured: {e}")

def on_disconnect(client, userdata, flags, rc, properties):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Disconnected with result code {rc}")

def on_log(client, userdata, level, buf):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - MQTT Log: {buf}")

"""
set up mqtt server connection
"""
# Publish the data to the MQTT broker
def publish_mqtt_batch(data):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        # payload = json.dumps({"sensor_id": sensor_id, "temperature": temperature})
        payload = json.dumps(data)
        client.publish(MQTT_TOPIC, payload, qos=1)
        print(f"Published playload to MQTT: {payload}")
    except Exception as e:
        print(f"Cannot publish data to MQTT: {e}")
    finally:
        client.disconnect()
        
"""
setting database
"""
def db_pool_setting():
    # Set up PostgresSQL connection pool
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20, # Min and max connections
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        if db_pool:
            print("PostgresSQL connection pool created")
    except Exception as e:
        print(f"Cannot create PostgresSQL connection pool: {e}")

    return db_pool

"""
Database write worker thread, retrieves messages from the queue and writes to PostgreSQL.
"""
def db_worker(worker_id):
    taipei_tz = pytz.timezone('Asia/Taipei')
    while True:
        msg_playload = msg_queue.get()
        if msg_playload is None:
            print(f"Worker {worker_id} stopping")
            msg_queue.task_done()
            break # Stop the thread
        try:
            # get a connection from the pool
            conn = db_pool.getconn()
            cursor = conn.cursor()
            data = json.loads(msg_playload)

            time_str = data['measured_at']
            local_time = datetime.fromisoformat(time_str) # fromisoformat() 函式，它能自動解析 ISO 8601 時間格式
            utc_time = local_time.astimezone(pytz.utc)
   
            insert_sql = "INSERT INTO measurement (sensor_id, metric, value, measured_at) VALUES (%s, %s, %s, %s)"       
            for sensor_id in sensor_id_list:
                cursor.execute(insert_sql, (sensor_id, data[sensor_id][1], data[sensor_id][0], utc_time))
            conn.commit()   
            cursor.close()
            db_pool.putconn(conn)
            # print(f"Woker {worker_id} inserted data to the database: {data}")
        except Exception as e:
            print(f"woker {worker_id} error: {e}")
        finally:
            msg_queue.task_done()

def workers_setting():
    for i in range(num_workers):
        t = threading.Thread(target=db_worker, args=(i+1,), daemon=True)
        t.start()
        workers.append(t)

"""
PLC works functions
"""
    
# Get the data from the PLC
def read_sensor_real(offset):
    with plc_lock:
        data = plc_client.db_read(DB_NUMBER, offset, 4)
    return snap7.util.get_real(data, 0)

def schedule_job():
    try:
        # temperature
        rtd01 = read_sensor_real(6)
        rtd02 = read_sensor_real(44)
        rtd03 = read_sensor_real(82)
        rtd04 = read_sensor_real(120)
        rtd05 = read_sensor_real(158)
        rtd06 = read_sensor_real(196)
        rtd07 = read_sensor_real(234)
        rtd08 = read_sensor_real(272)
        # dew point
        dmt01 = read_sensor_real(314)
        dmt02 = read_sensor_real(356)
        # chiller temp
        chiller01 = read_sensor_real(410)
        chiller02 = read_sensor_real(418)
        chiller03 = read_sensor_real(422)

        # Combine the data into a single payload
        data = {
            "RTD-01": [rtd01, 'temperature_C'],
            "RTD-02": [rtd02, 'temperature_C'],
            "RTD-03": [rtd03, 'temperature_C'],
            "RTD-04": [rtd04, 'temperature_C'],
            "RTD-05": [rtd05, 'temperature_C'],
            "RTD-06": [rtd06, 'temperature_C'],
            "RTD-07": [rtd07, 'temperature_C'],
            "RTD-08": [rtd08, 'temperature_C'],
            "DMT-01": [dmt01, 'dewpoint_C'],
            "DMT-02": [dmt02, 'dewpoint_C'],
            "Chiller-01":[chiller01, 'temperature_C'],
            "Chiller-T": [chiller02, 'temperature_C'],
            "Chiller-PrevT": [chiller03, 'temperature_C'],
            "measured_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        publish_mqtt_batch(data)
    except Exception as e:
        print(f"Error reading PLC data: {e}")

def main():

    # set up db pools and workers
    db_pool_setting()
    workers_setting()
    

    # Schedule the job with 20 seconds interval
    schedule.every().minute.at(":00").do(run_threaded, schedule_job)
    schedule.every().minute.at(":30").do(run_threaded, schedule_job)

    # Set up the MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_log = on_log

    # Connect to the MQTT broker
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()
    
    # Establish connection to the PLC
    init_plc()

    try:
        while True:
            schedule.run_pending()
            time.sleep(10)
            print(f"Queue size: {msg_queue.qsize()}")
    except KeyboardInterrupt:
        print("Exiting")        
    except Exception as e:  
        print(f"Error occured: {e}")

    finally:
        client.loop_stop()
        for _ in range(num_workers):
            msg_queue.put(None)
        for worker in workers:
            worker.join() # Wait for the worker threads to finish
        # msg_queue.put(None) # Stop the worker thread

if __name__ == "__main__":
    main()