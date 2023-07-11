
from kafka import KafkaConsumer
from multiprocessing import Process
from datetime import datetime, timedelta
import json
import os
import mysql.connector

OUTPUT_TO_DB = True

db_config = {
    'user': 'db_user',
    'password': 'db_password',
    'host': 'db_host',
    'database': 'db_name',
    'raise_on_warnings': True
}

def create_consumer(bootstrap_servers):
    return KafkaConsumer(
        'courier_location_topic',
        bootstrap_servers=bootstrap_servers,
        group_id='group1',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: x.decode('utf-8')
    )

def consume_messages(consumer, consumer_id):
    date_start = datetime.strptime("2023-07-07", "%Y-%m-%d")
    date_end = datetime.strptime("2023-07-08", "%Y-%m-%d") + timedelta(days=1)

    try:
        for message in consumer:
            message_data = json.loads(message.value)
            message_date = datetime.strptime(message_data.get('positionDate'), "%Y-%m-%d %H:%M:%S")

            if date_start <= message_date < date_end:
                if OUTPUT_TO_DB:
                    write_to_db(message.value)
                else:
                    write_to_file(message.value, consumer_id)
    finally:
        if not OUTPUT_TO_DB:
            file.close()

def write_to_db(message):
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()

    message_data = json.loads(message)

    add_data = ("INSERT INTO courier_location "
               "(isDrift, appVersion, supplierId, OS, orderId, updateTime, cityId, userId, speed, listenStatus, "
               "locationList, grabStatus, taskAppointType, travelWay, positionDate, loginName, isOnceLocation, "
               "position, locType, radius, status) "
               "VALUES (%s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    data_values = (message_data.get('isDrift'), message_data.get('appVersion'), message_data.get('supplierId'), message_data.get('OS'),
                   message_data.get('orderId'), message_data.get('updateTime'), message_data.get('cityId'), message_data.get('userId'),
                   message_data.get('speed'), message_data.get('listenStatus'), json.dumps(message_data.get('locationList')),
                   message_data.get('grabStatus'), message_data.get('taskAppointType'), message_data.get('travelWay'),
                   message_data.get('positionDate'), message_data.get('loginName'), message_data.get('isOnceLocation'),
                   json.dumps(message_data.get('position')), message_data.get('locType'), message_data.get('radius'),
                   message_data.get('status'))

    cursor.execute(add_data, data_values)

    cnx.commit()

    cursor.close()
    cnx.close()

def write_to_file(data, consumer_id):
    file_name = f'/mnt/courier_location_{consumer_id}'
    file = open(file_name, 'w')
    file.write(data + '\n')

if __name__ == '__main__':
    bootstrap_servers='xxxxxx:9092,xxxxx:9092'

    kafka_consumer = create_consumer(bootstrap_servers)

    topic_partitions = kafka_consumer.partitions_for_topic('courier_location_topic')

    processes = []
    for i in range(len(topic_partitions)):
        consumer = create_consumer(bootstrap_servers)
        p = Process(target=consume_messages, args=(consumer, f'consumer{i}'))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
