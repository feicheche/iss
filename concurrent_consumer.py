
from kafka import KafkaConsumer
from multiprocessing import Process
from datetime import datetime, timedelta
import json
import os

def create_consumer(bootstrap_servers):
    # 创建一个Kafka消费者
    return KafkaConsumer(
        'courier_location_topic',
        bootstrap_servers=bootstrap_servers,
        group_id='group1',  # all consumers belong to the same group
        auto_offset_reset='earliest',  # start consuming from the earliest message
        value_deserializer=lambda x: x.decode('utf-8')  # 消息值的解码方法
    )

def consume_messages(consumer, consumer_id):
    # 创建一个文件，以存储从Kafka获取的消息
    file_name = f'/mnt/courier_location_{consumer_id}'
    file = open(file_name, 'w')

    # 设定日期范围
    date_start = datetime.strptime("2023-07-07", "%Y-%m-%d")
    date_end = datetime.strptime("2023-07-08", "%Y-%m-%d") + timedelta(days=1)

    try:
        # 开始无限循环，来持续获取新的消息
        for message in consumer:
            # 消息是一个包含'positionDate'字段的JSON对象
            message_data = json.loads(message.value)
            message_date = datetime.strptime(message_data.get('positionDate'), "%Y-%m-%d %H:%M:%S")

            # 如果消息的日期在范围内，那么处理这个消息
            if date_start <= message_date < date_end:
                # message.value是实际的消息内容
                file.write(message.value + '\n')
    finally:
        file.close()

# 使用并行库创建多个消费者实例
if __name__ == '__main__':
    # 服务器地址
    bootstrap_servers='xxxxxx:9092,xxxxx:9092'
    
    # 创建Kafka消费者用于获取Topic的Partition数量
    kafka_consumer = create_consumer(bootstrap_servers)

    # 获取Topic的Partition数量
    topic_partitions = kafka_consumer.partitions_for_topic('courier_location_topic')

    # 创建与Partition数量一致的消费者
    processes = []
    for i in range(len(topic_partitions)):
        # 为每个消费者创建一个新的Kafka消费者实例
        consumer = create_consumer(bootstrap_servers)
        p = Process(target=consume_messages, args=(consumer, f'consumer{i}'))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
