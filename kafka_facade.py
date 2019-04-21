import logging
import time
from pykafka import KafkaClient
from pykafka.common import OffsetType

import logging

# LOG_FILE = 'mylog.log'

# file_handler = logging.FileHandler(LOG_FILE) #输出到文件
console_handler = logging.StreamHandler()  #输出到控制台
# file_handler.setLevel('ERROR')     #error以上才输出到文件
console_handler.setLevel('INFO')   #info以上才输出到控制台

fmt = '%(asctime)s - %(funcName)s - %(lineno)s - %(levelname)s - %(message)s'  
formatter = logging.Formatter(fmt) 
# file_handler.setFormatter(formatter) #设置输出内容的格式
console_handler.setFormatter(formatter)

logger = logging.getLogger('kafka_facade')
# logger.setLevel('DEBUG')     #设置了这个才会把debug以上的输出到控制台

# logger.addHandler(file_handler)    #添加handler
logger.addHandler(console_handler)


class KafkaFacade(object):
    def __init__(self, host="192.168.237.129:9092"):
        self.host = host
        self.client = KafkaClient(hosts=self.host)
    
    def produce_message(self, message, topic, sync=False):
        """
        异步生产消息，消息会被推到一个队列里面，
        另外一个线程会在队列中消息大小满足一个阈值（min_queued_messages）
        或到达一段时间（linger_ms）后统一发送,默认5s
        :return:
        """

        last_offset = topic.latest_available_offsets()
        print("最近的偏移量 offset {}".format(last_offset))

        # 记录最初的偏移量
        prevous_offset = last_offset[0].offset[0]
        p = topic.get_producer(sync=False, partitioner=lambda pid, key: pid[0])
        p.produce(str(message).encode())

        s_time = time.time()
        while not sync:
            last_offset = topic.latest_available_offsets()
            print("最近可用offset {}".format(last_offset))
            if last_offset[0].offset[0] != prevous_offset:
                e_time = time.time()
                print('cost time {}'.format(e_time-s_time))
                break
            time.sleep(1)
        print('end')

    def get_messge(self, topic, consumer):
        """
        使用simple consumer去消费kafka
        :return:
        """
        if self.has_pendding_message(topic, consumer):
            message = consumer.consume()

            if message:
                messageValue = message.value.decode()
                print('New message{} fetched from topic {}'.format(messageValue, topic.name))
                return messageValue

        return False

    def commit(self, consumer):
        consumer.commit_offsets()

    def get_consumer(self, topic):
        # consumer = topic.get_balanced_consumer(b"consumer_group_balanced2", managed=True)
        consumer = topic.get_simple_consumer( b"ts_group", reset_offset_on_start=False, auto_commit_enable=False)
        return consumer

    
    def get_topic(self, topicName):
        return self.client.topics[topicName.encode()]

    def is_new_consumer(self, consumer):
        held_offsets = consumer.held_offsets
        for key in held_offsets.keys():
            if held_offsets[key] >= 0:
                return False
        return True

    def has_pendding_message(self, topic, consumer):
        last_offsets = topic.latest_available_offsets()

        held_offsets = consumer.held_offsets
        for key in held_offsets.keys():
            last_offeset_partition = last_offsets[key].offset[0]
            held_offset_partition = held_offsets[key]
            if held_offset_partition < last_offeset_partition - 1 and last_offeset_partition > 0:
                return True
        return False
        
        
if __name__ == '__main__':  
    host = '192.168.137.101:9092'
    topicName = 'topic_sync_mongo_hive_ts'
    kafka_facade = KafkaFacade(host)
    topic = kafka_facade.get_topic(topicName)
    consumer = kafka_facade.get_consumer(topic)
    msg = kafka_facade.get_messge(topic, consumer)

    if msg:
        print("message for kafka: {}".format(msg))
    else:
        print('end without message')
