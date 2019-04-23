import logging
import time
from pykafka import KafkaClient
from pykafka.common import OffsetType

import logging
logger = logging.getLogger()

#### kafka的一些重要概念
# broker: 消息中间件的处理节点,一个broker是一个节点,多个broker可以组成集群
# Topic: 一类消息
# Partition: Topic物理上的分组,一个Topic可以分为多个Partition, 每个Partition都是有序的队列
# Segement: 每个Partition有多个segement文件组成
# offset: 每个partition都有一系列连续的,不可变的消息组成,这些消息被追加到partition中. 每个消息都有一个连续的序号叫做offset,作为消息在partition中的唯一标识.
# message: kafka文件中的最小存储单位, 也就是a commit log  

# 利用pykafka操作kafka
# api: https://pykafka.readthedocs.io/en/latest/
# kafka 安装配置：https://blog.csdn.net/hg_harvey/article/details/79174104
# kafka 教程：https://www.w3cschool.cn/apache_kafka/apache_kafka_fundamentals.html
# kafka 分区器：https://blog.csdn.net/abinge317/article/details/84542073
# kafka 位移(offset)与提交(commit)：https://blog.csdn.net/roshy/article/details/88579034
# kafka 消费者与消费者组：http://www.cnblogs.com/huxi2b/p/6223228.html
class KafkaFacade(object):
    def __init__(self, host="192.168.237.129:9092"):
        self.host = host
        self.client = KafkaClient(hosts=self.host)
    
    # send message in asynchronzed mode by default.
    def produce_message(self, producer, message):
        producer.produce(str(message).encode())

    def get_message(self, topic, consumer):
        logger.info('fetching message for topic: %s'%(topic.name))

        message = consumer.consume(block = False)

        if message:
            messageValue = message.value.decode()
            logger.info('New message%s fetched from topic %s'%(messageValue, topic.name))
            return messageValue

        logger.info('no message is available in topic')
        return False

    def commit(self, consumer):
        consumer.commit_offsets()

    def get_consumer(self, topic):
        # consumer = topic.get_balanced_consumer(b"consumer_group_balanced2", managed=True)
        consumer = topic.get_simple_consumer( b"ts_group", reset_offset_on_start=False, auto_commit_enable=False)
        return consumer

    def get_prducer(self, topic, sync = False):
        return topic.get_producer(sync=sync)
    
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
        logger.info('last available offsets are %s'%(last_offsets))
        logger.info('consumer held offsets are %s'%(held_offsets))
        for key in held_offsets.keys():
            last_offeset_partition = last_offsets[key].offset[0]
            held_offset_partition = held_offsets[key]
            if held_offset_partition < last_offeset_partition - 1 and last_offeset_partition > 0:
                return True
        logger.info('No pending message found')
        return False

    def release(self, consumer, producer):
        consumer.stop()
        producer.stop()
        
        
# if __name__ == '__main__':  
#     host = '192.168.137.101:9092'
#     topicName = 'topic_sync_mongo_hive_ts'
#     kafka_facade = KafkaFacade(host)
#     topic = kafka_facade.get_topic(topicName)
#     consumer = kafka_facade.get_consumer(topic)
#     msg = kafka_facade.get_message(topic, consumer)

#     if msg:
#         print("message for kafka: {}".format(msg))
#     else:
#         print('end without message')
