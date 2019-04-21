import logging
import time
from pykafka import KafkaClient
from pykafka.common import OffsetType

import logging
logger = logging.getLogger()


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
