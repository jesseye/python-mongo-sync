import time
from pykafka import KafkaClient
from pykafka.common import OffsetType


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

        topic = self.client.topics[topic.encode()]
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
    
    def consume_messge(self, topic, offset=0):
        """
        使用simple consumer去消费kafka
        :return:
        """
        topic = self.client.topics[topic.encode()]
        # consumer = topic.get_balanced_consumer(b"consumer_group_balanced2", managed=True)
        consumer = topic.get_simple_consumer( b"ts_group", reset_offset_on_start=False)
        partition = topic.partitions[0]
        # print("分区 {}".format(partition))
        last_offset = topic.latest_available_offsets()[0].offset
        # print("最近可用offset {}".format(last_offset))
        offset = consumer.held_offsets[0]
        # print("当前消费者分区offset情况{}".format(offset))
        if offset < last_offset[0]:
            msg = consumer.consume()
            consumer.commit_offsets([(partition, last_offset[0])])
            offset = consumer.held_offsets
            # print("{}, 当前消费者分区offset情况{}".format(msg.value.decode(), last_offset[0]))
            print("end")
            return msg.value.decode()
        
        return False
        
        
        
if __name__ == '__main__':  
    host = '192.168.137.101:9092'
    topic = 'topic_sync_mongo_hive_ts'
    # kafka_ins.producer_partition(topic)
    # kafka_ins.producer_designated_partition(topic)
    # kafka_ins.async_produce_message(topic)
    kafka_facade = KafkaFacade(host)
    # kafka_facade.produce_message('test msg3', topic)
    kafka_facade.consume_messge(topic, 2)
