from pymongo import MongoClient
from bson import Timestamp
import sched
import time
from datetime import datetime
from kafka_facade import KafkaFacade

import logging
logger = logging.getLogger()

# 利用pymongo来操作mongodb,
# 阅读资料：
#   中文教程：http://www.runoob.com/mongodb/mongodb-java.html
#   API：http://api.mongodb.com/python/current/
class MongoSynchronizer(object):
    def __init__(self, config):
        logger.info('initializing mongo sychronizer...')
        self.config = config

        # init sheduler
        self.schedule = sched.scheduler(time.time, time.sleep)

        # init kakfka
        kafkaServer = self.config.get('kafka', 'kafka.server')
        self.kafkaFacade = KafkaFacade(kafkaServer)
        ts_topic = self.config.get('kafka', 'kafka.topic.ts')
        self.ts_topic = self.kafkaFacade.get_topic(ts_topic)

    def parseTimestamp(self, tsString):
        timestampParts = tsString.split(':')
        if len(timestampParts) < 2:
            raise ValueError("Invalid Message found, a Timestamp should like [1553406314:1], but [{}] is found.".format(tsString))
        return Timestamp(int(timestampParts[0]), int(timestampParts[1]))

    def get_current_ts(self, ts_consumer, ts_producer):
        has_pendding_msg = self.kafkaFacade.has_pendding_message(self.ts_topic, ts_consumer)
        if self.kafkaFacade.is_new_consumer(ts_consumer) and not has_pendding_msg:
            initStamp = Timestamp(datetime.utcnow(), 1)
            initMessage = '%s:%s'%(initStamp.time, initStamp.inc)
            self.kafkaFacade.produce_message(ts_producer, initMessage)
            return False
    
        if has_pendding_msg:
            messge = self.kafkaFacade.get_message(self.ts_topic, ts_consumer)
            if messge:
                ts_metadata = messge.split('~')
                return list(map(self.parseTimestamp, ts_metadata))

        return False

    def execute(self):
        interval = self.config.get('shedule', 'interval')

        try:
            ts_consumer = self.kafkaFacade.get_consumer(self.ts_topic)
            ts_producer = self.kafkaFacade.get_prducer(self.ts_topic)

            next_ts = self.get_current_ts(ts_consumer, ts_producer)
            if not next_ts:
                self.schedule.enter(int(interval), 0, self.execute)
                return

            ns = self.config.get('condition', 'namespace')
            queryDict = {'ns': ns}
            next_start_ts = None
            if next_ts and len(next_ts) == 1:
                queryDict['ts'] = {'$lt': next_ts[0]}
                next_start_ts = next_ts[0]
            elif len(next_ts) == 2:
                queryDict['ts'] = {'$gte': next_ts[0], '$lt': next_ts[1]}
                next_start_ts = next_ts[1]

            dbUrl = self.config.get('mongo', 'mongodb.url')
            conn = MongoClient(dbUrl)
            mongo_local = conn["local"]
            logger.info('synchronizing data for %s...'%(queryDict))
            for x in mongo_local.oplog.rs.find(queryDict).sort([('ts', 1)]):
                print(x) # TODO: handle every oplogs row - add sync logic here
            logger.info('data synchronized for %s...'%(queryDict))

            logger.info('figuring out next synchronztion ts scope ...')
            next_end_ts = Timestamp(datetime.utcnow(), 1)
            for x in mongo_local.oplog.rs.aggregate([{'$match': {'ts': {'$gt': next_start_ts}}}, {'$group': {'_id': '1', 'next_end_ts': {'$max': '$ts'}}}]):
                next_end_ts = x['next_end_ts']
        
            # post next synch message
            message = '%s:%s~%s:%s'%(next_start_ts.time, next_start_ts.inc, next_end_ts.time, next_end_ts.inc)
            self.kafkaFacade.produce_message(ts_producer, message)
            self.kafkaFacade.commit(ts_consumer)
        except BaseException:
            logger.error("Exception Occurs: ", exc_info = True)
        finally:
            self.kafkaFacade.release(ts_consumer, ts_producer)


        self.schedule.enter(int(interval), 0, self.execute) 

    def start(self):
        self.schedule.enter(0, 0, self.execute)
        self.schedule.run()

if __name__ == '__main__':  
    synchor = MongoSynchronizer()
    synchor.start()
    # print(Timestamp(datetime.utcnow(), 1))