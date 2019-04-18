from pymongo import MongoClient
from bson import Timestamp
import sched
import time
from configparser import ConfigParser
from configparser import RawConfigParser
from kafka_facade import KafkaFacade

schedule = sched.scheduler(time.time, time.sleep)

config = ConfigParser(RawConfigParser())
config.read('config.ini')

def get_next_ts():
    kafkaServer = config.get('kafka', 'kafka.server')
    kafkaFacade = KafkaFacade(kafkaServer)
    ts_topic = config.get('kafka', 'kafka.topic.ts')
    messge = kafkaFacade.consume_messge(ts_topic)
    if messge:
        ts_metadata = messge.split(':')
        return Timestamp(ts_metadata[0], ts_metadata[1])
    return False

def execute():
    ns = config.get('condition', 'namespace')
    dbUrl = config.get('mongo', 'mongodb.url')

    interval = config.get('shedule', 'interval')
    conn = MongoClient(dbUrl)
    hugdb = conn["local"]

    queryDict = {'ns': ns}
    next_ts = get_next_ts()
    if next_ts:
        queryDict['ts'] = {'$gte': next_ts}

    for x in hugdb.oplog.rs.find(queryDict).sort({'ts':1}):
        print(x)
    schedule.enter(int(interval), 0, execute) 


def main():
    schedule.enter(0, 0, execute)
    schedule.run()

if __name__ == '__main__':  
    main()