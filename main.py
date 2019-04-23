import sys
from os import path
from configparser import ConfigParser
from configparser import RawConfigParser
import logging
from mongosynchronizer import MongoSynchronizer
from LogConfigurer import LogConfigurer

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        config_file = sys.argv[1]
    else:
        config_file='config.ini'

    # 利用parse configuer读取配置信息
    # 阅读资料: https://blog.csdn.net/henulwj/article/details/49174355
    if config_file == 'config.ini':
        proDir = path.dirname(path.realpath(__file__))
        configPath = path.join(proDir, "config.ini")
        filePath = path.abspath(configPath)
    else:
        filePath = path.abspath(config_file)
    config = ConfigParser(RawConfigParser())
    config.read(filePath)

    logConfigurer = LogConfigurer(config)
    logConfigurer.configure()

    synchronizer = MongoSynchronizer(config)
    synchronizer.start()