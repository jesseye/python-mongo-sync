import logging, sys, re


# 利用PYTHON logging 模块来打印日志 
# logging 介绍: https://www.cnblogs.com/yyds/p/6901864.html
class LogConfigurer(object):
    def __init__(self, config):
        self.config = config
    
    def configure(self):
        logger = logging.getLogger()

        level_txt = self.config.get('logging', 'logging.level')  
        level = self.get_log_level(level_txt)
        logger.setLevel(level)
 
        format = '%(asctime)s - %(filename)s - %(funcName)s - %(lineno)s - %(levelname)s - %(message)s'

        logger_name = self.config.get('logging', 'logging.logger')
        # 记录日志到文件
        log_file = self.config.get('logging', 'logging.file.path')  
        if 'file' in logger_name and log_file:
            file_handler = logging.FileHandler(log_file) 
            file_handler.setLevel(level)
            file_formatter = logging.Formatter(format)
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
            self.file_handler = file_handler

        # 添加日志处理器，输出日志到控制台
        if 'console' in logger_name or not log_file:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            console_formatter = logging.Formatter(format)

            console_handler.setFormatter(console_formatter)
            file_filter = FileFilter('')
            console_handler.addFilter(file_filter)
            logger.addHandler(console_handler)
            self.console_handler = console_handler
    
    def get_log_level(self, level):
        if level == 'INFO':
            return logging.INFO
        elif level == 'DEBUG':
            return logging.DEBUG
        elif level == 'WARN':
            return logging.WARN
        elif level == 'ERROR':
            return logging.ERROR
        else:
            return logging.INFO

# 根据日志中的文件名字段,过滤干扰信息
class FileFilter(object):
    def __init__(self, include_exp):
        self.include_exp = r'%s'%(include_exp)

    def filter(self, record):
        try:
            file_name = record.filename
        except AttributeError:
            return False

        # if re.match(self.include_exp, filter_key).group(0)
        if file_name == 'mongosynchronizer.py' or file_name == 'kafka_facade.py':
            return True
        else:
            return False