'''
Description: expert_lz_task表查找kId程序
Author: jtx
Time: 2020-10-10 17:09
'''


import os
from logging.handlers import RotatingFileHandler
import logging
import pymongo

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

# 获取当前文件名，建立log
dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

def set_log():
    logging.basicConfig(level=logging.INFO)
    file_log_handler = RotatingFileHandler(os.path.join(dir_path,"log.txt"), maxBytes=1024 * 1024 * 300, backupCount=10)
    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)s - %(levelname)s - %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)

set_log()
logger = logging.getLogger(__name__)

client = pymongo.MongoClient('xxx')
db1 = client.res_kb
db1.authenticate("xxx", "xxx", mechanism='MONGODB-CR')
expert_list_collection = db1.expert_list
expert_index_collection = db1.expert_index

for num, i in enumerate(expert_list_collection.find()):
    record = {}
    name = i['expert_name']
    logger.info("num:{}, expert_name:{} ".format(num, name))
    institution = i['research_institution']
    department = i['department']
    url = i['url']
    kId = i['kId']
    if not kId:
        r_in_dbs = expert_index_collection.find({'expert_name': name})
        if r_in_dbs:
            for i2 in r_in_dbs:
                if institution in i2['research_institution']:
                    kId = i2.get('kId', '')
                    logger.info("kId:{}".format(kId))
                    if kId:
                        myquery = {"_id": i["_id"]}
                        newvalues = {"$set": {"kId": kId}}
                        expert_list_collection.update_one(myquery, newvalues)
                        logger.info('success {}'.format(str(kId)))
        else:
            kId = ''
            logger.info('no kId')
