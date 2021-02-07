#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-06-01 15:05
# Last modified: 2020-06-01 15:18
# Filename     : expert_category_refresh.py
# Description  : 手动执行脚本： 处理爬虫更新的专家标签
# 1 重新过一遍爬虫库的专家标签，并更新
#******************************************************************************
import configparser
import sys
from pymongo import MongoClient
from pymongo import errors
from pyArango.connection import Connection as ArangoConnection
import pymysql
from dateutil import parser
import datetime
import json
import logging
import re
import copy
import requests
import os
from bson import ObjectId
main_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_path)

from common_utils.address_parser import AddressParser



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

class KbpPipeline(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_expert = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_expert")] 
        self.res_kb_process_expert = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_expert")] # 清洗库
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.kb_expert = self.arango_con[self.config.get("arango","db")][self.config.get("arango","kb_expert")]
        self.total = 0 # 
        self.count_update = 0

    def query_data(self, process_date):
        '''定期手动执行，全量专家标签更新'''

        res = self.res_kb_process_expert.find({}).sort([("update_time",1)])
        # res = self.res_kb_process_expert.find({"_id":"5e7b8a9321e15799f9e68174"})
        self.total = res.count()
        self.process_date = datetime.datetime.today().strftime("%Y-%m-%d")
        logger.info("[{}]，清洗库查到全量专家数据记录[{}]个".format(process_date, self.total))
        return res

    def process(self, process_date):

        datas = self.query_data(process_date)
        for data in datas:
            logger.info("正在处理专家，ID=[{}]，名称=[{}]".format(data["_id"], data["name"]))
            data_id = data["_id"]
            crawl_data = self.res_kb_expert.find_one({"_id":ObjectId(data_id)})
            if data["category"] == crawl_data["category"]:
                logger.info("标签无更新，跳过")
                continue
            else:
                # 更新清洗库
                self.res_kb_process_expert.update_one({"_id":data["_id"]},{"$set":{"category": crawl_data["category"],"update_time":datetime.datetime.today()}})
                # 更新Arango
                doc = self.kb_expert[data["_id"]]
                properties = doc["properties"]
                properties["category"] = crawl_data["category"]
                doc["update_time"] = datetime.datetime.today()
                doc.save()
                logger.info("数据已更新")
                self.count_update += 1
        logger.info("日期[{}]共计专家数据[{}]条，更新专家分类标签数据[{}]条".format(self.process_date, self.total,self.count_update))

if __name__=="__main__":

    kbp = KbpPipeline()
    if len(sys.argv) > 1:
        kbp.process(sys.argv[1])
    else:
        kbp.process("yesterday")            
