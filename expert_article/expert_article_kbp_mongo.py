#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-10-03 22:24
# Filename     : expert_article_kbp_mongo.py
# Description  : 专家论文实体化
#******************************************************************************
from urllib.request import urlopen,quote
import json
import logging
import requests
from pymongo import MongoClient
import datetime
import re
import configparser
import sys
from dateutil import parser
import uuid
import os
from logging.handlers import RotatingFileHandler
import copy
from tqdm import tqdm 

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

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

class ArticleKbp(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.kb_expert_article = self.mongo_con[self.config.get("mongo","kb_arango")][self.config.get("mongo","kb_expert_article")] 
        self.res_kb_process_expert_article = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_expert_article")] # 论文清洗库
        self.count_ignore = 0
        self.count_insert = 0
        self.count_dupl = 0
        self.count_inner_dupl = 0
        
        
    def process_properties(self, doc):
        copy_doc = copy.deepcopy(doc)
        copy_doc.pop("_id")
        return copy_doc 



    def query_daily_data(self, crawl_date):
        if crawl_date == "yesterday":
            crawl_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif crawl_date == "today":
            crawl_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(crawl_date.split("-")) == 3:
            crawl_date = crawl_date
            
        else:
            raise Exception("无效参数的日期")
        
        self.process_date = crawl_date
        iso_date_str = crawl_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        res = self.res_kb_process_expert_article.find({"update_time": {"$gte": iso_date}}).sort([("_id",1)])
        return res


    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()
       

    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于crawl_date以后的论文数据
        '''
        count = 0
        docs = self.query_daily_data(crawl_date)
        total = docs.count()
        logging.info("日期[{}]查到待处理数据[{}]条".format(self.process_date, total))
        batch_data = []
        
        for doc in tqdm(docs):
            count += 1
            # 重跑时重复数据忽略
            new_properties = self.process_properties(doc)
            exist = self.kb_expert_article.find_one({"_id":doc["_id"]})           
            if exist:
                if new_properties == exist["properties"]:
                    continue
                # 属性有更新时KB更新
                doc_update = {"properties": new_properties, "update_time":datetime.datetime.today()}
                self.kb_expert_article.update_one({"_id":doc["_id"]},{"$set":doc_update})
                continue
            
            # 新增数据
            kf = {
                "_id": doc["_id"],
                "name": doc["title"],
                "properties": new_properties,
                "tags":[],
                "relations":[],
                "create_time": datetime.datetime.today(),
                "update_time":datetime.datetime.today()
            }

            batch_data.append(kf)
            
            # MongoDB批量写入
            if batch_data and len(batch_data)%100 == 0:
                logger.info("正在写入前[{}]条论文信息".format(count))
                insert_res = self.kb_expert_article.insert_many(batch_data)
                self.count_insert += len(insert_res.inserted_ids)
                batch_data = []
                
        if batch_data:
            logger.info("正在写入前[{}]条论文信息".format(count))
            insert_res = self.kb_expert_article.insert_many(batch_data)
            self.count_insert += len(insert_res.inserted_ids)
            batch_data = []
            

        logging.info("[{}]论文数据实体化完毕，共找到论文[{}]条，入库[{}]条".format(
                    self.process_date, total, self.count_insert) )
        self.close_connection()


if __name__ == "__main__":

    # 论文最早爬虫日期为 2020-09-14
    
	cleaner = ArticleKbp()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
