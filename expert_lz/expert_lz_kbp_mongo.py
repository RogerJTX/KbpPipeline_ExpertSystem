#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-10-03 22:24
# Filename     : expert_article_kbp_mongo.py
# Description  : 专家论文实体化，并添加属性完整度
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

class ExpertKbp(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.kb_expert_lz = self.mongo_con[self.config.get("mongo","kb_arango")][self.config.get("mongo","kb_expert_lz")] 
        self.res_kb_process_expert_lz = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_expert_lz")] # 专家清洗库
        self.count_ignore = 0
        self.count_insert = 0
        self.count_dupl = 0
        self.count_inner_dupl = 0
        
        
    def process_properties(self, doc):
        properties = copy.deepcopy(doc)
        properties.pop("_id")
        
        # 添加属性完整度
        main_props = ["name","resume","final_edu_degree","country","professional_title",
              "gender", "birthday","subject","tel","email","honors",
              "experiences"]
        props_num = len(main_props) 
        has_count = 0                  
        for prop in main_props:
            if properties[prop]:
                has_count += 1
        property_score = round(has_count/props_num,2)
        properties["prop_score"] = property_score
        
        return properties 



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
        res = self.res_kb_process_expert_lz.find({"update_time": {"$gte": iso_date}}).sort([("_id",1)])
        return res


    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

            

    def process(self, crawl_date):

        count = 0
        docs = self.query_daily_data(crawl_date)
        total = docs.count()
        logging.info("日期[{}]查到待处理数据[{}]条".format(self.process_date, total))
        batch_data = []
        
        for doc in tqdm(docs):
            
            exist = self.kb_expert_lz.find_one({"_id" : doc["_id"]})
            cur_properties = self.process_properties(doc)
            
            # 重跑时重复数据跳过
            if exist and exist["properties"] == cur_properties:
                continue 
            
            # 存在数据更新属性
            if exist and exist["properties"] != cur_properties:          
                update = {
                    "properties": cur_properties,
                    "update_time": datetime.datetime.today()
                }
                self.kb_expert_lz.update_one({"_id":exist["_id"]}, {"$set":update})
                continue

            # 新增实体
            kf = {
                "_id": doc["_id"],
                "name": doc["name"],
                "properties": cur_properties,
                "tags":[],
                "relations":[],
                "create_time": datetime.datetime.today(),
                "update_time":datetime.datetime.today()
            }

            batch_data.append(kf)
            count += 1

            # MongoDB批量写入
            if batch_data and len(batch_data)%100 == 0:
                logger.info("正在写入前[{}]条专家信息，专家ID RANGE=[{} - {}]".format(count, batch_data[0]["_id"], batch_data[-1]["_id"]))
                insert_res = self.kb_expert_lz.insert_many(batch_data)
                self.count_insert += len(insert_res.inserted_ids)
                batch_data = []
                
        if batch_data:
            logger.info("正在写入前[{}]条专家信息".format(count))
            insert_res = self.kb_expert_lz.insert_many(batch_data)
            self.count_insert += len(insert_res.inserted_ids)
            batch_data = []
            

        logging.info("[{}]专家数据实体化完毕，共找到[{}]条，入库[{}]条".format(
                    self.process_date, total, self.count_insert) )
        self.close_connection()


if __name__ == "__main__":

    # 最早爬虫日期为 2020-09-14
    
	kbp = ExpertKbp()
	if len(sys.argv) > 1:
		kbp.process(sys.argv[1])
	else:
		kbp.process("yesterday")
