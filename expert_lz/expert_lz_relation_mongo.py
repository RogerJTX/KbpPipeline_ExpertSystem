#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-10-10 17:17
# Filename     : expert_lz_relation_mongo.py
# Description  : 专家添加产业和人脉关联
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
main_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(main_path)
from expert_classifier import ExpertClassifier

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

class ExpertRelation(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.kb_expert_lz = self.mongo_con[self.config.get("mongo","kb_arango")][self.config.get("mongo","kb_expert_lz")]
        self.kb_expert_lz_patent = self.mongo_con[self.config.get("mongo","kb_arango")][self.config.get("mongo","kb_expert_patent")]
        self.kb_expert_lz_article = self.mongo_con[self.config.get("mongo","kb_arango")][self.config.get("mongo","kb_expert_article")]
        self.kb_expert_lz_project = self.mongo_con[self.config.get("mongo","kb_arango")][self.config.get("mongo","kb_expert_project")]
        self.count_update = 0
        

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
        # res = self.kb_expert_lz.find({"update_time": {"$gte": iso_date}}).sort([("_id",1)])
        res = self.kb_expert_lz.find({}, no_cursor_timeout=True).sort([("_id",1)])
        return res
    
    def process_expert_expert_relation(self, doc):
        relations = []
        
        expert_id = doc["_id"]
        # 论文合作作者
        articles = self.kb_expert_lz_article.find({"relations.object_id":expert_id})
        for article in articles:
            author_relation = list(filter(lambda x:x["object_type"] == "expert", article["relations"]))
            for rel in author_relation:
                if rel["object_id"] == doc["_id"]:
                    continue 
                relation = {
                    "relation_type":"concept_relation/100021",
                    "object_type":"expert",
                    "object_name": rel["object_name"],
                    "object_id": rel["object_id"]
                }
                if relation not in relations:
                    relations.append(relation)
                    
        # 专利合作作者
        patents = self.kb_expert_lz_patent.find({"relations.object_id":expert_id})
        for patent in patents:
            author_relation = list(filter(lambda x:x["object_type"] == "expert", patent["relations"]))
            for rel in author_relation:
                if rel["object_id"] == doc["_id"]:
                    continue 
                relation = {
                    "relation_type":"concept_relation/100021",
                    "object_type":"expert",
                    "object_name": rel["object_name"],
                    "object_id": rel["object_id"]
                }
                if relation not in relations:
                    relations.append(relation)
                    
        # 科研课题合作作者
        projects = self.kb_expert_lz_project.find({"relations.object_id":expert_id})
        for project in projects:
            author_relation = list(filter(lambda x:x["object_type"] == "expert", project["relations"]))
            for rel in author_relation:
                if rel["object_id"] == doc["_id"]:
                    continue 
                relation = {
                    "relation_type":"concept_relation/100021",
                    "object_type":"expert",
                    "object_name": rel["object_name"],
                    "object_id": rel["object_id"]
                }
                if relation not in relations:
                    relations.append(relation)
                    
        return relations
        
    
    def process_relations(self, doc):
        relations = []  
        
        # 专家与产业产品关联     
        # expert_product_relation = self.expert_product_classifer.score_one(doc)
        expert_product_relation = self.expert_product_classifer.process_one(doc)
        if expert_product_relation:
            relations.extend(expert_product_relation)
        
        # 专家人脉关系   
        expert_expert_relation = self.process_expert_expert_relation(doc)
        if expert_expert_relation:
            relations.extend(expert_expert_relation)
             
        return relations
    

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()
         

    def process(self, crawl_date):
        '''
        爬虫时间大于等于crawl_date以后的专家数据添加关系
        '''

        count = 0
        docs = self.query_daily_data(crawl_date)
        total = docs.count()
        logging.info("日期[{}]查到待处理数据[{}]条".format(self.process_date, total))
        batch_data = []
        self.expert_product_classifer = ExpertClassifier()
        
        for doc in tqdm(docs):
            
            update = {}
            relations = self.process_relations(doc)
            if relations:
                update["relations"] = relations
                update["update_time"] = datetime.datetime.today()
                self.kb_expert_lz.update_one({"_id":doc["_id"]},{"$set":update})
                self.count_update += 1


        logging.info("[{}]专家数据关联完毕，共找到记录[{}]条，添加关系记录[{}]条".format(
                    self.process_date, total, self.count_update) )
        self.close_connection()


if __name__ == "__main__":

    # 最早kbp日期为 2020-10-10
    
	rel = ExpertRelation()
	if len(sys.argv) > 1:
		rel.process(sys.argv[1])
	else:
		rel.process("yesterday")
