# 论文数据添加关联
#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-10-02 22:57
# Filename     : expert_article_relation_mongo.py
# Description  : 论文数据添加关联；专家关联
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

class ArticleRelation(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.kb_expert_article = self.mongo_con[self.config.get("mongo","kb_arango")][self.config.get("mongo","kb_expert_article")] 
        self.kb_expert = self.mongo_con[self.config.get("mongo","kb_arango")][self.config.get("mongo","kb_expert_lz")]
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
        res = self.kb_expert_article.find({"update_time": {"$gte": iso_date}}, no_cursor_timeout=True).sort([("_id",1)])
        return res
    
    def process_relations(self, doc):
        relations = []
        
        # 论文与专家关联
        article_expert_relation = self.process_expert_relation(doc)
        if article_expert_relation:
            relations.extend(article_expert_relation)
            
        return relations
    
    def process_expert_relation(self, doc):
        relations = []
        authors = doc["properties"]["authors"]
        for a in authors:
            if ("kId" in a) and a["kId"]:
                expert = self.kb_expert.find_one({"properties.kId":{"$in":[a["kId"]]}})
                if expert:
                    rel = {
                        "relation_type":"concept_relation/100013",
                        "object_name":a["name"],
                        "object_type": "expert",
                        "object_id": expert["_id"]
                    }
                    if rel not in relations:
                        relations.append(rel)
        return relations
        


    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

            

    def process(self, crawl_date):
        '''
        爬虫时间大于等于crawl_date以后的论文数据关系添加
        '''

        count = 0
        docs = self.query_daily_data(crawl_date)
        total = docs.count()
        logging.info("日期[{}]查到待处理数据[{}]条".format(self.process_date, total))
        batch_data = []
        
        for doc in tqdm(docs):
            
            update = {}
            relations = self.process_relations(doc)
            if relations:
                update["relations"] = relations
                update["update_time"] = datetime.datetime.today()
                self.kb_expert_article.update_one({"_id":doc["_id"]},{"$set":update})
                self.count_update += 1


        logging.info("[{}]论文数据关联完毕，共找到论文[{}]条，添加关系记录[{}]条".format(
                    self.process_date, total, self.count_update) )
        self.close_connection()


if __name__ == "__main__":

    # 论文最早爬虫日期为 2020-09-14
    
	cleaner = ArticleRelation()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
