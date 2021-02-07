#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-10-10 17:17
# Filename     : expert_lz_relation_organization.py
# Description  : 根据专家成果提取专家最近的任职机构
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
        res = self.kb_expert_lz.find({"update_time": {"$gte": iso_date},"properties.org":{"$exists":False}}, no_cursor_timeout=True).sort([("_id",1)])
        # res = self.kb_expert_lz.find({"_id":{"$gte":"5ee35855bccdeda414e32d8b"}}, no_cursor_timeout=True).sort([("_id",1)])
        return res
        
    
    def process_organization(self, doc):
        org = ""
        org_year = []
        expert_id = doc["_id"]
        expert_name = doc["name"]
        print(expert_id, expert_name)
        # 论文
        articles = self.kb_expert_lz_article.find({"relations.object_id":expert_id})
        for article in articles:
            year = article["properties"]["year"]
            item = list(filter(lambda x:x["name"]==expert_name, article["properties"]["authors"]))
            if not item:
                continue 
            item = item[0]
            item_org = ""
            if "research_institution" in item and item["research_institution"]:
                item_org = item["research_institution"]
            if year and item_org and ([item_org, year] not in org_year):
                org_year.append([item_org, year])
                
        # 专利
        patents = self.kb_expert_lz_patent.find({"relations.object_id":expert_id})
        for patent in patents:
            year = patent["properties"]["year"]
            item = list(filter(lambda x:x["name"]==expert_name, patent["properties"]["authors"]))
            if not item:
                continue
            item = item[0]
            item_org = ""
            if "research_institution" in item and item["research_institution"]:
                item_org = item["research_institution"]
            if year and item_org and ([item_org, year] not in org_year):
                org_year.append([item_org, year])
                
        # 科研课题
        projects = self.kb_expert_lz_project.find({"relations.object_id":expert_id})
        for project in projects:
            year = project["properties"]["year"]
            item = list(filter(lambda x:x["name"]==expert_name, project["properties"]["authors"]))
            if not item:
                continue
            item = item[0]
            item_org = ""
            if "research_institution" in item and item["research_institution"]:
                item_org = item["research_institution"]
            if year and item_org and ([item_org, year] not in org_year):
                org_year.append([item_org, year])       
        
        # 排序，提取最近的机构名称
        if org_year:
            org_year.sort(key=lambda x:x[1])
            org = org_year[-1][0]
        else:
            org = doc["properties"]["organizations"][0]
        return org 
        
        
    

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
        
        for doc in tqdm(docs):
            
            update = {}
            latest_organization = self.process_organization(doc)
            if latest_organization:
                self.kb_expert_lz.update_one({"_id":doc["_id"]},
                                             {"$set":{"properties.org":latest_organization,
                                                      "update_time":datetime.datetime.today()}
                                              })
                self.count_update += 1

        logging.info("[{}]专家数据所在机构更新，共找到记录[{}]条，添加记录[{}]条".format(
                    self.process_date, total, self.count_update) )
        self.close_connection()


if __name__ == "__main__":

    # 最早kbp日期为 2020-10-10
    
	rel = ExpertRelation()
	if len(sys.argv) > 1:
		rel.process(sys.argv[1])
	else:
		rel.process("yesterday")
