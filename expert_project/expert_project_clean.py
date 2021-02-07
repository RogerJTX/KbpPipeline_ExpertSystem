# 从专家成果中清洗出专家科研项目数据
#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-09-30 17:09
# Filename     : expert_project_clean.py
# Description  : 专家成果科研项目信息清洗
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

class ProjectClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_expert_project = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_expert_resource")] 
        self.res_kb_process_expert_project = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_expert_project")] # 科研项目清洗库
        self.count_ignore = 0
        self.count_insert = 0
        self.count_dupl = 0
        self.count_inner_dupl = 0
     

    def process_str(self, s):
        '''字符串常规处理'''
        string = ""
        if s:
            string = s.replace("（","(").replace("）",")").replace("\r","").replace("<br>","")
        return string


    def process_str_list(self, p):

        tmp_list = list( filter(lambda x:len(x)>0, re.split("[;,，；\n\t ]",p) ) )
        res_list = list(map(self.process_str, tmp_list))    
        return res_list
    
    
    def process_list_str(self, data:list):
        new_data = []
        if data:
            new_data = [self.process_str(d) for d in data]
        return new_data
    
    def process_authors(self, data, doc):
        '''根据发明人和申请机构，匹配发明人所属机构'''
        authors = []
        inventors = data["inventors"]
        applicants = data["applicants"]
        
        # 申请人或申请机构无值，不进行处理
        if inventors == [] or applicants == []:
            return authors
        
        # 一个申请机构，发明人均属于该机构
        if len(applicants) == 1:
            for a in inventors:
                author = {
                    "name": self.process_str(a),
                    "research_institution": self.process_str(applicants[0]),
                    "kId":""
                }
                authors.append(author)
          
        # 发明人和申请机构数量一样，则一一对应        
        if len(applicants)>1 and len(inventors) == len(applicants):
            for i, a in enumerate(inventors):
                author = {
                    "name": self.process_str(a), 
                    "research_institution": self.process_str(applicants[i]),
                    "kId":""
                }
                authors.append(author)
        
        # 发明人和申请机构均存在且数量不一致，只对应第一发明人和第一申请机构       
        if len(applicants) > 1 and len(inventors) != len(applicants):
            author = {
                "name": self.process_str(inventors[0]),
                "research_institution": self.process_str(applicants[0]),
                "kId":""
            }
            authors.append(author)
         
        # kId填充   
        for author in authors:
            if author["name"] == doc["expert_name"].strip():
                author["kId"] = doc["kId"].strip()
                break 
            
        if "candidateExperts" in doc and doc["candidateExperts"]:
            for c in doc["candidateExperts"]:
                name, kId = c.split("|")
                for author in authors:
                    if author["name"] == name:
                        author["kId"] = str(kId).upper()
                        break 
             
        return authors
                

    def insert_batch(self, batch_data):
        '''根据 resource_id 去重，并写入'''

        logger.info("批量数据组装完成，准备去重...")
        public_codes = []
        new_batch = []

        for data in batch_data:

            public_code = data["resource_id"]
            
            # 批内去重
            if public_code in public_codes:                  
                self.count_inner_dupl += 1
                continue
             
            new_batch.append(data)
            public_codes.append(public_code)

        logger.info("批量数据批内去重，组内去重[{}]条数据".format(self.count_inner_dupl))

        logger.info("批量数据与MongoDB去重中...")
        
        dupl_datas = self.res_kb_process_expert_project.find({"resource_id":{"$in":public_codes}})
        for dupl_data in dupl_datas:
            index = public_codes.index(dupl_data["resource_id"])
            public_codes.pop(index)
            new_batch.pop(index)
            self.count_dupl += 1

        if not new_batch:
            logger.info("去重完成，无数据写入")
            return

        logger.info("去重完成，准备写入...")
        insert_res = self.res_kb_process_expert_project.insert_many(new_batch)
        self.count_insert += len(insert_res.inserted_ids)
        logger.info("批量数据写入完成")



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
        res = self.res_kb_expert_project.find({"resourceCode": {"$in": ["A", "O"]}, "crawl_time": {"$gte": iso_date}}).sort([("_id",1)])
        return res


    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

            

    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于crawl_date以后的科研项目数据
        '''

        count = 0
        docs = self.query_daily_data(crawl_date)
        total = docs.count()
        logging.info("日期[{}]查到待处理数据[{}]条".format(self.process_date, total))
        batch_data = []
        
        for doc in tqdm(docs):
            print(doc["_id"])

            clean_project = {
                "_id": str(doc["_id"]), 
                "resource_id": doc["resourceId"].strip() if doc["resourceId"] else "",
                "title": self.process_str(doc["title"]),
                "keywords": self.process_list_str(doc["keyword"]),
                "subject": self.process_list_str(doc["subject"]),
                "resource_code": self.process_str(doc["resourceCode"]),
                "year": self.process_str(doc["year"]),
                "abstract": self.process_str(doc["abstract"]),
                "language": self.process_str(doc["languageStandard"]),
                "inventors": self.process_list_str(doc["creatorStandard"]),
                "applicants": self.process_list_str(doc["instituteStandard"]),
                "authors":[],
                "crawl_time": doc["crawl_time"],
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }
            
            authors = self.process_authors(clean_project, doc)
            clean_project.update({"authors": authors})

            batch_data.append(clean_project)
            count += 1

            # MongoDB批量写入
            if count % 100 == 0 or count == total:
                logger.info("正在写入前[{}]条科研项目信息".format(count))
                self.insert_batch(batch_data)
                batch_data = []

        logging.info("[{}]科研项目数据清洗完毕，共找到科研项目[{}]条，跳过无科研项目号的数据[{}]条，批内去重[{}]条，已存在数据[{}]条，清洗库入库[{}]条".format(
                    self.process_date, total, self.count_ignore, self.count_inner_dupl, self.count_dupl, self.count_insert) )
        self.close_connection()


if __name__ == "__main__":

    # 科研项目最早爬虫日期为 2020-09-14
    
	cleaner = ProjectClean()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
