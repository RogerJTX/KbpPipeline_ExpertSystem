#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-05-13 19:49
# Filename     : invest_institution_crawber.py
# Description  : 专业投资机构的工商信息迁移
#******************************************************************************
from urllib.request import urlopen,quote
import json
import logging
import requests
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import datetime
import re
import configparser
import sys
from dateutil import parser
import uuid
import os
from logging.handlers import RotatingFileHandler
import copy

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

def set_log():
    logging.basicConfig(level=logging.INFO) 
    file_log_handler = RotatingFileHandler(os.path.join(dir_path,"invest_institution_log.txt"), maxBytes=1024 * 1024 * 300, backupCount=10)
    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)s - %(levelname)s - %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)

set_log()
logger = logging.getLogger(__name__)

class InvestInstitutionTransfer(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_company = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_company")] 
        self.res_kb_invest_institution = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_invest_institution")] 



    def query_daily_data(self, crawl_date):
        if crawl_date == "yesterday":
            crawl_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif crawl_date == "today":
            crawl_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(crawl_date.split("-")) == 3:
            crawl_date = crawl_date
            
        else:
            raise Exception("无效参数")
        
        iso_date_str = crawl_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        res = self.res_kb_company.find({"tag":"投资机构","crawl_time": {"$gte": iso_date}},no_cursor_timeout=True).sort([("_id",1)])
        return res


    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()
     

    def process(self, crawl_date):
        '''
        迁移投资机构工商信息
        '''

        count = 0
        res = self.query_daily_data(crawl_date)
        total = res.count()
        logger.info("企业采集库本次查询投资机构数据[{}]条".format(total))
        
        for doc in res:
            try:
                self.res_kb_invest_institution.insert_one(doc)
                count += 1
            except DuplicateKeyError as e:
                logger.warn("重复数据，企业名称=[]，ObjectId=[]".format(doc["company_name"], str(doc["_id"])),e)
                
        logger.info("迁移完成，投资机构新增数据[{}]条".format(count))
        self.close_connection()


if __name__ == "__main__":

    # 商标最早爬虫日期为 2019-07-03
    
	transfer = InvestInstitutionTransfer()
	if len(sys.argv) > 1:
		transfer.process(sys.argv[1])
	else:
		transfer.process("yesterday")
