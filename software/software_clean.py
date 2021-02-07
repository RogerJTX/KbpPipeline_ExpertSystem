#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-13 16:09
# Filename     : software_clean.py
# Description  : res_kb_software企业软著信息清洗
# 1 软著关键字段：登记号，过滤掉无登记号的数据；
# 2 根据登记号进行去重  
# 3 日期和字符串的格式化；
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

class SoftwareClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_software = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_software")] 
        self.res_kb_process_software = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_software")] # 软著清洗库
        self.count_ignore = 0
        self.count_insert = 0
        self.count_dupl = 0
        self.count_inner_dupl = 0
     

    def process_str(self, s):
        '''字符串常规处理'''
        string = ""
        if s == "-":
            s = ""
        if s:
            string = s.replace("（","(").replace("）",")").replace("\r","")
        return string         

    def process_date(self,datetime_str):
        '''
        日期清洗，将日期字符串格式转为ISODate格式存储
        '''
        if not datetime_str: # like ""
            return None
        if type(datetime_str) == datetime.datetime: # 软著爬取时间部分是字符串，部分是ISODate格式
            return datetime_str
            
        tmp = datetime_str.split(" ")
        d = ""
        if len(tmp) == 1:
            tmp1 = re.split("[-./]",tmp[0])

            if len(tmp1) == 3: # like 2020-03-25 or 2020/03/27
                if "-" in tmp[0]:
                    d = datetime.datetime.strptime(tmp[0], "%Y-%m-%d")
                elif "." in tmp[0]:
                    d = datetime.datetime.strptime(tmp[0], "%Y.%m.%d")
                elif "/" in tmp[0]:
                    d = datetime.datetime.strptime(tmp[0], "%Y/%m/%d")

            elif len(tmp1) == 4: # like 2020-03-25-10:3
                d = datetime.datetime.strptime('-'.join(tmp1[:3]) + ' ' + tmp1[3], "%Y-%m-%d %H:%M")
            else:
                d = datetime_str
                
        elif len(tmp) == 2:
            
            if len(re.split("[-/]",tmp[0])) == 3:
                d1 = tmp[0].replace("/","-")
            else:
                d1 = tmp[0]
            if len(tmp[1].split(':')) == 2:
                d = datetime.datetime.strptime(d1 + ' ' + tmp[1], "%Y-%m-%d %H:%M")
            elif len(tmp[1].split(':')) == 3:
                d = datetime.datetime.strptime(d1 + ' ' + tmp[1], "%Y-%m-%d %H:%M:%S")
                
        else:
            d = datetime_str
        if type(d) == datetime.datetime:
            return d
        return parser.parse(d) 

    def insert_batch(self, batch_data):
        '''根据软著注册号去重，并写入'''

        logger.info("批量数据组装完成，准备去重...")
        reg_numbers = []
        new_batch = []
        for data in batch_data:
            if (not data["registration_number"]):
                self.count_ignore += 1
                logging.info("跳过软著，缺少有效软著注册号。软著名=[{}]，ObjectId=[{}]".format(data["name"], data["_id"]))
                continue
            if data["registration_number"] in reg_numbers:
                self.count_inner_dupl += 1
            else:
                reg_numbers.append(data["registration_number"])
                new_batch.append(data)
        
        logger.info("批量数据批内去重，组内累计共去重[{}]条数据".format(self.count_inner_dupl))

        logger.info("批量数据与MongoDB去重中...")
        
        dupl_datas = self.res_kb_process_software.find({"registration_number":{"$in":reg_numbers}})
        for dupl_data in dupl_datas:
            index = reg_numbers.index(dupl_data["registration_number"])
            reg_numbers.pop(index)
            new_batch.pop(index)
            self.count_dupl += 1

        if not new_batch:
            logger.info("去重完成，无数据写入")
            return

        logger.info("去重完成，准备写入...")
        insert_res = self.res_kb_process_software.insert_many(new_batch)
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
            raise Exception("无效参数")
        
        iso_date_str = crawl_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        res = self.res_kb_software.find({'crawl_time': {'$gte': iso_date}}).sort([("crawl_time",1)])
        # 剩余数据全量导入
        # res = self.res_kb_software.find({'create_time': {'$gte': iso_date}}).sort([("crawl_time",1)])

        return res

    def company_from_file(self):
        '''读取候选企业'''
        with open(os.path.join(kbp_path,"necar_companys.txt"), "r") as fr:
            companys_read = fr.readlines()
        
        companys = list(map(lambda x:x.strip(), companys_read))

        return companys


            

    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于crawl_date以后的软著数据
        '''

        count = 0
        res = self.query_daily_data(crawl_date)
        total = res.count()

        # 手动执行步骤
        # res = self.company_from_file()
        # total = len(res)


        batch_data = []
        
        for doc in res:
        
        #手动执行步骤
        # for name in res:
            
        #     exist = self.res_kb_process_software.find_one({"company_name": name})
        #     if exist:
        #         continue

            
        #     docs = self.res_kb_software.find({"search_key":name})
        #     if not docs:
        #         zn_name = name.replace("(","（").replace(")","）")
        #         docs = self.res_kb_software.find({"search_key":zn_name})

        #     for doc in docs:


            logging.info("正在处理软著，软著名=[{}]，ObjectId=[{}]".format(doc["software_name"], str(doc["_id"])))

            clean_software = {
                "_id": str(doc["_id"]), 
                "registration_number": doc["registration_number"], 
                "classification_number": doc["classification_number"],
                "name": self.process_str(doc["software_name"]),
                "short_name":self.process_str(doc["short_name"]),
                "approval_date": self.process_date(doc["approval_date"]),
                "version": doc["version"].replace(" ","") if doc["version"] else "",
                "company_name": self.process_str(doc["company_name"]),
                "source": self.process_str(doc["source"]),
                "url": doc["url"],
                "html": doc["html"],
                "crawl_time": self.process_date(doc["crawl_time"]),
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }
        
            batch_data.append(clean_software)
            count += 1

            # MongoDB批量写入
            if count % 100 == 0 or count == total:
                logger.info("正在写入前[{}]家企业信息".format(count))
                self.insert_batch(batch_data)
                batch_data = []

        logging.info("[{}] 企业软著数据清洗完毕，共找到软著[{}]条，跳过无软著号的数据[{}]条，批内去重[{}]条，已存在数据[{}]条，清洗库入库[{}]条".format(
                    crawl_date, total, self.count_ignore, self.count_inner_dupl, self.count_dupl, self.count_insert) )


if __name__ == "__main__":

    # 软著最早爬虫日期为 2019-06-25
    
	cleaner = SoftwareClean()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
