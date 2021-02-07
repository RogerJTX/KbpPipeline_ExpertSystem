#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-22 20:33
# Filename     : conference_clean.py
# Description  : res_kb_conference会议信息清洗，主要是值的清洗和字段按新schema数据格式组织重新插入
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

class conferenceClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_conference = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_conference_es")] 
        self.res_kb_process_conference = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_conference_es")] # 会议清洗库
        self.count_ignore = 0
        self.count_insert = 0
        self.count_dupl = 0
        self.count_inner_dupl = 0
     

    def process_str(self, s):
        '''字符串常规处理'''
        string = ""
        if s:
            string = s.replace("（","(").replace("）",")").replace("\r","").replace("nan","")
            
        return string

    def process_name(self, s):
        '''标题中字母归一化大写'''
        if s:
            s = s.upper()
            s = self.process_str(s)
        else:
            s = ""
        return s


    def process_str_list(self, p):
        if type(p) == list:
            return p

        tmp_list = list( filter(lambda x:len(x)>0, re.split("[;,，；\n\t]",p) ) )
        res_list = list(map(self.process_str, tmp_list))    
        return res_list

    def process_date(self,datetime_str):
        '''
        日期清洗，将日期字符串格式转为 2020-02-03 12:23:34 格式存储
        '''
        if not datetime_str: # like ""
            return ""
        if type(datetime_str)== datetime.datetime: # some like datetime.datetime type
            datetime_str = str(datetime_str)[:19]
            
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

        return d

    def process_departments(self, doc):
        
        departments = []
        if "mainHolder" in doc:
            departments.extend(self.process_str_list(doc["mainHolder"]))
        if "takeHolder" in doc:
            departments.extend(self.process_str_list(doc["takeHolder"]))
        departments = list(set(map(self.process_str,departments)))
        return departments



    def insert_batch(self, batch_data):
        '''根据会议名称去重，并写入'''

        logger.info("批量数据组装完成，准备去重...")
        conference_names = []
        inner_dupl_index = []
        for index, data in enumerate(batch_data):
            if data["name"] in conference_names:
                inner_dupl_index.append(index) 
                self.count_inner_dupl += 1            
            conference_names.append(data["name"])
        
        logger.info("批量数据批内去重，组内去重[{}]条数据".format(self.count_inner_dupl))
        for i, pop_index in enumerate(inner_dupl_index):
            batch_data.pop(pop_index - i)

        logger.info("批量数据与MongoDB去重中...")
        
        dupl_datas = self.res_kb_process_conference.find({"name":{"$in":conference_names}})
        for dupl_data in dupl_datas:
            index = conference_names.index(dupl_data["name"])
            conference_names.pop(index)
            batch_data.pop(index)
            self.count_dupl += 1

        if not batch_data:
            logger.info("去重完成，无数据写入")
            return

        logger.info("去重完成，准备写入...")
        insert_res = self.res_kb_process_conference.insert_many(batch_data)
        self.count_insert += len(insert_res.inserted_ids)
        logger.info("批量数据写入完成")

    def process_contact(self, doc):
        '''
        整合ES中的联系方式
        '''

        contact = {
            "contact_person":[],
            "contact":[]
        }
        if not "contract" in doc:
            return contact
        contact_info = doc["contract"]
        if contact_info:
            infos = re.split("[\n]", contact_info)
            for info in infos:
                phone = re.findall("^1\d{10}", info)
                contact["contact"].extend(phone)
                if "电话" in info or "Tel" in info or "联系人" in info or "热线" in info or "座机" in info:
                    tel = re.findall("\d+[-]*\d+-+\d+", info)
                    contact["contact"].extend(tel)
                    for c in contact["contact"]:
                        info = info.replace(c,"")
                if "邮箱" in info or "E-mail" in info or "E" in info:
                    email = re.findall("[\da-zA-Z]+@[a-zA-Z]+.[a-zA-Z]+", info)
                    contact["contact"].extend(email)
                if "联系人" in info:
                    info = info.replace(" ","").replace("（","(").replace("）",")")\
                                .replace("(微信同号)","").replace("(同微信)","")\
                                .replace("手机","").replace("电话","")\
                                .replace(":","").replace("：","")
                    person = info[info.index("联系人")+3:].strip()
                    person = re.sub("\d","",person)
                    contact["contact_person"].extend([person])

        return contact
            

    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于crawl_date以后的会议数据
        '''

        count = 0
        
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
        # res = self.res_kb_conference.find({'crawl_time': {'$gte': iso_date}}).sort([("crawl_time",1)])
        res = self.res_kb_conference.find({}).sort([("channelTime",1)])

        total = res.count()

        batch_data = []
        
        for doc in res:

            logging.info("正在处理会议，会议名=[{}]，ObjectId=[{}]".format(doc["content"], str(doc["_id"])))
            if not doc["content"]:
                self.count_ignore += 1
                logging.info("跳过会议，无会议名称，ObjectId=[{}]".format(str(doc["_id"])))
                count += 1
                continue
 
            clean_conference = {
                "_id": str(doc["_id"]),  
                "name": self.process_name(doc["content"]),
                "alter_names": [],
                "start_time": self.process_str(doc["channelTime"]),
                "end_time": self.process_str(doc["endTime"]),
                "address": self.process_str(doc["place"]),
                "province": self.process_str(doc["province"]),
                "city": self.process_str(doc["city"]),
                "area": "",
                "desc": self.process_str(doc["description"]) if "description" in doc else "",
                "posters": self.process_str_list(doc["picture"]) if "picture" in doc else [],
                "companys": doc["companys"] if "companys" in doc else [],
                "departments":self.process_departments(doc),
                "sponsors":[],
                "source": doc["source"] if "source" in doc else "",
                "url": doc["url"],
                "html": doc["html"] if "html" in doc else "",
                "crawl_time": doc["crawl_time"] if "crawl_time" in doc else "",
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }

            contact = self.process_contact(doc)
            clean_conference.update(contact)

            batch_data.append(clean_conference)
            count += 1

            # MongoDB批量写入
            if count % 100 == 0 or count == total:
                logger.info("正在写入前[{}]家信息".format(count))
                self.insert_batch(batch_data)
                batch_data = []

        logging.info("[{}] 会议数据清洗完毕，共找到会议[{}]条，跳过无会议号的数据[{}]条，批内去重[{}]条，已存在数据[{}]条，清洗库入库[{}]条".format(
                    crawl_date, total, self.count_ignore, self.count_inner_dupl, self.count_dupl, self.count_insert) )


if __name__ == "__main__":

    # 会议最早爬虫日期为 2019-05-24
    
	cleaner = conferenceClean()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
