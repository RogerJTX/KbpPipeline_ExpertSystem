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

main_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_path)

from common_utils.address_parser import AddressParser

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

class ConferenceClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_conference = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_conference")] 
        self.res_kb_process_conference = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_conference")] # 会议清洗库
        self.count_ignore = 0
        self.count_insert = 0
        self.count_dupl = 0
        self.count_inner_dupl = 0
     

    def process_str(self, s):
        '''字符串常规处理'''
        string = ""
        if s:
            string = s.replace("（","(").replace("）",")")
            string = re.sub("[\r\t\n\xa0]","",string)
            
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

        tmp_list = list( filter(lambda x:len(x)>0, re.split("[;,，；\n\t ]",p) ) )
        res_list = list(map(self.process_str, tmp_list))    
        return res_list

    def process_date(self,datetime_str):
        '''
        日期清洗，将日期字符串格式转为 2020-02-03 12:23:34 格式存储
        '''
        if not datetime_str: # like ""{}
            return ""
        if type(datetime_str)== datetime.datetime: # some like datetime.datetime type
            datetime_str = str(datetime_str)[:19]

        datetime_str = datetime_str.replace("日","")  
        tmp = datetime_str.split(" ")
        d = ""
        try:
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
                
        except Exception as e:
            logger.error("日期转化有误，日期=[{}]".format(datetime_str))
            return datetime_str

       

    def insert_batch(self, batch_data):
        '''过滤无效数据，根据会议名称去重，并写入'''

        logger.info("批量数据组装完成，准备批内去重...")
        conference_names = []
        conference_ids = []
        new_batch = []
        for index, data in enumerate(batch_data):
            if not data["name"]:
                self.count_ignore += 1
                logging.info("跳过会议，无会议名称，ObjectId=[{}]".format(data["_id"]))
                continue
            if data["name"] in conference_names:
                inner_index = conference_names.index(data["name"])
                logger.info("会议批内去重，会议(名称=[{}]，ObjectId=[{}])被去除，留存主会议(名称=[{}]，ObjectId=[{}])".format(
                            data["name"], data["_id"], data["name"], conference_ids[inner_index]))
                self.count_inner_dupl += 1 
                continue           
            conference_names.append(data["name"])
            conference_ids.append(data["_id"])
            new_batch.append(data)
        
        logger.info("批量数据批内去重，批内累计去重[{}]条数据".format(self.count_inner_dupl))

        logger.info("批量数据与MongoDB去重中...")
        
        dupl_datas = self.res_kb_process_conference.find({"name":{"$in":conference_names}})
        for dupl_data in dupl_datas:
            index = conference_names.index(dupl_data["name"])
            conference_names.pop(index)
            conference_ids.pop(index)
            new_batch.pop(index)
            self.count_dupl += 1

        if not new_batch:
            logger.info("去重完成，无数据写入")
            return

        logger.info("去重完成，准备写入...")
        insert_res = self.res_kb_process_conference.insert_many(new_batch)
        self.count_insert += len(insert_res.inserted_ids)
        logger.info("批量数据写入完成")

    def address_parse(self, address):
        '''高德地图地址解析API'''
        KEY = '80d45fcac2a813aa387481046e5bb487'
        url = 'https://restapi.amap.com/v3/geocode/geo?'
        url = url + "key=" + KEY + "&address=" + address
        result = requests.get(url)
        result = json.loads(result.content.decode('utf-8'))

        res = {
            "province":"",
            "city":"",
            "area":""
        }

        if result['info'] == 'OK':

            if len(result['geocodes']) > 0:

                content = result['geocodes'][0]
                res["province"] = content['province']
                if res["province"] in ["北京市","上海市","重庆市","天津市"]:
                    res["province"] = res["province"].strip("市")
                if content["city"]:
                    res["city"] = content['city']
                if content["district"]:
                    res["area"] = content['district']

        return res

    def process_desc(self, doc):
        '''过滤含有HTML标签的数据，质量低，过滤掉'''
        res = ""
        desc = doc["conference_details"]
        if desc and "<div" in desc:
            desc = ""
        res = self.process_str(desc)
        return res 
        

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
        res = self.res_kb_conference.find({'crawl_time': {'$gte': iso_date}}).sort([("crawl_time",1)])

        total = res.count()

        batch_data = []
        
        for doc in res:

            count += 1
            logging.info("正在处理会议，会议名=[{}]，ObjectId=[{}]".format(doc["conference_name"], str(doc["_id"])))
 
            clean_conference = {
                "_id": str(doc["_id"]),  
                "name": self.process_name(doc["conference_name"]),
                "alter_names":[],
                "start_time": self.process_date(doc["start_time"]),
                "end_time": self.process_date(doc["end_time"]),
                "address": self.process_str(doc["address"]),
                "desc": self.process_desc(doc),
                "contact_person":[],
                "contact": [],
                "posters": doc["img_url"] if doc["img_url"] else [],
                "departments":[],
                "sponsors":[],
                "companys":[],
                "source": self.process_str(doc["source"]),
                "url": doc["url"],
                "html": doc["html"],
                "crawl_time": doc["crawl_time"],
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }


            address = clean_conference["address"]
            #address_info = self.address_parse(address)

            address_processor = AddressParser()
            province = ""
            if "province" in doc:
                province = doc["province"]
            request_data = {
                "name": clean_conference["name"],
                "address": address,
                "province": province#data["province"]
            }
            address_info = address_processor.parse(request_data)
            clean_conference.update(address_info)
          
            batch_data.append(clean_conference)

            # MongoDB批量写入
            if count % 100 == 0 or count == total:
                logger.info("正在写入前[{}]家信息".format(count))
                self.insert_batch(batch_data)
                batch_data = []

        logging.info("[{}] 会议数据清洗完毕，共找到会议[{}]条，跳过无会议号的数据[{}]条，批内去重[{}]条，已存在数据[{}]条，清洗库入库[{}]条".format(
                    crawl_date, total, self.count_ignore, self.count_inner_dupl, self.count_dupl, self.count_insert) )


if __name__ == "__main__":

    # 会议最早爬虫日期为 2019-08-16
    
	cleaner = ConferenceClean()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
