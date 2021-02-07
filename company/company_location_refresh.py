#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-05-22 15:05
# Last modified: 2020-05-22 15:18
# Filename     : company_location_refresh.py
# Description  : 手动执行脚本： 处理经纬度信息没有添加上的数据
# 1 重新过一遍百度接口和地址处理函数
#******************************************************************************
import configparser
import sys
from pymongo import MongoClient
from pymongo import errors
from pyArango.connection import Connection as ArangoConnection
import pymysql
from dateutil import parser
import datetime
import json
import logging
import re
import copy
import requests
import os
main_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_path)

from common_utils.address_parser import AddressParser



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

class KbpPipeline(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_process_company = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_company")] # 清洗库
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.kb_company = self.arango_con[self.config.get("arango","db")][self.config.get("arango","kb_company")]
        self.total = 0 # 
        self.count_update = 0

    def process_str(self, s):
        '''字符串常规处理'''
        string = ""
        if s:
            string = s.replace("（","(").replace("）",")")
            string = re.sub("[\r\t\n\xa0]","",string)
            
        return string

    def query_data(self, process_date):
        '''定期手动执行，检查经纬度数据情况'''
        if process_date == "yesterday":
            process_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif process_date == "today":
            process_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(process_date.split("-")) == 3:
            process_date = process_date
            
        else:
            raise Exception("无效参数")
        
        iso_date_str = process_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        res = self.res_kb_process_company.find({'longitude':'','create_time': {'$gte': iso_date}}).sort([("crawl_time",1)])

        self.total = res.count()
        self.process_date = process_date
        logger.info("[{}]，清洗库查到无经纬度待处理数据[{}]个".format(process_date, self.total))
        return res

    def process(self, process_date):

        datas = self.query_data(process_date)
        for data in datas:
            address = self.process_str(data["address"])
            province = data["province"]
            address_processor = AddressParser()
            request_data = {
                "name": data["name"],
                "address": data["address"],
                "province": data["province"]
            }
            address_info = address_processor.parse(request_data)
            if address_info["longitude"]:
                address_info.update({"update_time":datetime.datetime.today()})
                # 更新清洗库
                self.res_kb_process_company.update_one({"_id":data["_id"]},{"$set":address_info})
                # 更新Arango
                doc = self.kb_company[data["_id"]]
                properties = doc["properties"]
                properties.update(address_info)
                doc["properties"] = properties
                doc["update_time"] = datetime.datetime.today()
                doc.save()
                self.count_update += 1
        logger.info("日期[{}]共发现无经纬度的数据[{}]条，更新添加了经纬度数据[{}]条".format(self.process_date, self.total,self.count_update))

if __name__=="__main__":

    kbp = KbpPipeline()
    if len(sys.argv) > 1:
        kbp.process(sys.argv[1])
    else:
        kbp.process("yesterday")            
