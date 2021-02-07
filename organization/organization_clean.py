#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-29 10:55
# Filename     : organization_clean.py
# Description  : res_kb_organization人工导入高校机构机构信息清洗，主要是添加高校机构机构的省市区经纬度信息和type类型
#******************************************************************************

import logging
from logging.handlers import RotatingFileHandler
import requests
from pymongo import MongoClient
import datetime
import re
import configparser
import sys
from dateutil import parser
import uuid
import pymysql
import os

main_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_path)

from common_utils.address_parser import AddressParser

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class OrganizationClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_organization = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_organization")] 
        self.res_kb_process_organization = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_organization")] # 高校机构机构清洗库
        self.count_skip = 0
        self.count_insert = 0


    def process_str(self, string):
        s = ""
        if string:
            string = string.replace("（","(").replace("）",")")
            string = re.sub("[\n\t\r]","",string)
            s = string
        return s


    def query_res(self, process_date):
        '''根据新导入的更新时间清洗记录'''

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
        query = self.res_kb_organization.find({"crawl_time":{"$gte":iso_date}}).sort([("crawl_time",1)])
        self.process_date = process_date
        return query

    def process_tel(self, doc):
        '''联系方式数组化'''
        res = []
        if "tel" in doc and doc["tel"]:
            tel_str = doc["tel"].replace("（","(").replace("）",")")
            tel = re.split("[;,，、 ]",tel_str)
            res.extend(tel)
        return res

    def process(self, process_date):

        organizations = self.query_res(process_date)
        self.total = organizations.count()
        logger.info("高校机构机构库发现需处理数据[{}]条，准备清洗".format(organizations.count()))

        clean_datas = []
        count = 0

        for organization in organizations:

            logger.info("正在处理高校机构机构，高校机构机构名称=[{}]".format(organization["name"]))

            data = {
                "_id": str(organization["_id"]),
                "name": organization["name"],
                "type": str(organization["type"]),
                "industry":self.process_str(organization["industry"]),
                "address": self.process_str(organization["address"]),
                "img_url": organization["img_url"],
                "tel": self.process_tel(organization),
                "desc": self.process_str(organization["desc"]),
                "website": organization["website"],
                "email": organization["email"],
                "tags": [ self.process_str(t) for t in organization["tags"]],
                "university": self.process_str(organization["university"]), # 研究机构所在高校
                "wechat_name":self.process_str(organization["wechat_name"]),
                "wechat_num": self.process_str(organization["wechat_num"]),
                "qrcode": self.process_str(organization["qrcode"]),
                "crawl_time": organization["crawl_time"],
                "url": organization["url"],
                "html": organization["html"],
                "source": organization["source"],
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }
            
            address = organization["address"]
            address_processor = AddressParser()
            request_data = {
                "name": organization["name"],
                "address": organization["address"],
                "province": ""
            }
            address_info = address_processor.parse(request_data)
            data.update(address_info)

            exist = self.res_kb_process_organization.find_one({"name":data["name"]})
            if exist:
                logger.warning("高校机构机构信息已存在，跳过数据。高校机构机构名称=[{}]，ObjectId=[{}]".format(data["name"], data["_id"]))
                self.count_skip += 1
            else:
                clean_datas.append(data)

            count += 1

            if count % 100 == 0 or count == self.total:
                logger.info("已经处理高校机构机构信息[{}]条".format(count))
                if clean_datas:
                    insert_res = self.res_kb_process_organization.insert_many(clean_datas)
                    self.count_insert += len(insert_res.inserted_ids)
                    clean_datas = []

        logger.info("日期[{}]，高校机构机构库清洗完毕，共查到处理数据[{}]条，跳过已存在数据[{}]条，新增数据[{}]条".format(
                    self.process_date, self.total, self.count_skip, self.count_insert
        ))


if __name__ == "__main__":

    # 数据最早更新日期 2019-11-01
    
	cleaner = OrganizationClean()

	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")



                


