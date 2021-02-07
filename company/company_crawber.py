#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-13 15:34
# Filename     : company_crawber.py
# Description  : res_kb_company企业信息爬取，目前是转移企业信息，实际这一步是用爬虫替换
#******************************************************************************

import configparser
import sys
from pymongo import MongoClient
from pymongo import errors
import pymysql
from dateutil import parser
from datetime import datetime, date, timedelta
import json
import logging
import re
import copy
import os


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

class PatentCrawber(object):

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.crawber_company = self.mongo_con[self.config.get("mongo","crawber_db")][self.config.get("mongo","crawber_company")]
        self.res_kb_company = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_company")]
        self.transfer_count = 0

    def company_from_ai(self):
        '''读取候选企业'''
        with open(os.path.join(kbp_path,"necar_companys.txt"), "r") as fr:
            companys_read = fr.readlines()
        
        companys = list(map(lambda x:x.strip(), companys_read))

        return companys

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    
    def process(self):


        # 获取业务需求，企业名
        process_companys = self.company_from_ai()
        logger.info("AI企业名单获取完成，共[{}]个".format(len(process_companys)))
        # TODO 其他渠道获取企业

        no_info_company = []


        # 根据企业名单进行知识构建库的新增
        for i, company in enumerate(process_companys):

            if( i%100 == 0):
                logger.info("开始处理第{}个企业".format(i)) 

            logger.info("查询企业[{}]基础信息".format(company))

            company_info = self.crawber_company.find_one({"company_name":company})

            if not company_info:
                company_info = self.crawber_company.find_one({"company_name": company.replace("(","（").replace(")","）")}) # base库里的信息括号格式化过

            if company_info:
                try:
                    insert_res = self.res_kb_company.insert_one(company_info)
                    self.transfer_count += 1
                except errors.DuplicateKeyError:
                    logger.warning("公司已存在，公司名=[{}]".format(company))
            else:
                no_info_company.append(company)

        with open(os.path.join(dir_path,"no_necar_company_info.txt"),"w") as fw:
            fw.write("\n".join(no_info_company))

        self.close_connection() 
        logger.info("企业{}个，迁移企业基础信息[{}]个".format(len(process_companys),self.transfer_count))

if __name__=="__main__":
    pc = PatentCrawber()
    pc.process()
