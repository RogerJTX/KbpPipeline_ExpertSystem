#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-15 15:34
# Filename     : crawber.py
# Description  : res_kb_software软著信息生成，目前是转移企业相关的软著信息，实际这一步是用爬虫替换
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

class SoftwareCrawber(object):

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.crawber_software = self.mongo_con[self.config.get("mongo","crawber_db")][self.config.get("mongo","crawber_software")]
        self.res_kb_software = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_software")]
        self.transfer_count = 0
            

    def company_from_ai(self):
        '''读取候选企业'''
        with open("../ai_companys.txt","r") as fr:
            companys_read = fr.readlines()
        
        companys = list(map(lambda x:x.strip(), companys_read))

        return companys

    def company_from_file(self):
        '''读取候选企业'''
        with open(os.path.join(kbp_path,"necar_companys.txt"), "r") as fr:
            companys_read = fr.readlines()
        
        companys = list(map(lambda x:x.strip(), companys_read))

        return companys

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    def process_company_software(self, company:str):

        process_res = []

        exist = self.res_kb_software.find_one({"company_name":company})
        if exist:
            return process_res

        
        software_res = self.crawber_software.find({"company_name":company})
        if not software_res:
            # 这里需要匹配括号的中英文两种形式，因为爬虫的公司名可能是中文也可能是英文
            company = company.replace("(","（").replace(")","）")
            software_res = self.crawber_software.find({"company_name":company})


        logger.info("企业[{}]查询到目前软著库中有[{}]个软著信息".format(company,software_res.count()))
        for software in software_res:
            process_res.append(software)

        return process_res
    
    def process(self):



        # 获取业务需求，企业名
        # process_companys = self.company_from_ai()
        process_companys = self.company_from_file()
        logger.info("企业名单获取完成，共[{}]个".format(len(process_companys)))
        no_software_company = []


        # 根据目前AI企业进行知识构建库的新增
        for i, company in enumerate(process_companys):

            if( i%100 == 0):
                logger.info("开始处理第{}个企业".format(i)) 

            logger.info("查询推荐企业[{}]".format(company))

            company_softwares = self.process_company_software(company)

            if company_softwares:
                insert_res = self.res_kb_software.insert_many(company_softwares)
                self.transfer_count += len(insert_res.inserted_ids)
            else:
                no_software_company.append(company)

        self.close_connection() 

        with open(os.path.join(dir_path,"necar_no_software_company.txt"),"w") as fw:
            fw.write("\n".join(no_software_company))

        logger.info("AI企业{}个，迁移软著信息[{}]个".format(len(process_companys),self.transfer_count))

if __name__=="__main__":
    sc = SoftwareCrawber()
    sc.process()
