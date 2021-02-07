#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-13 15:34
# Filename     : patent_crawber.py
# Description  : res_kb_patent专利信息生成，目前是转移企业相关的专利信息，实际这一步是用爬虫替换
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
        # self.company_ai = self.mongo_con[self.config.get("mongo","info_db")][self.config.get("mongo","company_ai")]
        self.crawber_patent = self.mongo_con[self.config.get("mongo","crawber_db")][self.config.get("mongo","crawber_patent")]
        self.res_kb_patent = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_patent")]
        self.transfer_count = 0

    def company_from_ai(self):
        '''
        数据源切换为所有AI企业
        '''
        process_companys = set()

        query = {
            "sector_ai":{
                "$ne":[]
            }
        }

        company_res = self.company_ai.find(query)
        for company in company_res:
            process_companys.add(company["company_name"])
        
        sorted_process_companys = list(process_companys)
        sorted_process_companys.sort()

        return sorted_process_companys

    def company_from_file(self):
        '''读取候选企业'''
        with open(os.path.join(kbp_path,"necar_companys.txt"), "r") as fr:
            companys_read = fr.readlines()
        
        companys = list(map(lambda x:x.strip(), companys_read))

        return companys

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    def process_company_patent(self, company:str):

        process_res = []

        ##### 按行业迁移数据，先判断量知产业知识中心的库里是否有该企业信息，如果有，不再重复处理
        exist = self.res_kb_patent.find_one({"search_key":company})
        if not exist:
            zn_company = company.replace("(","（").replace(")","）")
            exist = self.res_kb_patent.find_one({"search_key":zn_company})

        if exist:
            return process_res
        #### 判断完毕

        patents_res = self.crawber_patent.find({"search_key":company})
        if not patents_res:
            zn_company = company.replace("(","（").replace(")","）")  # 再匹配下中文的括号格式是否有
            patents_res = self.crawber_patent.find({"search_key":zn_company})

        logger.info("企业[{}]查询到目前专利库中有[{}]个专利信息".format(company,patents_res.count()))
        for patent in patents_res:
            process_res.append(patent)

        return process_res
    
    def process(self):



        # 获取业务需求，企业名
        # process_companys = self.company_from_ai()
        process_companys = self.company_from_file()
        logger.info("企业名单获取完成，共[{}]个".format(len(process_companys)))
        # TODO 其他渠道获取企业


        # 根据推荐的AI企业进行知识构建库的新增
        for i, company in enumerate(process_companys):

            if( i%100 == 0):
                logger.info("开始处理第{}个企业".format(i)) 

            logger.info("查询推荐企业[{}]".format(company))

            company_patents = self.process_company_patent(company)

            if company_patents:
                insert_res = self.res_kb_patent.insert_many(company_patents)
                self.transfer_count += len(insert_res.inserted_ids)

        self.close_connection() 
        logger.info("企业{}个，迁移专利信息[{}]个".format(len(process_companys),self.transfer_count))

if __name__=="__main__":
    pc = PatentCrawber()
    pc.process()
