#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-15 15:34
# Filename     : crawber.py
# Description  : res_kb_product产品信息生成，目前是转移企业相关的产品信息，实际这一步是用爬虫替换
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


class ProductCrawber(object):

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.crawber_product = self.mongo_con[self.config.get("mongo","crawber_db")][self.config.get("mongo","crawber_product")]
        self.res_kb_product = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_product")]
        self.transfer_count = 0
            

    def company_from_file(self):
        '''读取候选企业'''
        with open( os.path.join(kbp_path,"necar_companys.txt"),"r") as fr:
            companys_read = fr.readlines()
        
        companys = list(map(lambda x:x.strip(), companys_read))

        return companys

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    def process_company_product(self, company:str):

        process_res = []

        # 先校验爬虫库有无该企业的产品信息
        exist = self.res_kb_product.find_one({"company_name":company})
        if not exist:
            zn_name = company.replace("(","（").replace(")","）")
            exist = self.res_kb_product.find_one({"company_name":zn_name})
        if exist:
            return None 

        # 开始迁移
        product_res = self.crawber_product.find({"company_name":company}).sort([("crawl_time",1)])
        # 检查爬虫是否用的中文括号
        if product_res.count() == 0:
            zn_name = company.replace("(","（").replace(")","）")
            product_res = self.crawber_product.find({"company_name":zn_name}).sort([("crawl_time",1)])


        logger.info("企业[{}]查询到目前产品库中有[{}]个产品信息".format(company,product_res.count()))
        for product in product_res:
            process_res.append(product)

        return process_res
    
    def process(self):


        # 获取业务需求，企业名
        process_companys = self.company_from_file()
        logger.info("企业名单获取完成，共[{}]个".format(len(process_companys)))
        no_product_company = []


        # 根据目前企业进行知识构建库的新增
        for i, company in enumerate(process_companys):

            if( i%100 == 0):
                logger.info("开始处理第{}个企业".format(i)) 

            logger.info("查询企业[{}]".format(company))

            company_products = self.process_company_product(company)

            if company_products:
                insert_res = self.res_kb_product.insert_many(company_products)
                self.transfer_count += len(insert_res.inserted_ids)
            # 返回空列表的是无数据的，返回none的是该公司数据已迁移过
            elif company_products == []:
                no_product_company.append(company)

        self.close_connection() 

        with open( os.path.join(dir_path, "necar_no_product_company.txt"), "w") as fw:
            fw.write("\n".join(no_product_company))

        logger.info("企业{}个，迁移产品信息[{}]个".format(len(process_companys),self.transfer_count))

if __name__=="__main__":
    pc = ProductCrawber()
    pc.process()
