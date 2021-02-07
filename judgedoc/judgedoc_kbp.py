#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-04-13 19:47
# Last modified: 2020-04-13 19:47
# Filename     : judgedoc_kbp.py
# Description  : 裁判文书信息从构建库转移到arango
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
        self.res_kb_process_judgedoc = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_judgedoc")]
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.kb_judgedoc = self.arango_con[self.config.get("arango","db")][self.config.get("arango","kb_judgedoc")]
        self.count_graph_insert = 0 # arango新增数据数量
        self.count_graph_exist = 0 # 信息在图数据库中
        self.count_graph_update = 0
            

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    def insert_graph_kb(self, kf):
        
        try:
            doc = self.kb_judgedoc.createDocument(kf)
            doc.save()
            #logger.info("Arango裁判文书库新增完成，裁判文书名=[{}]".format(kf["name"]))
            self.count_graph_insert += 1

        except Exception as e:
            logger.error("新增裁判文书信息至arango失败，裁判文书名=[{}]，信息=[{}]".format(kf["name"], kf), e) 

    def update_graph_kb(self, kf, up_key):

        try:
            doc = self.kb_judgedoc[kf['_key']]
            doc[up_key] = kf[up_key]
            doc.save()
            #logger.info("Arango企业库更新完成，企业名=[{}]，更新字段[{}]".format(kf["name"],up_key))
            self.count_graph_update += 1
            if self.count_graph_update%1000 ==0:
                logger.info("企业前[{}]条更新完成".format(self.count_graph_update))

        except Exception as e:
            logger.error("更新企业信息至arango失败，企业名=[{}]，更新信息=[{}]".format(kf["name"], kf[up_key]), e)


    def process_properties(self, _property):

        #_property.pop("crawl_time")
        _property["crawl_time"] = str(_property["crawl_time"])
        _property.pop("html")
        return _property

    def query_process(self, process_date, end_date=""):
        
        if process_date == "yesterday":
            process_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif process_date == "today":
            process_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(process_date.split("-")) == 3:
            process_date = process_date
            if end_date and 3 ==len(end_date.split("-")):
                end_process_date = end_date
                iso_end_date_str = end_process_date + 'T00:00:00+08:00'
                iso_end_date = parser.parse(iso_end_date_str)
            
        else:
            raise Exception("无效参数")
        
        iso_date_str = process_date + 'T00:00:00+08:00' 
        iso_date = parser.parse(iso_date_str)
        if not end_date:
            mg_cmd_str = {'crawl_time': {'$gte': iso_date}}
        else:
            mg_cmd_str = {'crawl_time': {'$gte': iso_date,'$lt': iso_end_date}}
        res = self.res_kb_process_judgedoc.find(mg_cmd_str).sort([("_id",1)])
        # res = self.res_kb_process_judgedoc.find({'create_time': {'$gte': iso_date}}).sort([("crawl_time",1)])
        self.total = res.count()
        self.process_date = process_date
        self.end_process_date = end_date
        logger.info("[{}]-[{}]，清洗库查到待处理数据[{}]个".format(self.process_date, self.end_process_date, self.total))
        return res



    def process(self, date_str, end_date=""):

        judgedocs = self.query_process(date_str, end_date)

        count = 0

        # arango数据库裁判文书信息处理
        for judgedoc in judgedocs:

            logger.info("处理裁判文书，编号=[{}]".format(judgedoc["case_num"]))

            # arango现存裁判文书查询
            exist = self.kb_judgedoc.fetchFirstExample( {"_key":judgedoc["_id"]}  )

            # 没有name字段，组合一个
            name = judgedoc["case_num"] + "_" + judgedoc["case_name"]

            if not exist:

                #logger.info("Arango新增裁判文书[{}]，ID=[{}]".format(name, judgedoc["_id"]))
                properties = self.process_properties(judgedoc)

                # tags 暂时没有需要加的
                kf = {
                    "_key":judgedoc["_id"],
                    "name":name,
                    "properties":properties,
                    "tags":[],
                    "relations":[],
                    "create_time": datetime.datetime.today(),  # Arango实体统一添加，方便后面审核修改数据使用
                    "update_time": datetime.datetime.today(),
                }

                self.insert_graph_kb(kf) 

            else:

                logger.info("裁判文书数据已存在图数据库中，裁判文书名=[{}]，裁判文书ID=[{}]".format(name, judgedoc["_id"])) 
                self.count_graph_exist += 1

            count += 1

        if count % 100 == 0 or count == self.total:
            logger.info("清洗库前[{}]条裁判文书数据导入裁判文书知识库处理完成".format(count))

        self.close_connection() 

        logger.info("日期[{}]共找到清洗库裁判文书数据[{}]条，arango裁判文书库新增数据{}条，arango已存在裁判文书数据[{}]条".format(
            self.process_date, self.total, self.count_graph_insert, self.count_graph_exist))

if __name__=="__main__":

    # 裁判文书最早爬虫日期为 2019-05-24

    kbp = KbpPipeline()
    if len(sys.argv) > 1:
        if len(sys.argv) ==3:
            kbp.process(sys.argv[1],sys.argv[2])
        else:
            kbp.process(sys.argv[1])
    else:
        kbp.process("yesterday")
