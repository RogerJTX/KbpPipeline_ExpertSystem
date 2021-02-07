#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-08-19 19:47
# Last modified: 2020-08-19 19:47
# Filename     : leader_kbp.py
# Description  : 高管信息从构建库转移到arango
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
from tqdm import tqdm 


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
        self.res_kb_process_leader = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_leader")]
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.kb_leader = self.arango_con[self.config.get("arango","db")][self.config.get("arango","kb_leader")]
        self.count_graph_insert = 0 # arango新增数据数量
        self.count_graph_update = 0
            

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    def insert_graph_kb(self, kf):
        
        try:
            doc = self.kb_leader.createDocument(kf)
            doc.save()
            logger.info("Arango高管库新增完成，高管名=[{}]".format(kf["name"]))
            self.count_graph_insert += 1

        except Exception as e:
            logger.error("新增高管信息至arango失败，高管名=[{}]，信息=[{}]".format(kf["name"], kf), e) 


    def process_properties(self, _property):

        _property["crawl_time"] = str(_property["crawl_time"])
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
        res = self.res_kb_process_leader.find(mg_cmd_str).sort([("_id",1)])
        # 剩余全量数据迁移Arango
        # res = self.res_kb_process_leader.find({'create_time': {'$gte': iso_date}}).sort([("crawl_time",1)])
        self.total = res.count()
        self.process_date = process_date
        self.end_process_date = end_date
        logger.info("[{}]-[{}]，清洗库查到待处理数据[{}]个".format(self.process_date, self.end_process_date, self.total))
        return res


    def process(self, date_str, end_date=""):

        leaders = self.query_process(date_str, end_date)
        count = 0
        batch_data = []

        for leader in tqdm(leaders):
            count += 1
            
            # 待插入数据
            properties = self.process_properties(leader)
            kf = {
                "_key":leader["_id"],
                "name":leader["name"],
                "properties":properties,
                "tags":[],
                "relations":[],
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today(),
            }
            
            exist = self.kb_leader.fetchFirstExample( {"_key":leader["_id"]})
            
            # 新增数据
            if not exist:                              
                batch_data.append(kf)
                if batch_data and len(batch_data)%100==0:
                    self.kb_leader.bulkSave(batch_data)
                    logger.info("前[{}]条记录处理完成，ID RANGE = [{} - {}]".format(count, batch_data[0]["_key"],batch_data[-1]["_key"]))
                    self.count_graph_insert += len(batch_data)
                    batch_data = []
            else:
                exist = exist[0] # fetchFirstExample返回的是指针       
                # 更新数据    
                if exist["properties"] != kf["properties"]:
                    exist_doc = self.kb_leader[exist["_key"]]
                    exist_doc["properties"] = kf["properties"]
                    exist_doc["update_time"] = datetime.datetime.today()
                    exist_doc.save()                
                    logger.info("高管记录更新，ID=[{}]".format(exist["_key"])) 
                    self.count_graph_update += 1
        
        # 剩余数据写入       
        if batch_data:
            self.kb_leader.bulkSave(batch_data)
            logger.info("前[{}]条记录处理完成，ID RANGE = [{} - {}]".format(count, batch_data[0]["_key"],batch_data[-1]["_key"]))
            self.count_graph_insert += len(batch_data)
            batch_data = []
            
        self.close_connection() 
        logger.info("日期[{}]共找到清洗库高管数据[{}]条，arango高管库新增数据{}条，更新数据[{}]条".format(
            self.process_date, self.total, self.count_graph_insert, self.count_graph_update))

if __name__=="__main__":

    # 高管最早爬虫日期为 2020-07-25

    kbp = KbpPipeline()
    if len(sys.argv) > 1:
        if len(sys.argv) ==3:
            kbp.process(sys.argv[1],sys.argv[2])
        else:
            kbp.process(sys.argv[1])
    else:
        kbp.process("yesterday")
