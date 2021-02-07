#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-10-12 10:47
# Last modified: 2020-10-12 10:47
# Filename     : expert_kbp.py
# Description  : 专家科研项目实体mongo -- arango同步
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
import bson
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
        self.kb_expert_lz_project_mongo = self.mongo_con[self.config.get("mongo","kb_arango")][self.config.get("mongo","kb_expert_project")]
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.kb_expert_lz_project_arango = self.arango_con[self.config.get("arango","db")][self.config.get("arango","kb_expert_lz_project")]
        self.count_graph_update = 0
            

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()
            

    def query_process(self, process_date):
        '''从清洗库获取数据同时看两个字段，爬虫时间和更新时间'''
        
        if process_date == "yesterday":
            process_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif process_date == "today":
            process_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(process_date.split("-")) == 3:
            process_date = process_date
            
        else:
            raise Exception("无效参数")
        new_g_table = self.kb_expert_lz_project_mongo.with_options(codec_options = bson.CodecOptions(unicode_decode_error_handler="ignore")) 
        iso_date_str = process_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        res = new_g_table.find({"update_time":{"$gte":iso_date}}).sort([("_id",1)])
        self.total = res.count()
        self.process_date = process_date
        return res



    def process(self, process_date, end_date=""):

        iso_end_date_str = ""
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
        
        new_g_table = self.kb_expert_lz_project_mongo.with_options(codec_options = bson.CodecOptions(unicode_decode_error_handler="ignore")) 
        iso_date_str = process_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        self.process_date = process_date
        self.end_process_date = end_date
        if not end_date:
            mg_cmd_str = {"update_time":{"$gte": iso_date}}
        else:
            mg_cmd_str = {"update_time":{"$gte": iso_date,"$lt": iso_end_date}}
        self.total = new_g_table.count_documents(mg_cmd_str)
        if self.total > 0:
            res_one = new_g_table.find(mg_cmd_str).sort([("_id",1)])
            last_id = res_one[0]['_id']
            sum_block = int(self.total/1000)+1
        else:
            self.total = 0
            sum_block = 0
        logger.info("[{}]-[{}]，清洗库查到待处理数据[{}]个,分为{}块处理".format(self.process_date, self.end_process_date, self.total,sum_block))

        count = 0
        doc_list = []
        first_block_f = True

        # arango科研项目信息处理
        for block in range(sum_block):
            if first_block_f:
                first_block_f = False
                mg_cmd_str["_id"] = {'$gte':last_id}
            else:
                mg_cmd_str["_id"] = {'$gt':last_id}
            projects = list(new_g_table.find(mg_cmd_str).sort([("_id",1)]).limit(1000))
            if projects:
                last_id = projects[-1]['_id']
            for project in projects:
                count += 1
                kf = project
                kf["_key"] = project["_id"]
                kf.pop("_id")
                self.count_graph_update += 1
                doc_list.append(kf)
                if self.count_graph_update %100==0:
                    self.kb_expert_lz_project_arango.bulkSave(doc_list,onDuplicate="update")
                    doc_list = []
                    logger.info("MONGO科研项目构建库前[{}]条数据同步Arango完成, 更新[{}]条".format(count,self.count_graph_update))

        if doc_list:                       
            self.kb_expert_lz_project_arango.bulkSave(doc_list,onDuplicate="update")
 
        logger.info("日期[{}]MONGO科研项目构建库共找到数据[{}]条，arango更新数据[{}]条".format(
            self.process_date, self.total, self.count_graph_update))
        self.close_connection()

if __name__=="__main__":

    # 科研项目最早同步日期为 2020-10-03

    kbp = KbpPipeline()
    if len(sys.argv) > 1:
        if len(sys.argv) ==3:
            kbp.process(sys.argv[1],sys.argv[2])
        else:
            kbp.process(sys.argv[1])
    else:
        kbp.process("yesterday")
