#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-05-08 17:47
# Last modified: 2020-05-08 17:47
# Filename     : expert_kbp.py
# Description  : 专家信息从构建库转移到arango
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
        self.res_kb_process_expert = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_expert")]
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.kb_expert = self.arango_con[self.config.get("arango","db")][self.config.get("arango","kb_expert")]
        self.count_graph_insert = 0 # arango新增数据数量
        self.count_graph_exist = 0 # 信息在图数据库中
        self.count_graph_update = 0
            

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    def insert_graph_kb(self, kf):
        
        try:
            doc = self.kb_expert.createDocument(kf)
            doc.save()
            #logger.info("Arango专家库新增完成，专家名=[{}]".format(kf["name"]))
            self.count_graph_insert += 1

        except Exception as e:
            logger.error("新增专家信息至arango失败，专家名=[{}]，信息=[{}]".format(kf["name"], kf), e) 

    def update_graph_kb(self, kf, up_key):

        try:
            doc = self.kb_expert[kf['_key']]
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
        new_g_table = self.res_kb_process_expert.with_options(codec_options = bson.CodecOptions(unicode_decode_error_handler="ignore")) 
        iso_date_str = process_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        res = new_g_table.find({'$or':[{'crawl_time': {'$gte': iso_date}},{'update_time':{'$gte': iso_date}}]}).sort([("crawl_time",1)])
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
        new_g_table = self.res_kb_process_expert.with_options(codec_options = bson.CodecOptions(unicode_decode_error_handler="ignore")) 
        iso_date_str = process_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        self.process_date = process_date
        self.end_process_date = end_date
        if not end_date:
            mg_cmd_str = {'$or':[{'crawl_time': {'$gte': iso_date}},{'update_time':{'$gte': iso_date}}]}
        else:
            mg_cmd_str = {'$or':[{'crawl_time': {'$gte': iso_date,'$lt': iso_end_date}},{'update_time':{'$gte': iso_date,'$lt': iso_end_date}}]}
        self.total = new_g_table.count_documents(mg_cmd_str)
        #experts = self.query_process(date_str)
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

        # arango数据库专家信息处理
        for block in range(sum_block):
            if first_block_f:
                first_block_f = False
                mg_cmd_str["_id"] = {'$gte':last_id}
            else:
                mg_cmd_str["_id"] = {'$gt':last_id}
            experts = list(new_g_table.find(mg_cmd_str).sort([("_id",1)]).limit(1000))
            if experts:
                last_id = experts[-1]['_id']
            for expert in experts:
                #logger.info("处理专家[{}]".format(expert["name"]))
                count += 1

                # arango现存专家查询
                exist = self.kb_expert.fetchFirstExample( {"_key":expert["_id"]}  )

                if not exist:

                    #logger.info("Arango新增专家[{}]".format(expert["name"]))

                    properties = self.process_properties(expert)

                    # tags 暂时没有需要加的
                    kf = {
                        "_key":expert["_id"],
                        "name":expert["name"],
                        "properties":properties,
                        # "crawl_time":properties["crawl_time"],
                        "tags":[],
                        "relations":[],
                        "create_time": datetime.datetime.today(),  # Arango实体统一添加，方便后面审核修改数据使用
                        "update_time": datetime.datetime.today(),
                    }

                    #self.insert_graph_kb(kf) 
                    self.count_graph_insert += 1
                    doc_list.append(kf)
                    if self.count_graph_insert %100==0:
                        self.kb_expert.bulkSave(doc_list)
                        doc_list = []
                        logger.info("清洗库前[{}]条专家数据导入专家知识库处理完成,写入[{}]条".format(count,self.count_graph_insert))

                else:
                    # 专家已存在，但属性有更新
                    #logger.info("专家数据已存在图数据库中，专家名=[{}]，专家ID=[{}]".format(expert["name"], expert["_id"])) 
                    try:
                        #doc = self.kb_expert[expert["_id"]]
                        #doc["properties"] = self.process_properties(expert)
                        #doc["update_time"] = datetime.datetime.today()
                        #doc.save()
                        self.count_graph_exist += 1
                    except Exception as e:
                        logger.error("专家属性更新失败，专家名=[{}]".format(expert["name"]))
        if doc_list:                       
            self.kb_expert.bulkSave(doc_list)

        self.close_connection() 

        logger.info("日期[{}]共找到清洗库专家数据[{}]条，arango专家库新增数据{}条，arango已存在专家数据[{}]条".format(
            self.process_date, self.total, self.count_graph_insert, self.count_graph_exist))

if __name__=="__main__":

    # 专家最早爬虫日期为 2019-10-09

    kbp = KbpPipeline()
    if len(sys.argv) > 1:
        if len(sys.argv) ==3:
            kbp.process(sys.argv[1],sys.argv[2])
        else:
            kbp.process(sys.argv[1])
    else:
        kbp.process("yesterday")
