#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-04-13 19:47
# Last modified: 2020-04-13 19:47
# Filename     : financing_kbp.py
# Description  : 融资信息从构建库转移到arango
#******************************************************************************

import configparser
import sys
from pymongo import MongoClient
from pymongo import errors
from pyArango.connection import Connection as ArangoConnection
from pyArango.theExceptions import AQLFetchError
import pymysql
from dateutil import parser
import datetime
import json
import logging
import re
import copy
import requests
import os
import bson

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
        self.res_kb_process_financing = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_financing")]
        self.res_kb_process_company = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_company")]
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.arango_db = self.arango_con[self.config.get("arango","db")]
        self.kb_financing = self.arango_con[self.config.get("arango","db")][self.config.get("arango","kb_financing")]
        self.kb_company = self.arango_con[self.config.get("arango","db")][self.config.get("arango","kb_company")]
        self._init_f_round_schema() #融资论次的序列号
        self.count_graph_insert = 0 # arango新增数据数量
        self.count_graph_exist = 0 # 信息在图数据库中
        self.count_graph_update = 0
        self.count_graph_update_round = 0

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    def _init_f_round_schema(self):

        self.f_round_sort = {}

        sql_conn = pymysql.connect( host = "xxx" ,
                user = "xxx",
                passwd = "xxx",
                port = 0,
                db = "xxx",
                charset = "xxx" )
        sql_cur = sql_conn.cursor() 
        currency_sql = "select name,sequence from res_invest_round where sequence is not null"
        sql_cur.execute(currency_sql)
        for c in sql_cur.fetchall():
            self.f_round_sort[c[0]] = c[1]

        sql_cur.close()
        sql_conn.close()


    def insert_graph_kb(self, kf):
        
        try:
            doc = self.kb_financing.createDocument(kf)
            doc.save()
            #logger.info("Arango融资库新增完成，融资名=[{}]".format(kf["name"]))
            self.count_graph_insert += 1

        except Exception as e:
            logger.error("新增融资信息至arango失败，融资名=[{}]，信息=[{}]".format(kf["name"], kf), e) 

    def update_graph_kb(self, kf, up_key):

        try:
            doc = self.kb_financing[kf['_key']]
            doc[up_key] = kf[up_key]
            doc.save()
            #logger.info("Arango企业库更新完成，企业名=[{}]，更新字段[{}]".format(kf["name"],up_key))
            self.count_graph_update += 1
            if self.count_graph_update%1000 ==0:
                logger.info("企业前[{}]条更新完成".format(self.count_graph_update))

        except Exception as e:
            logger.error("更新企业信息至arango失败，企业名=[{}]，更新信息=[{}]".format(kf["name"], kf[up_key]), e)

    def update_graph_company_round(self, company_name, financing_round):
        "查询kb_company的financing_round字段，如果层次低于当前获取的，则更新"
        company_key = ''
        company_round = ''
        #根据公司名获取公司_key和属性下的融资论次字段
        try:
            res = self.res_kb_process_company.find_one({'name':company_name})
        except AQLFetchError as e:
            '''没有查到数据时，fetch_list会抛出异常'''
            res = []
            logger.warn("Arango企业库没有查到数据",e)
        if res:
            company_round = res["financing_round"]
            company_key = str(res["_id"])
            #print(company_key,company_round,financing_round)
            #print(self.f_round_sort[company_round] , self.f_round_sort[financing_round])
            if company_round not in self.f_round_sort or financing_round not in self.f_round_sort:
                return False
            if self.f_round_sort[company_round] < self.f_round_sort[financing_round]:#新获取的融资轮次大于企业旧数据，则更新
                doc = self.kb_company[company_key]
                old_round = doc["financing_round"]
                if self.f_round_sort[old_round] < self.f_round_sort[financing_round]:
                    doc["properties"]["financing_round"] = financing_round
                    doc["update_time"] = datetime.datetime.today()
                    doc.save()
                    self.count_graph_update_round += 1
                    return True
        return False

    def process_properties(self, _property):

        #_property.pop("crawl_time")
        _property["crawl_time"] = str(_property["crawl_time"])
        _property.pop("html")

        return _property

    def query_process(self, process_date):
        
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
        # res = self.res_kb_process_financing.find({'crawl_time': {'$gte': iso_date}}).sort([("crawl_time",1)])
        # 迁移余量数据使用
        new_g_table = self.res_kb_process_financing.with_options(codec_options = bson.CodecOptions(unicode_decode_error_handler="ignore")) 
        res = new_g_table.find({'create_time': {'$gte': iso_date}}, no_cursor_timeout=True).sort([("crawl_time",1)])
        self.total = res.count()
        self.process_date = process_date
        return res



    def process(self, date_str, end_date = ""):
        process_date = date_str
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
        new_g_table = self.res_kb_process_financing.with_options(codec_options = bson.CodecOptions(unicode_decode_error_handler="ignore")) 
        mg_cmd_str = {}
        iso_date_str = process_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        self.process_date = process_date
        self.end_process_date = end_date
        #res_one = new_g_table.find_one({'$or':[{'crawl_time': {'$gte': iso_date}},{'update_time':{'$gte': iso_date}}]})
        if not end_date:
            mg_cmd_str = {'$or':[{'crawl_time': {'$gte': iso_date}},{'update_time':{'$gte': iso_date}}]}
        else:
            mg_cmd_str = {'$or':[{'crawl_time': {'$gte': iso_date,'$lt': iso_end_date}},{'update_time':{'$gte': iso_date,'$lt': iso_end_date}}]}
        self.total = new_g_table.count_documents(mg_cmd_str)
        #experts = self.query_process(date_str)
        if self.total > 0:
            res_sort = new_g_table.find(mg_cmd_str).sort([("_id",1)])
            last_id = res_sort[0]['_id']
            sum_block = int(self.total/1000)+1
        else:
            self.total = 0
            sum_block = 0
        logger.info("[{}]-[{}]，清洗库查到待处理数据[{}]个,分为{}块处理".format(self.process_date, self.end_process_date, self.total,sum_block))


        count = 0
        first_block_f = True
        # arango数据库融资信息处理
        for block in range(sum_block):
            financings = []
            if first_block_f:
                first_block_f = False
                mg_cmd_str['_id'] = {'$gte':last_id}
            else:
                mg_cmd_str['_id'] = {'$gt':last_id}
            financings = list(new_g_table.find(mg_cmd_str).sort([("_id",1)]).limit(1000))
            if financings:
                last_id = financings[-1]['_id']
            for financing in financings:
                count += 1
                #logger.info("处理融资公司[{}]".format(financing["company"]))
                # arango现存融资查询
                exist = self.kb_financing.fetchFirstExample( {"_key":financing["_id"]}  )
                properties = self.process_properties(financing)

                if not exist:
                    #logger.info("Arango新增融资[{}]".format(financing["company"]))
                    # tags 暂时没有需要加的
                    kf = {
                        "_key":financing["_id"],
                        "name":financing["company"],
                        "properties":properties,
                        "crawl_time":properties["crawl_time"],
                        "tags":[],
                        "relations":[],
                        "create_time": datetime.datetime.today(),  # Arango实体统一添加，方便后面审核修改数据使用
                        "update_time": datetime.datetime.today(),
                    }
                    self.insert_graph_kb(kf) 
                else:
                    #logger.info("融资数据已存在图数据库中，融资名=[{}]，融资ID=[{}]".format(financing["name"], financing["_id"])) 
                    self.count_graph_exist += 1
                    #self.update_graph_company_round(financing['company'],properties['finance_round'])

                if count % 1000 == 0 or count == self.total:
                    logger.info("清洗库前[{}]条融资数据导入融资知识库处理完成,插入{}条，更新{}条".format(count,self.count_graph_insert,self.count_graph_update_round))

        self.close_connection() 

        logger.info("日期[{}]共找到清洗库融资数据[{}]条，arango融资库新增数据{}条，arango已存在融资数据[{}]条,更新企业融资轮次字段[{}]条".format(
            self.process_date, self.total, self.count_graph_insert, self.count_graph_exist,self.count_graph_update_round))
        print(count)

if __name__=="__main__":

    # 融资最早爬虫日期为 2019-10-23

    kbp = KbpPipeline()
    if len(sys.argv) > 1:
        if len(sys.argv) ==3:
            kbp.process(sys.argv[1],sys.argv[2])
        else:
            kbp.process(sys.argv[1])
    else:
        kbp.process("yesterday")
