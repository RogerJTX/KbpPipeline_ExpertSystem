#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-08-13 17:35
# Last modified: 2020-08-13 17:53
# Filename     : invest_institution_kbp.py
# Description  : 投资机构清洗库转移到投资机构知识库，处理：产业/产业领域标签添加、投资机构标签schema添加
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
        self.res_kb_process_invest_institution = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_invest_institution")] # 清洗库
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.graph_kb_invest_institution = self.arango_con[self.config.get("arango","db")][self.config.get("arango","kb_invest_institution")]
        self._init_label_schema() # init self.label_schema from mysql
        self.count_graph_insert = 0 # arango新增数据数量
        self.count_graph_exist = 0 # 投资机构信息在图数据库中
        self.total = 0 # 处理日期清洗库待处理的投资机构数

    def _init_label_schema(self):
        '''
        init loading label schema at mysql res_label table
        '''
        self.label_schema = {}
        self.industry_schema = {}
        sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                            user = self.config.get("mysql","user") ,
                            passwd = self.config.get("mysql","passwd"),
                            port = self.config.getint("mysql","port") ,
                            db = self.config.get("mysql","db"),
                            charset = "utf8" )
        sql_cur = sql_conn.cursor() 

        # 初始化投资机构标签schema
        sql_query_label = "select name, type, id from {}".format(self.config.get("mysql","res_tag"))
        sql_cur.execute(sql_query_label)
        labels = sql_cur.fetchall()
        for label in labels:
            label_name, label_type, label_id = label
            self.label_schema[label_name] = {
                "name":label_name,
                "type": label_type,
                "id": label_id
            }

        sql_cur.close()
        sql_conn.close()
        logger.info("MYSQL label schema 加载完成")


    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    def process_basic_info(self,company_info):

        #company_info.pop("crawl_time")
        company_info["crawl_time"] = str(company_info["crawl_time"])
        company_info.pop("html")
     
        return company_info

    def process_company_tags(self,properties):
        '''
        schema中定义的公司标签ID化添加
        '''

        # 人工梳理的标签确定，再进行ID化
        res = []

        sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                            user = self.config.get("mysql","user") ,
                            passwd = self.config.get("mysql","passwd"),
                            port = self.config.getint("mysql","port") ,
                            db = self.config.get("mysql","db"),
                            charset = "utf8" )
        sql_cur = sql_conn.cursor() 
        # 查询投资机构相关的标签信息
        sql_state = self.config.get("mysql","company_tags_query").replace("eq","=").format(properties["name"])
        sql_cur.execute(sql_state)
        datas = sql_cur.fetchall()
        for data in datas:
            company_name, tag_name, tag_type, tag_id = data
            tag = {
                "type":tag_type,
                "name":tag_name,
                "id": tag_id
            }
            res.append(tag)

        sql_cur.close()
        sql_conn.close()
        return res

    def process_tags(self,properties):

        process_res = []

        # 投资机构标签
        company_tags = self.process_company_tags(properties)
        process_res.extend(company_tags)

        return process_res

    def insert_graph_kb(self, kf):
        
        try:
            doc = self.graph_kb_invest_institution.createDocument(kf)
            doc.save()
            logger.info("Arango投资机构库新增完成，投资机构名=[{}]".format(kf["name"]))
            self.count_graph_insert += 1

        except Exception as e:
            logger.error("新增投资机构信息至arango失败，投资机构名=[{}]，信息=[{}]".format(kf["name"], kf), e)   

    def update_graph_kb(self, kf, up_key):

        try:
            doc = self.graph_kb_invest_institution[kf['_key']]
            doc[up_key] = kf[up_key]
            doc.save()
            #logger.info("Arango投资机构库更新完成，投资机构名=[{}]，更新字段[{}]".format(kf["name"],up_key))
            self.count_graph_update += 1
            if self.count_graph_update%1000 ==0:
                logger.info("投资机构前[{}]条更新完成".format(self.count_graph_update))

        except Exception as e:
            logger.error("更新投资机构信息至arango失败，投资机构名=[{}]，更新信息=[{}]".format(kf["name"], kf[up_key]), e)

    def query_process_company(self, process_date, end_date=""):
        
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
        res = self.res_kb_process_invest_institution.find(mg_cmd_str).sort([("_id",1)])

        # 迁移剩余所有数据使用
        # res = self.res_kb_process_invest_institution.find({'create_time': {'$gte': iso_date}}, no_cursor_timeout=True).sort([("crawl_time",1)])

        self.total = res.count()
        self.process_date = process_date
        self.end_process_date = end_date
        logger.info("[{}]-[{}]，清洗库查到待处理数据[{}]个".format(self.process_date, self.end_process_date, self.total))
        return res

    
    def process(self, scan_date, end_date=""):

        process_companys = self.query_process_company(scan_date, end_date)
        logger.info("[{}]清洗库新增投资机构[{}]条，准备处理".format(self.process_date, self.total))
        count = 0
        
        # arango数据库投资机构信息处理
        for company in process_companys:
            logger.info("处理投资机构[{}]".format(company["name"]))
            # Arango查重
            exist = self.graph_kb_invest_institution.fetchFirstExample({"name":company["name"]})

            if not exist:
                logger.info("新增投资机构[{}]，准备转移到投资机构知识库".format(company["name"]))
                properties = self.process_basic_info(company)
                alter_names = properties.pop("alter_names")
                tags = self.process_tags(properties)
                               
                kf = {
                    "_key":properties["_id"],
                    "name":company["name"],
                    "alter_names": alter_names,
                    "properties":properties,
                    "tags":tags,
                    "relations":[],
                    "create_time": datetime.datetime.today(),
                    "update_time": datetime.datetime.today()
                }
                self.insert_graph_kb(kf) 
            else:
                logger.info("投资机构数据已存在图数据库中，投资机构名=[{}]".format(company)) 
                self.count_graph_exist += 1

            count += 1
            if count % 100 == 0 or count == self.total:
                logger.info("清洗库前[{}]家投资机构导入投资机构知识库处理完成".format(count))

        self.close_connection() 
        logger.info("日期[{}]清洗库共找到投资机构{}个，arango投资机构库新增{}个，arango已有[{}]个".format(
            self.process_date, self.total, self.count_graph_insert, self.count_graph_exist))

if __name__=="__main__":

    # 最早日期 2019-07-03

    kbp = KbpPipeline()
    if len(sys.argv) > 1:
        if len(sys.argv) ==3:
            kbp.process(sys.argv[1],sys.argv[2])
        else:
            kbp.process(sys.argv[1])
    else:
        kbp.process("yesterday")
