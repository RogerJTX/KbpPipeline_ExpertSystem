#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-03-31 19:05
# Last modified: 2020-04-09 14:18
# Filename     : company_kbp.py
# Description  : 企业清洗库转移到企业知识库，处理：产业/产业领域标签添加、企业标签schema添加
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
        self.res_kb_process_company = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_company")] # 清洗库
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.graph_kb_company = self.arango_con[self.config.get("arango","db")][self.config.get("arango","kb_company")]
        self._init_label_schema() # init self.label_schema from mysql
        self.count_graph_insert = 0 # arango新增数据数量
        self.count_graph_exist = 0 # 企业信息在图数据库中
        self.total = 0 # 处理日期清洗库待处理的企业数

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

        # 初始化企业标签schema
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
        # 查询企业相关的标签信息
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

        # # 产业/产业领域标签 产业领域标签添加到relations，不再tags里添加
        # industry_tags = self.process_industry_tags(properties["name"])
        # process_res.extend(industry_tags)

        # 企业标签
        company_tags = self.process_company_tags(properties)
        process_res.extend(company_tags)

        return process_res

    def insert_graph_kb(self, kf):
        
        try:
            doc = self.graph_kb_company.createDocument(kf)
            doc.save()
            logger.info("Arango企业库新增完成，企业名=[{}]".format(kf["name"]))
            self.count_graph_insert += 1

        except Exception as e:
            logger.error("新增企业信息至arango失败，企业名=[{}]，信息=[{}]".format(kf["name"], kf), e)   

    def update_graph_kb(self, kf, up_key):

        try:
            doc = self.graph_kb_company[kf['_key']]
            doc[up_key] = kf[up_key]
            doc.save()
            #logger.info("Arango企业库更新完成，企业名=[{}]，更新字段[{}]".format(kf["name"],up_key))
            self.count_graph_update += 1
            if self.count_graph_update%1000 ==0:
                logger.info("企业前[{}]条更新完成".format(self.count_graph_update))

        except Exception as e:
            logger.error("更新企业信息至arango失败，企业名=[{}]，更新信息=[{}]".format(kf["name"], kf[up_key]), e)

    def get_last_execute_time(self):
        sql_conn = pymysql.connect(host="xxxx",
                           user="xxxx",
                           passwd="xxxx",
                           port=0000,
                           db="xxxx",
                           charset="xxxx")
        sql_cur = sql_conn.cursor()
        currency_sql = """
        SELECT FROM_UNIXTIME(start_time/1000,'%Y-%m-%d') as start_time FROM execution_jobs
        where job_id="{}" and status=50
         order by exec_id desc limit 1
        """.format("company_clean")
        sql_cur.execute(currency_sql)
        sql_result = sql_cur.fetchall()
        return sql_result[0][0]


    def query_process_company(self, process_date, end_date=""):
        
        if process_date == "yesterday":
            #process_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            process_date = self.get_last_execute_time()
        
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
        res = self.res_kb_process_company.find(mg_cmd_str).sort([("_id",1)])

        # 迁移剩余所有数据使用
        # res = self.res_kb_process_company.find({'create_time': {'$gte': iso_date}}, no_cursor_timeout=True).sort([("crawl_time",1)])

        self.total = res.count()
        self.process_date = process_date
        self.end_process_date = end_date
        logger.info("[{}]-[{}]，清洗库查到待处理数据[{}]个".format(self.process_date, self.end_process_date, self.total))
        return res

    
    def process(self, scan_date, end_date=""):

        process_companys = self.query_process_company(scan_date, end_date)

        count = 0

        # arango数据库企业信息处理

        for company in process_companys:

            logger.info("查询推荐企业[{}]".format(company["name"]))

            # Arango查重
            exist = self.graph_kb_company.fetchFirstExample({"name":company["name"]})

            if not exist:

                logger.info("发现需新增企业[{}]，准备转移到企业知识库".format(company["name"]))
                properties = self.process_basic_info(company)
                alter_names = properties.pop("alter_names")
                tags = self.process_tags(properties)
                
                kf = {
                    "_key":properties["_id"],
                    "name":company["name"],
                    #"financing_round":properties["financing_round"],
                    "alter_names": alter_names,
                    "properties":properties,
                    #"crawl_time":properties["crawl_time"],
                    "tags":tags,
                    "relations":[],
                    "create_time": datetime.datetime.today(),
                    "update_time": datetime.datetime.today()
                }

                self.insert_graph_kb(kf) 

            else:
                logger.info("企业数据已存在图数据库中，企业名=[{}]".format(company)) 
                self.count_graph_exist += 1

            count += 1

            if count % 100 == 0 or count == self.total:
                logger.info("清洗库前[{}]家企业导入企业知识库处理完成".format(count))


        self.close_connection() 

        logger.info("日期[{}]清洗库共找到企业{}个，arango企业库新增{}个，arango已有[{}]个".format(
            self.process_date, self.total, self.count_graph_insert, self.count_graph_exist))

if __name__=="__main__":

    # 最早日期 2019-06-03

    kbp = KbpPipeline()
    if len(sys.argv) > 1:
        if len(sys.argv) ==3:
            kbp.process(sys.argv[1],sys.argv[2])
        else:
            kbp.process(sys.argv[1])
    else:
        kbp.process("yesterday")
