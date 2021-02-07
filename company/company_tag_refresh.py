#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-04-28 15:05
# Last modified: 2020-04-28 15:18
# Filename     : company_tag_kbp.py
# Description  : 人工梳理环节 企业标签更新检测与同步到Arango库
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
        self.sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                user = self.config.get("mysql","user"),
                passwd = self.config.get("mysql","passwd"),
                port = self.config.getint("mysql","port") ,
                db = self.config.get("mysql","db"),
                charset = self.config.get("mysql","charset") )
        self.sql_cur = self.sql_conn.cursor() 
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.graph_kb_company = self.arango_con[self.config.get("arango","db")][self.config.get("arango","kb_company")]
        self._init_tag_schema() # init self.tag_schema, self.industry_schema from mysql
        self.total = 0 # 发现更新企业标签数量
        self.count_skip = 0 # Arango中不存在的企业数量
        self.count_update = 0 # 实际更新数量
        self.count_update_fail = 0 # 更新失败数量

    def _init_tag_schema(self):
        '''
        init loading tag schema at mysql res_tag table
        '''
        self.tag_schema = {}

        # 初始化企业标签schema
        sql_query_label = "select name, type, id from {}".format(self.config.get("mysql","res_tag"))
        self.sql_cur.execute(sql_query_label)
        labels = self.sql_cur.fetchall()
        for label in labels:
            label_name, label_type, label_id = label
            self.tag_schema[label_name] = {
                "name":label_name,
                "type": label_type,
                "id": label_id
            }
        logger.info("MYSQL tag schema 加载完成")

    def close_connection(self):

        if self.sql_cur:
            self.sql_cur.close()
        if self.sql_conn:
            self.sql_conn.close()


    def process_company_tags(self,company_name):
        '''
        获取公司新的标签数据
        '''
        res = []

        sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                            user = self.config.get("mysql","user") ,
                            passwd = self.config.get("mysql","passwd"),
                            port = self.config.getint("mysql","port") ,
                            db = self.config.get("mysql","db"),
                            charset = "utf8" )
        sql_cur = sql_conn.cursor() 
        # 查询企业相关的标签信息
        sql_state = self.config.get("mysql","company_tags_query").replace("eq","=").format(company_name)
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

    




    def query_new_tags(self, process_date):
        '''根据人工梳理MYSQL库的更新时间获取更新数据'''

        datas = []

        if process_date == "yesterday":
            process_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif process_date == "today":
            process_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(process_date.split("-")) == 3:
            process_date = process_date
            
        else:
            raise Exception("无效参数")
        
        self.process_date = process_date
        sql_state = self.config.get("mysql","company_tag_refresh").replace("eq","=").replace("gte",">=").format(process_date)
        self.sql_cur.execute(sql_state)

        for data in self.sql_cur.fetchall():
            company_name = data[0]
            if company_name in datas:
                continue 
            else:
                datas.append(company_name)
        return datas           
    
    def process(self, scan_date):

        no_data = []

        process_companys = self.query_new_tags(scan_date)
        self.total = len(process_companys)

        count = 0

        # arango数据库企业信息处理

        for company in process_companys:

            logger.info("更新企业标签，企业名称=[{}]".format(company))

            # Arango查找企业
            exist = self.graph_kb_company.fetchFirstExample({"name":company})

            if exist:
                try:
                    exist_key = exist[0]["_key"]
                    doc = self.graph_kb_company[exist_key]
                    old = doc["tags"]
                    # 更新公司类型的tag
                    for tag in copy.deepcopy(old):
                        if tag["type"] == "company_tag":
                            old.remove(tag)
                    new_tags = self.process_company_tags(company)
                    new_tags.extend(old)
                    doc["tags"] = new_tags  # 更新
                    doc["update_time"] = datetime.datetime.today()
                    doc.save()
                    self.count_update += 1
                except Exception as e:
                    logger.error("Arango企业标签更新失败，公司名=[{}]".format(company), e)
                    self.count_update_fail += 1
                
            else:
                logger.warning("企业尚未导入企业库，企业名称=[{}]".format(company))
                no_data.append(company)
                self.count_skip += 1


            count += 1

            if count % 100 == 0 or count == self.total:
                logger.info("人工梳理库前[{}]家企业的标签更新到企业知识库处理完成".format(count))


        self.close_connection() 
        with open("tag_no_company.txt","w") as fw:
            fw.write("\n".join(no_data))

        logger.info("日期[{}]人工梳理的企业标签库发现更新{}家企业标签，跳过企业不存在记录[{}]个，更新记录[{}]个，更新失败[{}]个".format(
            self.process_date, self.total, self.count_skip, self.count_update, self.count_update_fail))

if __name__=="__main__":

    # 最早日期 2019-06-03

    kbp = KbpPipeline()
    if len(sys.argv) > 1:
        kbp.process(sys.argv[1])
    else:
        kbp.process("yesterday")


        

                    









