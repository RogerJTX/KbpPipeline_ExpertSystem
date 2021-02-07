#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-04-28
# Last modified: 2020-04-28
# Filename     : company_channel_relation.py
# Description  : 人工整理的企业-渠道数据监测更新与同步到Arango库
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
        self.total = 0 # 发现更新企业渠道记录的企业数量
        self.count_skip = 0 # Arango中不存在的企业数量
        self.count_update = 0 # 实际更新数量
        self.count_update_fail = 0 # 更新失败数量

    def close_connection(self):

        if self.sql_cur:
            self.sql_cur.close()
        if self.sql_conn:
            self.sql_conn.close()

    def process_label_tags(self,tags):
        '''
        schema中定义的公司标签ID化添加
        '''
        process_res = []

        if tags:
            process_res.extend([self.tag_schema[tag] for tag in tags])

        return process_res

    def query_company_channel(self, company_name):
        '''查询公司更新过的渠道数据'''
        channel_rel = []
        sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                            user = self.config.get("mysql","user") ,
                            passwd = self.config.get("mysql","passwd"),
                            port = self.config.getint("mysql","port") ,
                            db = self.config.get("mysql","db"),
                            charset = "utf8" )
        sql_cur = sql_conn.cursor() 
        # 查询企业相关的渠道信息
        sql_state = self.config.get("mysql","company_channel_query").replace("eq","=").format(company_name)
        sql_cur.execute(sql_state)
        datas = sql_cur.fetchall()
        for data in datas:
            company_name, channel_name, channel_id = data
            rel = {
                "relation_type":"concept_relation/100010",
                "object_name":channel_name,
                "object_type": "channel",
                "object_id": channel_id,
            }
            channel_rel.append(rel)

        sql_cur.close()
        sql_conn.close()
        return channel_rel



    def query_new_data(self, process_date):
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
        sql_state = self.config.get("mysql","company_channel_refresh").replace("gte",">=").replace("eq","=").format(process_date)
        self.sql_cur.execute(sql_state)

        for data in self.sql_cur.fetchall():
            company_name = data[0]
            if company_name in datas:
                continue
            else:
                datas.append(company_name)

        return datas           
    
    def process(self, scan_date):

        updated_datas = self.query_new_data(scan_date)
        self.total = len(updated_datas)

        count = 0

        # arango数据库企业信息处理

        for company in updated_datas:

            logger.info("更新企业渠道关系，企业名称=[{}]".format(company))

            # Arango查找企业
            exist = self.graph_kb_company.fetchFirstExample({"name":company})

            if exist:
                try:
                    exist_key = exist[0]["_key"]
                    doc = self.graph_kb_company[exist_key]
                    old = doc["relations"]
                    # 更新关系为channel的关系数据
                    for relation in copy.deepcopy(old):
                        if relation["object_type"] == "channel":
                            old.remove(relation)
                    new_rels = self.query_company_channel(company) #新渠道数据
                    new_rels.extend(old) # 旧其他类型的关系
                    doc["relations"] = new_rels  # 更新
                    doc["update_time"] = datetime.datetime.today()
                    doc.save()
                    self.count_update += 1
                except Exception as e:
                    logger.error("Arango企业渠道关系更新失败，公司名=[{}]".format(company), e)
                    self.count_update_fail += 1
                
            else:
                logger.warning("企业尚未导入企业库，企业名称=[{}]".format(company))
                self.count_skip += 1


            count += 1

            if count % 100 == 0 or count == self.total:
                logger.info("人工梳理库前[{}]家企业渠道关系更新到企业知识库处理完成".format(count))


        self.close_connection() 

        logger.info("日期[{}]人工梳理的企业渠道关系表发现更新{}个，跳过企业不存在记录[{}]个，更新记录[{}]个，更新失败[{}]个".format(
            self.process_date, self.total, self.count_skip, self.count_update, self.count_update_fail))

if __name__=="__main__":

    # 最早日期 2019-06-03

    kbp = KbpPipeline()
    if len(sys.argv) > 1:
        kbp.process(sys.argv[1])
    else:
        kbp.process("yesterday")


        

                    









