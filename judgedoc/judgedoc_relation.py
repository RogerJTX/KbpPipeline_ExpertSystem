#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-03-31 19:05
# Last modified: 2020-04-09 14:18
# Filename     : judgedoc_kbp.py
# Description  : 裁判文书-->企业 关系添加
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

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

class RelationPipeline(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.arango_db = self.arango_con[self.config.get("arango","db")]
        self.kb_judgedoc = self.arango_db[self.config.get("arango","kb_judgedoc")]
        self.kb_company = self.arango_db[self.config.get("arango","kb_company")]
        self.count_graph_update = 0 # arango更新关系数据数量
        self.total = 0 # 处理日期总共需要添加关系的数量


    def query_process_judgedoc(self, process_date):
        
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

        aql = "FOR judgedoc IN {} FILTER judgedoc.create_time >= '{}' SORT judgedoc.create_time return judgedoc".format(
                        self.config.get("arango","kb_judgedoc"), iso_date) 
        try:
            res = self.arango_db.fetch_list(aql)
        except AQLFetchError as e:
            '''没有查到相关数据时，fetch_list会抛出异常'''
            res = []
            logger.warn("Arango裁判文书库没有查到数据",e)

        self.total = len(res)
        self.process_date = process_date
        logger.info("[{}]，裁判文书知识库查到待处理数据[{}]个".format(process_date, self.total))
        return res


    def process_company_rel(self, properties):
        '''裁判文书所属企业关系建立'''
        company_rels = []

        company_name = properties["company_name"]

        company = self.kb_company.fetchFirstExample({"name": company_name})
        if not company:
            return company_rels
        company = company[0] # company返回的是cursor
        company_rel = {
            "relation_type":"concept_relation/100007",
            "object_name": company["name"],
            "object_type": "company",
            "object_id": company["_id"]
        }
        company_rels.append(company_rel)

        return company_rels   


    def process_relations(self, properties):
        '''
        添加关系
        '''
        relations = []
        company_rel = self.process_company_rel(properties)
        relations.extend(company_rel)

        return relations

            
    
    def process(self, scan_date):

        process_judgedocs = self.query_process_judgedoc(scan_date)

        count = 0

        # arango数据库裁判文书信息处理

        for judgedoc in process_judgedocs:

            #logger.info("处理裁判文书关系，裁判文书名=[{}]".format(judgedoc["name"]))
            judgedoc_key = judgedoc["_key"]
            relations = self.process_relations(judgedoc["properties"])
            try:
                doc = self.kb_judgedoc[judgedoc_key]
                doc["relations"] = relations
                doc["update_time"] = datetime.datetime.today()
                doc.save()
                self.count_graph_update += 1
            except Exception as e:
                logger.error("裁判文书关系添加失败，裁判文书名=[{}]".format(judgedoc["name"]))


            count += 1

            if count % 500 == 0 or count == self.total:
                logger.info("前[{}]家裁判文书关系添加完成".format(count))

        logger.info("日期[{}]裁判文书知识库共找到裁判文书{}个，arango裁判文书库添加裁判文书关系{}个".format(
            self.process_date, self.total, self.count_graph_update))

if __name__=="__main__":

    # 最早日期 2019-06-03

    rel = RelationPipeline()
    if len(sys.argv) > 1:
        rel.process(sys.argv[1])
    else:
        rel.process("yesterday")
