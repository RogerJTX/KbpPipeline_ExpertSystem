#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-03-31 19:05
# Last modified: 2020-04-09 14:18
# Filename     : organization_kbp.py
# Description  : 高校机构-->企业 关系添加
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
        self.kb_organization = self.arango_db[self.config.get("arango","kb_organization")]
        self.kb_company = self.arango_db[self.config.get("arango","kb_company")]
        self._init_division_schema()
        self.count_graph_update = 0 # arango更新关系数据数量
        self.total = 0 # 处理日期总共需要添加关系的数量

    def _init_division_schema(self):
        '''
        行政区域实体关系加载
        '''
        self.division_schema = {}
        sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                            user = self.config.get("mysql","user") ,
                            passwd = self.config.get("mysql","passwd"),
                            port = self.config.getint("mysql","port") ,
                            db = self.config.get("mysql","db"),
                            charset = "utf8" )
        sql_cur = sql_conn.cursor() 

        # 初始化行政区域的关系schema
        sql_query_industry = "select name, id, level, parent_id from {}".format(self.config.get("mysql","res_division"))
        sql_cur.execute(sql_query_industry)
        divisions = sql_cur.fetchall()
        for division in divisions:
            division_name, division_id, division_level, division_parent_id = division
            self.division_schema[division_name] = {
                "relation_type":"concept_relation/100004",
                "object_name":division_name,
                "object_type": "division",
                "object_id": division_id
            }

        sql_cur.close()
        sql_conn.close()
        logger.info("MYSQL division schema 加载完成")


    def query_process_organization(self, process_date):
        
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

        aql = "FOR organization IN {} FILTER organization.create_time >= '{}' SORT organization.create_time return organization".format(
                        self.config.get("arango","kb_organization"), iso_date) 
        try:
            res = self.arango_db.fetch_list(aql)
        except AQLFetchError as e:
            '''没有查到数据时，fetch_list会抛出异常'''
            res = []
            logger.warn("Arango高校机构库没有查到数据",e)

        self.total = len(res)
        self.process_date = process_date
        logger.info("[{}]，高校机构知识库查到待处理数据[{}]个".format(process_date, self.total))
        return res


    def process_university_rel(self, properties):
        '''机构所属高校关系建立'''
        university_rels = []

        university_name = properties["university"]
        # 有高校信息的添加所属高校
        if university_name:
            university = self.kb_organization.fetchFirstExample({"name": university_name})
            if not university:
                return university_rels
            university = university[0] # university 返回的是cursor
            university_rel = {
                "relation_type":"concept_relation/100009",
                "object_name": university["name"],
                "object_type": "organization",
                "object_id": university["_id"]
            }
            university_rels.append(university_rel)

        return university_rels  

    def process_division_rel(self, properties):
        div_rel = []
        province = properties["province"]
        city = properties["city"]
        area = properties["area"]

        if province and province in self.division_schema.keys():
            if province in ["北京市","上海市","重庆市","天津市"]:
                province = province.replace("市","")
            div_rel.append(self.division_schema[province])

        if city and city in self.division_schema.keys():
            div_rel.append(self.division_schema[city])

        if area and area in self.division_schema.keys():
            div_rel.append(self.division_schema[area])

        return div_rel 


    def process_relations(self, properties):
        '''
        添加行政区划与所属高校关系
        '''
        relations = []

        division_rel = self.process_division_rel(properties)
        relations.extend(division_rel)

        university_rel = self.process_university_rel(properties)
        relations.extend(university_rel)

        return relations

            
    
    def process(self, scan_date):

        process_organizations = self.query_process_organization(scan_date)

        count = 0

        # arango数据库高校机构信息处理

        for organization in process_organizations:

            logger.info("处理高校机构关系，高校机构名=[{}]".format(organization["name"]))
            organization_key = organization["_key"]
            relations = self.process_relations(organization["properties"])
            try:
                doc = self.kb_organization[organization_key]
                doc["relations"] = relations
                doc["update_time"] = datetime.datetime.today()
                doc.save()
                self.count_graph_update += 1
            except Exception as e:
                logger.error("高校机构关系添加失败，高校机构名=[{}]".format(organization["name"]),e)


            count += 1

            if count % 100 == 0 or count == self.total:
                logger.info("前[{}]家高校机构关系添加完成".format(count))


        logger.info("日期[{}]高校机构知识库共找到高校机构{}个，arango高校机构库添加高校机构关系{}个".format(
            self.process_date, self.total, self.count_graph_update))

if __name__=="__main__":

    # 最早日期 2019-06-25

    rel = RelationPipeline()
    if len(sys.argv) > 1:
        rel.process(sys.argv[1])
    else:
        rel.process("yesterday")


        

                    









