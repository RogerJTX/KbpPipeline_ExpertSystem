#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-03-31 19:05
# Last modified: 2020-04-09 14:18
# Filename     : patent_kbp.py
# Description  : 专利-->企业 关系添加
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
        self.kb_patent = self.arango_db[self.config.get("arango","kb_patent")]
        self.kb_company = self.arango_db[self.config.get("arango","kb_company")]
        self.industry_url = self.config.get("url","patent_classifier")
        self._init_division_schema() # init division_schema from mysql
        self._init_industry_schema()
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


    def _init_industry_schema(self):
        '''
        init loading industry schema at mysql res_industry table
        '''
        self.industry_schema = {}
        sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                            user = self.config.get("mysql","user") ,
                            passwd = self.config.get("mysql","passwd"),
                            port = self.config.getint("mysql","port") ,
                            db = self.config.get("mysql","db"),
                            charset = "utf8" )
        sql_cur = sql_conn.cursor() 

        # 初始化产业/产业领域 schema
        sql_query_industry = "select name, id, parent_id from {}".format(self.config.get("mysql","res_industry"))
        sql_cur.execute(sql_query_industry)
        labels = sql_cur.fetchall()
        for industry in labels:
            industry_name, industry_id, parent_id = industry
            self.industry_schema[industry_id] = {
                "relation_type":"concept_relation/100011",
                "object_name":industry_name,
                "object_type": "industry",
                "object_id": industry_id,
                "object_parent_id": parent_id
            }



    def query_process_patent(self, process_date):
        
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

        aql = "FOR patent IN {} FILTER patent.create_time >= '{}' SORT patent.create_time return patent".format(
                        self.config.get("arango","kb_patent"), iso_date) 
        try:
            res = self.arango_db.fetch_list(aql)
        except AQLFetchError as e:
            '''没有查到相关数据时，fetch_list会抛出异常'''
            res = []
            logger.warn("Arango专利库没有查到数据",e)

        self.total = len(res)
        self.process_date = process_date
        logger.info("[{}]，专利知识库查到待处理数据[{}]个".format(process_date, self.total))
        return res

        
    def get_related_industry_tags(self, industry_id):
        '''
        根据子领域名称递归返回领域及所有父领域标签
        '''
        relations = []
        # 过滤招商领域与图谱定义不一致的
        if not industry_id in self.industry_schema:
            return relations
        
        relations.append(self.industry_schema[industry_id])
        parent_id = self.industry_schema[industry_id]["object_parent_id"]
        while (parent_id):
            node = self.industry_schema[parent_id]
            relations.append(node)
            parent_id = node["object_parent_id"]
        return relations



    def process_company_rel(self, properties):
        '''专利所属企业关系建立'''
        company_rels = []

        applicants = properties["applicant"]
        applicant = list(set(applicants))
        for applicant in applicants:
            company = self.kb_company.fetchFirstExample({"name": applicant})
            if not company:
                continue
            company = company[0] # company返回的是cursor
            company_rel = {
                "relation_type":"concept_relation/100001",
                "object_name": company["name"],
                "object_type": "company",
                "object_id": company["_id"]
            }
            company_rels.append(company_rel)

        return company_rels   
        
    def process_industry_rel(self, _key):
        '''
        产业领域标签ID化添加
        '''
        industry_tags = []

        industry_field_tags = []
        patent_id = _key

        post_data = {
            "patent_id":  patent_id ,
        }
        pack_data = json.dumps(post_data)
        try:
            res = requests.post(self.industry_url, data=pack_data)

            if res.status_code == 200:
                tags = res.json().get("body")
                industry_field_tags.extend(tags)

        except Exception as e:
            logging.error("获取专家产业领域失败，专家id=[{}]，接口=[{}]".format(patent_id,self.industry_url),e)

        for field in industry_field_tags:
            for node in self.get_related_industry_tags(field["id"]):
                if node not in industry_tags:
                    industry_tags.append(node)

        return industry_tags

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
            if city in ["北京","上海","重庆","天津"]:
                div_rel.append(self.division_schema[city+'市'])

        if area and area in self.division_schema.keys():
            div_rel.append(self.division_schema[area])

        return div_rel

    def process_relations(self, properties, _key):
        '''
        添加关系
        '''
        relations = []
        company_rel = self.process_company_rel(properties)
        relations.extend(company_rel)

        # 关联产业分类
        industry_rel = self.process_industry_rel(_key)
        relations.extend(industry_rel)

        # 籍贯关联的行政区划
        #division_rel = self.process_division_rel(properties)
        #relations.extend(division_rel)

        return relations

    def process(self, scan_date):

        process_patents = self.query_process_patent(scan_date)

        count = 0

        # arango数据库专利信息处理

        for patent in process_patents:

            count += 1
            #logger.info("处理专利关系，专利名=[{}]".format(patent["name"]))
            patent_key = patent["_key"]
            relations = self.process_relations(patent["properties"], patent_key)
            try:
                doc = self.kb_patent[patent_key]
                doc["relations"] = relations
                doc["update_time"] = datetime.datetime.today()
                doc.save()
                self.count_graph_update += 1
            except Exception as e:
                logger.error("专利关系添加失败，专利名=[{}],id=[{}]".format(patent["name"],patent_key))

            if count % 100 == 0 or count == self.total:
                logger.info("前[{}]家专利关系添加完成".format(count))

        logger.info("日期[{}]专利知识库共找到专利{}个，arango专利库添加专利关系{}个".format(
            self.process_date, self.total, self.count_graph_update))

if __name__=="__main__":

    # 最早日期 2019-06-03

    rel = RelationPipeline()
    if len(sys.argv) > 1:
        rel.process(sys.argv[1])
    else:
        rel.process("yesterday")
