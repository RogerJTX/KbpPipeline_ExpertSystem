#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-03-31 19:05
# Last modified: 2020-04-09 14:18
# Filename     : expert_kbp.py
# Description  : 专家清洗库转移到专家知识库，处理：产业/产业领域标签添加、专家标签schema添加
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
        self.kb_expert = self.arango_db[self.config.get("arango","kb_expert")]
        self.kb_organization = self.arango_db[self.config.get("arango","kb_organization")]
        self.industry_url = self.config.get("url","expert_classifier")
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

        sql_cur.close()
        sql_conn.close()
        logger.info("MYSQL industry schema 加载完成")
        


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

    def process_organization_rel(self, properties):
        '''专家所在机构关系建立'''
        organization_rels = []

        organizations = properties["research_institution"]
        rel_names = []
        for organization_name in organizations:
            organization = self.kb_organization.fetchFirstExample({"name": organization_name})
            if organization:
                organization = organization[0] # organization返回的是cursor
                if organization['name'] in rel_names:
                    continue
                rel_names.append(organization['name'])
                organization_rel = {
                    "relation_type":"concept_relation/100006",
                    "object_name": organization["name"],
                    "object_type": "organization",
                    "object_id": organization["_id"]
                }
                organization_rels.append(organization_rel)
        return organization_rels   

        

        
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

        
    def process_industry_rel(self, _key):
        '''
        产业领域标签ID化添加
        '''
        industry_tags = []

        industry_field_tags = []
        expert_id = _key

        post_data = {
            "expert_id":  expert_id ,
        }
        pack_data = json.dumps(post_data)
        try:
            res = requests.post(self.industry_url, data=pack_data)

            if res.status_code == 200:
                tags = res.json().get("body")
                industry_field_tags.extend(tags)

        except Exception as e:
            logging.error("获取专家产业领域失败，专家id=[{}]，接口=[{}]".format(expert_id,self.industry_url),e)

        for field in industry_field_tags:
            for node in self.get_related_industry_tags(field["id"]):
                if node not in industry_tags:
                    industry_tags.append(node)

        return industry_tags



    def process_relations(self, properties,_key):
        '''
        添加关系
        '''
        relations = []

        # 籍贯关联的行政区划
        division_rel = self.process_division_rel(properties)
        relations.extend(division_rel)

        # 关联机构高校
        organization_rel = self.process_organization_rel(properties)
        relations.extend(organization_rel)

        # 关联产业分类
        industry_rel = self.process_industry_rel(_key)
        relations.extend(industry_rel)

        return relations


    def query_process_expert(self, process_date):
        
        if process_date == "yesterday":
            process_date_tmp = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            end_date = datetime.datetime.today().strftime("%Y-%m-%d")
            end_process_date = end_date
            iso_end_date_str = end_process_date + 'T00:00:00+08:00'
            iso_end_date = parser.parse(iso_end_date_str)
        
        elif process_date == "today":
            process_date = datetime.datetime.today().strftime("%Y-%m-%d")
            
        elif len(process_date.split("-")) == 3:
            process_date = process_date
            if end_time:
                end_process_date = end_time
                iso_end_date_str = end_process_date + 'T00:00:00+08:00'
                iso_end_date = parser.parse(iso_end_date_str)
        else:
            raise Exception("无效参数")
        
        self.process_date = process_date_tmp
        iso_date_str = process_date_tmp + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)

        if process_date == "yesterday" or end_time:
            aql = "FOR expert IN {} FILTER expert.create_time >= '{}' and expert.create_time < '{}' return ".format(
                        self.config.get("arango","kb_expert"), iso_date,iso_end_date)
        else:
            aql = "FOR expert IN {} FILTER expert.create_time >= '{}' return ".format(
                        self.config.get("arango","kb_expert"), iso_date) 
        #aql = "for com in kb_expert filter com._key=='5ee35b26bccdeda414e32f9d' return com"
        
        end_str_sql = "{'_key':expert._key,'name':expert.properties.name,'research_institution':expert.properties.research_institution,'province':expert.properties.province,'city':expert.properties.city,'area':expert.properties.area}"

        try:
            res = self.arango_db.AQLQuery(aql+end_str_sql, rawResults=True, batchSize=100)
        except AQLFetchError as e:
            '''没有查到数据时，fetch_list会抛出异常'''
            res = []
            logger.warn("Arango专家库没有查到数据",e)

        self.total = len(res)
        logger.info("[{}]，专家知识库查到待处理数据[{}]个".format(process_date, self.total))
        return res


    def process(self, scan_date):

        process_experts = self.query_process_expert(scan_date)

        count = 0
        doc_list = []
        # arango数据库专家信息处理

        for expert in process_experts:
            count += 1
            #logger.info("处理专家关系，专家名=[{}]".format(expert["name"]))
            expert_key = expert["_key"]
            relations = self.process_relations(expert, expert_key)
            if not relations:
                continue
            try:
                #doc = self.kb_expert[expert_key]
                doc = {}
                doc["_key"] = expert_key
                doc["relations"] = relations
                doc["update_time"] = datetime.datetime.today()
                #doc.save()
                doc_list.append(doc)
                self.count_graph_update += 1
                if self.count_graph_update%500 ==0:
                    self.kb_expert.bulkSave(doc_list,onDuplicate="update")
                    doc_list = []
                    logger.info("bulk up前[{}]家专家关系添加完成".format(count))
            except Exception as e:
                logger.error("专家关系添加失败，专家名=[{}]".format(expert["name"]))

            if count % 100 == 0 or count == self.total:
                logger.info("前[{}]家专家关系添加完成".format(count))

        if doc_list:
            self.kb_expert.bulkSave(doc_list,onDuplicate="update")
        logger.info("日期[{}]清洗库共找到专家{}个，arango专家库更新关系{}个".format(
            self.process_date, self.total, self.count_graph_update))

if __name__=="__main__":

    # 最早日期 2019-10-09

    rel = RelationPipeline()
    if len(sys.argv) > 1:
        rel.process(sys.argv[1])
    else:
        rel.process("yesterday")
