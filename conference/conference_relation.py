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
        self.kb_conference = self.arango_db[self.config.get("arango","kb_conference")]
        self.conference_url = self.config.get("url","conference_classifier")
        self._init_division_schema() # init division_schema from mysql
        self._init_industry_schema()
        self._init_conference_tag_schema()
        self.count_graph_update = 0 # arango更新关系数据数量
        self.total = 0 # 处理日期总共需要添加关系的数量

    def _init_conference_tag_schema(self):
        # 会议分类类型加载
        self.conference_tags_schema = {}
        sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                            user = self.config.get("mysql","user") ,
                            passwd = self.config.get("mysql","passwd"),
                            port = self.config.getint("mysql","port") ,
                            db = self.config.get("mysql","db"),
                            charset = "utf8" )
        sql_cur = sql_conn.cursor() 
        # 查询企业相关的标签信息
        sql_state = self.config.get("mysql","conference_tags_query").replace("eq","=")
        sql_cur.execute(sql_state)
        datas = sql_cur.fetchall()
        for data in datas:
            tag_name, tag_type, tag_id = data
            tag = {
                "type":tag_type,
                "name":tag_name,
                "id": tag_id
            }
            self.conference_tags_schema[tag_id] = tag

        sql_cur.close()
        sql_conn.close()


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


    def query_datas(self, process_date):
        
        if process_date == "yesterday":
            process_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif process_date == "today":
            process_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(process_date.split("-")) == 3:
            process_date = process_date
            
        else:
            raise Exception("无效参数")
        
        self.process_date = process_date
        iso_date_str = process_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)

        aql = "FOR conference IN {} FILTER conference.create_time >= '{}' SORT kb_conference.create_time return conference".format(
                        self.config.get("arango","kb_conference"), iso_date) 
        #aql = "FOR conference IN {} FILTER conference._key=='5ea0f3c9bf45745dcf5d38b5' return conference".format(self.config.get("arango","kb_conference"))
        
        try:
            res = self.arango_db.fetch_list(aql)
        except AQLFetchError as e:
            '''没有查到数据时，fetch_list会抛出异常'''
            res = []
            logger.warn("Arango会议库没有查到数据",e)

        self.total = len(res)           
        logger.info("[{}]，会议知识库查到待处理数据[{}]个".format(process_date, self.total))
        return res


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
                city = city + "市"
            div_rel.append(self.division_schema[city])

        if area and area in self.division_schema.keys():
            div_rel.append(self.division_schema[area])

        return div_rel

        
    def process_industry_rel(self,properties):
        '''
        会议分类标签ID化添加
        '''
        industry_rel = []
        conference_tag = []

        industry_field_tags = []

        post_data = {
            "conference_list": [ {
                "title": properties["name"],
                "content": properties["desc"]
            } ],
        
        }
        try:
            res = requests.post(self.conference_url, data=json.dumps(post_data))

            if res.status_code == 200:
                classify_res = res.json().get("body")[0]
                # 验证是否返回对应字段的分类值
                if "domain" in classify_res and classify_res["domain"]:
                    industry_field_tags.append(classify_res["domain"])

                if "industry" in classify_res and classify_res["industry"]:
                    industry_field_tags.append(classify_res["industry"])

                if "type" in classify_res and classify_res["type"]:
                    type_id = self.conference_tags_schema[classify_res["type"]]
                    conference_tag.append(type_id)


        except Exception as e:
            logging.error("获取会议分类结果失败，会议=[{}]，接口=[{}]".format(properties["name"], self.conference_url),e)

        #logger.info("会议分类结果=[{}]".format(industry_field_tags))

        for industry_id in industry_field_tags:
            for industry_node in self.get_related_industry_tags(industry_id):
                if industry_node not in industry_rel:
                    industry_rel.append(industry_node)

        return industry_rel, conference_tag


        

    def process_relations(self, properties):
        '''
        添加关系：行政区域、产业类别、渠道信息
        '''
        relations = []
        division_rel = self.process_division_rel(properties)
        relations.extend(division_rel)

        industry_rel, conference_tag = self.process_industry_rel(properties)
        relations.extend(industry_rel)
	
        return relations, conference_tag

            
    
    def process(self, scan_date):

        datas = self.query_datas(scan_date)

        count = 0

        # arango数据库企业信息处理

        for data in datas:

            #logger.info("处理会议关系，会议名=[{}]".format(data["name"]))
            conference_key = data["_key"]
            #if data["properties"]['city'] not in ["北京","上海","重庆","天津"]:
            #    continue
            relations, conference_tag = self.process_relations(data["properties"])
            # 删除无分类属性的会议？
            # if not relations:
            #     try:
            #         doc = self.kb_conference[conference_key]
            #         doc.delete()
            #         self.count_graph_update += 1
            #         logger.info("会议被移除，会议ID=[{}]，原因：该会议不属于任何产业领域".format(conference_key))
            #         continue
            #     except Exception as e:
            #         logger.error("会议数据移除失败，会议ID=[{}]".format(conference_key))

            try:
                doc = self.kb_conference[conference_key]
                doc["relations"] = relations
                doc["tags"] = conference_tag
                doc["update_time"] = datetime.datetime.today()
                doc.save()
                #print('relations:',doc['relations'])
                self.count_graph_update += 1
            except Exception as e:
                logger.error("会议标签、关系添加失败，会议名=[{}]".format(data["name"]))


            count += 1

            if count % 500 == 0 or count == self.total:
                logger.info("前[{}]个会议标签、关系添加完成".format(count))

        logger.info("日期[{}]清洗库共找到会议{}个，arango会议库更新关系{}个".format(
            self.process_date, self.total, self.count_graph_update))

if __name__=="__main__":

    # 最早日期 2019-06-03

    rel = RelationPipeline()
    if len(sys.argv) > 1:
        rel.process(sys.argv[1])
    else:
        rel.process("yesterday")


        

                    









