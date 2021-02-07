#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-08-17 15:37
# Filename     : financing_kbp.py
# Description  : 融资信息导入arangodb, 并整合为事件库
# 主要关联相关实体: 投资机构、融资公司
#******************************************************************************

import sys
import logging
import time
import datetime
from pymongo import MongoClient
from pyArango.connection import Connection as ArangoConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## 投融资事件mongodb数据库
MONGO_HOST = "xxx"
MONGO_PORT = 0
MONGO_USER = "xxx"
MONGO_PASSWD = "xxx"
MONGO_DB = "xxx"
MONGO_COLLECTION = "xxx"

## 导入目标arangodb数据库
ARANGO_URL = "xxx"
ARANGO_USER = "xxx"
ARANGO_PASSWD = "xxx"
ARANGO_DB = "xxx"
ARANGO_EVENT_COLLECTION = "xxx"                        ## 投融资事件库
ARANGO_COMPANY_COLLECTION = "xxx"                            ## 企业库
ARANGO_INVEST_INSTITUTION_COLLECTION = "xxx"      ## 投资机构库

class FinancingKBP:

    def __init__(self):
        arango_connector = ArangoConnection(arangoURL=ARANGO_URL,
                                            username=ARANGO_USER,
                                            password=ARANGO_PASSWD)
        self.arango_db = arango_connector[ARANGO_DB]


    ## 融资企业和投资机构与量知产业知识中心实体库的链接
    def entity_link(self, name, type=None):

        externalReference = None
        try:
            ## 匹配投资机构
            if not type or type == "invest_institution":
                aql = "FOR x in {} FILTER x.name == \"{}\" OR \"{}\" IN x.alter_names RETURN x".format(ARANGO_INVEST_INSTITUTION_COLLECTION, name, name)
                results = self.arango_db.AQLQuery(aql, rawResults=True, batchSize=1)
                if len(results):
                    result = results[0]
                    externalReference = {
                        "name": result["name"],
                        "id": result["_id"],
                        "conceptName": "投资机构",
                        "conceptId": "concept_entity/1018"
                    }
                    return externalReference, "invest_institution"          ## 匹配到投资机构

            ## 匹配企业
            if not type or type == "company":
                aql = "FOR x in {} FILTER x.name == \"{}\" OR \"{}\" IN x.alter_names RETURN x".format(ARANGO_COMPANY_COLLECTION, name, name)
                results = self.arango_db.AQLQuery(aql, rawResults=True, batchSize=1)
                if len(results):
                    result = results[0]
                    externalReference = {
                        "name": result["name"],
                        "id": result["_id"],
                        "conceptName": "企业",
                        "conceptId": "concept_entity/1001"
                    }
                    return externalReference, "company"          ## 匹配到企业
        except Exception as e:
            logger.error(str(e))

        return externalReference, type


    def process(self, date_str):
        process_date = None
        next_date = None

        ## 默认处理昨天采集的数据
        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info("执行采集时间为: {} 的投融资事件kbp".format(date_str))
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)
  
        start_time = time.time()

        ## 从mongodb获取需要处理的投融资事件
        mongo_client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
        admin_db = mongo_client["admin"]
        admin_db.authenticate(MONGO_USER, MONGO_PASSWD)
        mongo_collection = mongo_client[MONGO_DB][MONGO_COLLECTION]
        
        results = mongo_collection.find({"crawl_time": {"$gte": process_date, "$lte": next_date}}, no_cursor_timeout=True, batch_size=50)
        # results = mongo_collection.find(no_cursor_timeout=True, batch_size=100)
        source_count = 0
        target_count = 0

        for result in results:
            source_count += 1
            ## 组装事件库数据形式
            doc = {}
            doc["_key"] = str(result["_id"])
            doc["event_type"] = "投融资"                                    ## 事件类型
            doc["event_date"] = result["financing_date"]                    ## 事件日期
            doc["name"] = result["company"]                                 ## 事件主体
            doc["create_time"] = result["crawl_time"]                       ## 采集时间
            doc["update_time"] = result["crawl_time"]                       ## 更新时间, 初次更新时间为采集时间, 后续更新时间为修改时间
            doc["properties"] = {
                "url":                  result["url"],                      ## url
                "source":               result["source"],                   ## 来源
                "financing_round":      result["financing_round"],          ## 投资轮次
                "financing_money":      result["financing_money"],          ## 融资金额
                "raw_financing_money":  result["raw_financing_money"],      ## 融资金额原描述
                "financing_proportion": result["financing_proportion"],     ## 融资占比
                "financing_date":       result["financing_date"],           ## 融资日期
                "industry":             result["industry"],                 ## 涉及行业
                "news":                 result["news"]                      ## 详细描述
            }

            ## 实体信息, 融资公司
            doc["entities"] = []
            externalReference, type = self.entity_link(result["company"], "company")
            if not externalReference:
                logger.error("未匹配到相关实体, 事件id: {}, name: {}".format(str(result["_id"]), result["company"]))

            doc["entities"].append({
                "name": result["company"],
                "type": type,
                "externalReference": externalReference
            })

            ## 投资机构
            for investor in result["investors"]:
                externalReference, type = self.entity_link(investor, "invest_institution")
                if not externalReference:
                    logger.error("未匹配到相关实体, 事件id: {}, name: {}".format(str(result["_id"]), investor))
                
                doc["entities"].append({
                    "name": investor,
                    "type": type,
                    "externalReference": externalReference
                })

            ## relations, 目前只有投资关系
            doc["relations"] = []
            for investor in result["investors"]:
                relation = {
                    "relation_name": "投资",
                    "relation_type": "单向",
                    "start": investor,
                    "end": result["company"]
                }
                doc["relations"].append(relation)

            ## 导入arangodb, 覆盖重复数据
            try:
                ## 删除同id数据
                arango_collection = self.arango_db[ARANGO_EVENT_COLLECTION]
                query = arango_collection.fetchByExample({"_key": doc["_key"]}, batchSize=1)
                for q in query:
                    q.delete()

                arango_collection.createDocument(doc).save()
                target_count += 1
            except Exception as e:
                logger.error("导入arangodb错误, id: {}".format(doc["_key"]))

        end_time = time.time()
        logger.info("本次投融资事件kbp完成, 耗时: {} 秒".format(int(end_time - start_time)))
        logger.info("其中清洗库有: {} 条数据, 导入arangodb有 {} 条数据".format(source_count, target_count))


if __name__ == "__main__":
    financingKBP = FinancingKBP()
    if len(sys.argv) > 1:
        financingKBP.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")
