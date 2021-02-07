#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-08-18 15:19
# Filename     : financing_event_graph_relation.py
# Description  : 事件关系导入neo4j
# 投融资事件关系导入neo4j, 构建知识图谱
#******************************************************************************

import sys
import logging
import time
import datetime
from pyArango.connection import Connection as ArangoConnection
from py2neo import Graph, Relationship, NodeMatcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## 投融资事件arangodb数据库
ARANGO_URL = "http://xxx"
ARANGO_USER = "xxx"
ARANGO_PASSWD = "xxx"
ARANGO_DB = "xxx"
ARANGO_COLLECTION = "xxx"
## 投融资事件库

# 投融资知识图谱neo4j数据库
NEO4J_URL = "http://xxx"
NEO4J_USER = "xxx"
NEO4J_PASSWD = "xxx"


class FinancingEventGraphRelation:

    def __init__(self):
        self.graph = Graph(NEO4J_URL, username=NEO4J_USER, password=NEO4J_PASSWD)
        self.node_matcher = NodeMatcher(self.graph)


    def find_node(self, name):
        ## 首先查找投资机构
        node = None
        node = self.node_matcher.match("InvestInstitution", name=name).first()
        ## 再查找企业
        if not node:
            node = self.node_matcher.match("Company", name=name).first()
        return node


    def process(self, date_str):
        process_date = None
        next_date = None

        ## 默认处理昨天采集的数据
        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info("执行采集时间为: {} 的投融资事件导入neo4j".format(date_str))
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)
        process_date = datetime.datetime.strftime(process_date, "%Y-%m-%d")
        next_date = datetime.datetime.strftime(next_date, "%Y-%m-%d")

        start_time = time.time()
        source_count = 0                            ## 事件库数量
        relationship_count = 0                      ## 导入图谱关系数量

        ## 从arangodb获取需要同步的事件
        arango_connector = ArangoConnection(arangoURL=ARANGO_URL,
                                            username=ARANGO_USER,
                                            password=ARANGO_PASSWD)

        arango_db = arango_connector[ARANGO_DB]
        aql = "for x in {} FILTER x.update_time >= \"{}\" AND x.update_time <= \"{}\" RETURN x".format(ARANGO_COLLECTION, process_date, next_date)
        results = []
        try:
            results = arango_db.AQLQuery(aql)
        except Exception as e:
            logger.error("查询arangodb错误: " + str(e))

        ## 导入投融资事件关系, 在vertex脚本中就已经导入实体节点
        for result in results:
            source_count += 1

            ## 关系导入
            for relation in result["relations"]:
                start = relation["start"]
                end = relation["end"]
                for entity in result["entities"]:
                    if entity["name"] == start and entity["externalReference"]:
                        start = entity["externalReference"]["name"]
                    if entity["name"] == end and entity["externalReference"]:
                        end = entity["externalReference"]["name"]
                        
                start_node = self.find_node(start)
                end_node = self.find_node(end)
                relation_type = relation["relation_type"]
                relation_name = relation["relation_name"]

                if start_node and end_node:
                    relationship = Relationship(start_node, relation_name, end_node)
                    relationship["id"]                      = result["_id"]
                    relationship["url"]                     = result["properties"]["url"]
                    relationship["source"]                  = result["properties"]["source"]
                    relationship["financing_round"]         = result["properties"]["financing_round"]
                    relationship["financing_date"]          = result["properties"]["financing_date"]
                    relationship["raw_financing_money"]     = result["properties"]["raw_financing_money"]
                    relationship["financing_money"]         = result["properties"]["financing_money"]
                    relationship["financing_proportion"]    = result["properties"]["financing_proportion"]
                    relationship["industry"]                = result["properties"]["industry"]
                    relationship["news"]                    = result["properties"]["news"]

                    self.graph.create(relationship)
                    relationship_count += 1
                else:
                    logger.error("未找到关系节点, 起始节点: {} - {} ^^^, 终止节点: {} - {}".format(relation["start"], start, relation["end"], end))
                
        end_time = time.time()
        logger.info("关系导入结束, 共有事件 {} 条, 导入neo4j关系有 {} 条, 共耗时".format(source_count, relationship_count, int(end_time - start_time)))


if __name__ == "__main__":
    financingEventGraphRelation = FinancingEventGraphRelation()
    if len(sys.argv) > 1:
        financingEventGraphRelation.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")


