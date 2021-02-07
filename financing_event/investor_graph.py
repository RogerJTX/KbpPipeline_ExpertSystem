#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-09-04 13:52
# Filename     : investor_graph.py
# Description  : 投资人导入neo4j, 创建节点和关系边
#******************************************************************************

import sys
import logging
import time
import datetime
import json
from tqdm import tqdm
from pyArango.connection import Connection as ArangoConnection
from py2neo import Graph, Node, NodeMatcher, Relationship

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## 投资人arangodb数据库
ARANGO_URL = "http://xxx"
ARANGO_USER = "xxx"
ARANGO_PASSWD = "xxx"
ARANGO_DB = "xxx"
ARANGO_COLLECTION = "xxx"                        ## 投资人

# 投融资知识图谱neo4j数据库
NEO4J_URL = "http://xxx"
NEO4J_USER = "xxx"
NEO4J_PASSWD = "xxx"

class InvestorGraph:
    def __init__(self):
        self.graph = Graph(NEO4J_URL, auth=(NEO4J_USER, NEO4J_PASSWD))
        self.node_matcher = NodeMatcher(self.graph)


    def find_node(self, name):
        ## 首先查找投资机构
        node = None
        node = self.node_matcher.match("InvestInstitution", name=name).first()
        return node


    def process(self, date_str):
        process_date = None
        next_date = None

        ## 默认处理昨天采集的数据
        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info("执行采集时间为: {} 的投资人导入neo4j".format(date_str))
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)
        process_date = datetime.datetime.strftime(process_date, "%Y-%m-%d")
        next_date = datetime.datetime.strftime(next_date, "%Y-%m-%d")

        start_time = time.time()

        source_count = 0              ## arangodb中数量
        vertex_count = 0              ## 导入的节点数量
        relation_count = 0            ## 导入的关系数量
        ## 从arangodb获取需要同步的投资人
        arango_connector = ArangoConnection(arangoURL=ARANGO_URL,
                                            username=ARANGO_USER,
                                            password=ARANGO_PASSWD)

        arango_db = arango_connector[ARANGO_DB]
        aql = "for x in {} FILTER x.update_time >= \"{}\" AND x.update_time <= \"{}\" RETURN x".format(ARANGO_COLLECTION, process_date, next_date)
        # aql = "for x in {} RETURN x".format(ARANGO_COLLECTION)
        results = []
        try:
            results = arango_db.AQLQuery(aql)
        except Exception as e:
            logger.error("查询arangodb错误: " + str(e))

        for result in results:
            source_count += 1

            node = Node("Investor")
            node["name"] = result["name"]
            node["id"] = result["_id"]
            node["invest_institution"] = result["properties"]["invest_institution"]
            node["position"] = result["properties"]["position"]
            node["resume"] = result["properties"]["resume"]
            node["phone"] = result["properties"]["phone"]
            node["email"] = result["properties"]["email"]
            node["url"] = result["properties"]["url"]
            node["invest_industry"] = result["properties"]["invest_industry"]
            node["invest_round"] = result["properties"]["invest_round"]

             ## 重复检测
            duplicate = False
            find = self.node_matcher.match("Investor", name=node["name"], invest_institution=node["invest_institution"])
            if len(find):
                logger.info("数据库内已经有相同结点, {}".format(node["name"]))
            else:
                self.graph.create(node)
                vertex_count += 1
                logger.info("插入节点: {}".format(node["name"]))

                ## 导入关系
                for relation in result["relations"]:
                    end = self.find_node(relation["end"])
                    if end:
                        r = Relationship(node, relation["relation_name"], end)
                        r["position"] = node["position"]
                        self.graph.create(r)
                        logger.info("插入关系: {} -[{}]-> {}".format(node["name"], relation["relation_name"], end["name"]))
                        relation_count += 1
                    else:
                        logger.info("未找到投资机构: {}".format(relation["end"]))

        end_time = time.time()
        logger.info("本次投资人导入neo4j完成, 耗时: {} 秒".format(int(end_time - start_time)))
        logger.info("其中arangodb有: {} 条数据, 导入neo4j投资人节点有 {} 条数据, 关系有 {} 条数据".format(source_count, vertex_count, relation_count))


if __name__ == "__main__":
    investorGraph = InvestorGraph()
    if len(sys.argv) > 1:
        investorGraph.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")