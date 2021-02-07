#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-09-03 17:40
# Filename     : invest_institution_graph_vertex.py
# Description  : 投融资知识图谱投资机构的导入
#******************************************************************************

import sys
import logging
import time
import datetime
import json
from tqdm import tqdm
from pyArango.connection import Connection as ArangoConnection
from py2neo import Graph, Node, NodeMatcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## 投资机构arangodb数据库
ARANGO_URL = "http://xxx"
ARANGO_USER = "xxx"
ARANGO_PASSWD = "xxx"
ARANGO_DB = "xxx"
ARANGO_COLLECTION = "xxx"                        ## 投资机构库

# 投融资知识图谱neo4j数据库
NEO4J_URL = "http://xxx"
NEO4J_USER = "xxx"
NEO4J_PASSWD = "xxx"

class InvestInstitutionGraphVertex:
    def __init__(self):
        self.graph = Graph(NEO4J_URL, auth=(NEO4J_USER, NEO4J_PASSWD))
        self.node_matcher = NodeMatcher(self.graph)

    def process(self, date_str):
        process_date = None
        next_date = None

        ## 默认处理昨天采集的数据
        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info("执行采集时间为: {} 的投资机构导入neo4j".format(date_str))
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)
        process_date = datetime.datetime.strftime(process_date, "%Y-%m-%d")
        next_date = datetime.datetime.strftime(next_date, "%Y-%m-%d")

        start_time = time.time()

        source_count = 0              ## arangodb中数量
        target_count = 0              ## 导入的实体数量
        ## 从arangodb获取需要同步的投资机构
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
            
            node = Node("InvestInstitution")
            node["name"]                    = result["name"]
            node["alter_names"]             = result["alter_names"]
            node["id"]                      = result["_id"]
            node["establish_date"]          = result["properties"]["establish_date"]
            node["type"]                    = result["properties"]["type"]
            node["logo"]                    = result["properties"]["logo"]
            node["short_name"]              = result["properties"]["short_name"]
            node["description"]             = result["properties"]["description"]
            node["url"]                     = result["properties"]["url"]
            node["email"]                   = result["properties"]["email"]
            node["phone"]                   = result["properties"]["phone"]
            node["management_capital"]      = result["properties"]["management_capital"]
            node["funds"]                   = json.dumps(result["properties"]["funds"], ensure_ascii=False)
            node["members"]                 = json.dumps(result["properties"]["members"], ensure_ascii=False)

            ## 重复检测
            duplicate = False
            find = self.node_matcher.match("InvestInstitution", name=node["name"])
            if len(find):
                logger.info("数据库内已经有相同结点, {}".format(node["name"]))
            else:
                self.graph.create(node)
                target_count += 1
                logger.info("插入节点: {}".format(node["name"]))

        end_time = time.time()
        logger.info("本次投资机构导入neo4j完成, 耗时: {} 秒".format(int(end_time - start_time)))
        logger.info("其中arangodb有: {} 条数据, 导入neo4j有 {} 条数据".format(source_count, target_count))


if __name__ == "__main__":
    investInstitutionGraphVertex = InvestInstitutionGraphVertex()
    if len(sys.argv) > 1:
        investInstitutionGraphVertex.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")