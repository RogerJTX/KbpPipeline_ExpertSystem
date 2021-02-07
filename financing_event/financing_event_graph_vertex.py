#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-08-19 10:51
# Filename     : financing_event_graph_vertex.py
# Description  : 投融资知识图谱的结点导入, 包括企业和未匹配上的投资机构
# 投融资事件中结点导入neo4j, 构建知识图谱
#******************************************************************************

import sys
import logging
import time
import datetime
from tqdm import tqdm
from pyArango.connection import Connection as ArangoConnection
from py2neo import Graph, Node, NodeMatcher, Relationship

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## 投融资事件arangodb数据库
ARANGO_URL = "http://xxx"
ARANGO_USER = "xxx"
ARANGO_PASSWD = "xxx"
ARANGO_DB = "xxx"
COMPANY_COLLECTION = "xxx"                              ## 企业库
ARANGO_COLLECTION = "xxx"                        ## 投融资事件库

# 投融资知识图谱neo4j数据库
NEO4J_URL = "http://xxx"
NEO4J_USER = "xxx"
NEO4J_PASSWD = "xxx"


class FinancingEventGraphVertex:

    def __init__(self):
        self.graph = Graph(NEO4J_URL, username=NEO4J_USER, password=NEO4J_PASSWD)
        self.node_matcher = NodeMatcher(self.graph)
        self.arango_connector = ArangoConnection(arangoURL=ARANGO_URL,
                                                 username=ARANGO_USER,
                                                 password=ARANGO_PASSWD)

        self.arango_db = self.arango_connector[ARANGO_DB]

    ## 根据id获取企业库信息
    def fetch_company(self, company_id):
        collection = self.arango_db[COMPANY_COLLECTION]
        company_id = company_id.split("/")[1]
        return collection[company_id]


    def process(self, date_str):
        process_date = None
        next_date = None

        ## 默认处理昨天采集的数据
        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info("执行采集时间为: {} 的投融资事件中的节点导入neo4j".format(date_str))
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)
        process_date = datetime.datetime.strftime(process_date, "%Y-%m-%d")
        next_date = datetime.datetime.strftime(next_date, "%Y-%m-%d")

        start_time = time.time()

        source_count = 0                        ## 事件库数量
        node_count = 0                          ## 导入的实体数量
        ## 从arangodb获取需要同步的事件

        aql = "for x in {} FILTER x.update_time >= \"{}\" AND x.update_time <= \"{}\" RETURN x".format(ARANGO_COLLECTION, process_date, next_date)
        results = []
        try:
            results = self.arango_db.AQLQuery(aql)
        except Exception as e:
            logger.error("查询arangodb错误: " + str(e))

        ## 从arangodb中导入实体节点，如果arangodb中也不存在, 那就只有实体节点的名称
        for result in tqdm(results):
            source_count += 1
            ## 实体导入
            for entity in result["entities"]:
                ## 创建标签节点
                label = "InvestInstitution"
                if entity["type"] == "company":
                    label = "Company"
                
                node = Node(label)
                if not entity["externalReference"]:             ## 企业库、投资机构库中没有，需要创建节点
                    node["name"] = entity["name"]
                elif label == "Company":                        ## 企业节点需要跟随事件导入
                    company = self.fetch_company(entity["externalReference"]["id"])
                    node["name"]                            = company["name"]
                    node["alter_names"]                     = company["alter_names"]
                    node["address"]                         = company["properties"]["address"]
                    node["business_scope"]                  = company["properties"]["business_scope"]
                    node["description"]                     = company["properties"]["desc"]
                    node["email"]                           = company["properties"]["email"]
                    node["phone"]                           = company["properties"]["phone"]
                    node["logo"]                            = company["properties"]["logo_img"]
                    node["name_en"]                         = company["properties"]["name_en"]
                    node["province"]                        = company["properties"]["province"]
                    node["city"]                            = company["properties"]["city"]
                    node["profession"]                      = company["properties"]["profession"]
                    node["webiste"]                         = company["properties"]["website"]
                    node["legal_person"]                    = company["properties"]["legal_person"]
                ## 投资机构之前已经导入
                else:
                    continue

                ## 检查是否有重复节点
                duplicate = False
                companys = self.node_matcher.match('Company', name=node["name"])
                if len(companys):
                    duplicate = True
                else:
                    investors = self.node_matcher.match("InvestInstitution", name=node["name"])
                    if len(investors):
                        duplicate = True
                
                if not duplicate:
                    self.graph.create(node)
                    logger.info("插入节点: {}, 类型: {}".format(node["name"], label))
                else:
                    logger.info("数据库内已有相同节点: {}".format(node["name"]))


if __name__ == "__main__":
    financingEventGraphVertex = FinancingEventGraphVertex()
    if len(sys.argv) > 1:
        financingEventGraphVertex.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")

