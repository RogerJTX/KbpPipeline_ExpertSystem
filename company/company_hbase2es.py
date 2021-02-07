#!/usr/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-05-26 17:11
# Filename     : company_hbase2es.py
# Description  : 企业 hbase信息整合导入ES 
#******************************************************************************

from elasticsearch import Elasticsearch, helpers
from logging.handlers import RotatingFileHandler
from pyArango.theExceptions import AQLFetchError
from pymongo import MongoClient
from urllib.request import urlopen,quote
from pymongo import MongoClient
import pyArango.connection as ArangoDb
import happybase
import configparser
import requests
import pymysql
import json
import copy
import sys
import os
import logging

class TestLink:

    def __init__(self):

        dir_path = os.path.dirname(__file__)
        kbp_path = os.path.dirname(dir_path)
        config_path = os.path.join(kbp_path,"config.ini")
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        #self.init_concpet_classid() # 加载实体类别定义关键词映射

        self.es = Elasticsearch(self.config.get('ES','es_url'))

        self.hbase_pool = happybase.ConnectionPool( host = self.config.get("hbase","host"),
                port = self.config.getint("hbase","port"),
                size = self.config.getint("hbase","pool_size"))

        self.arango_conn = ArangoDb.Connection(\
                        arangoURL=self.config.get("arango","arango_url"),
                        username=self.config.get("arango","user") ,
                        password=self.config.get("arango","passwd")
                    )
        self.agdb_name = self.arango_conn[self.config.get("arango","db")]

        client = MongoClient(self.config.get('mongo','mongo_url'))
        self.mongo_coll = client['recommend_db']['company_score']

    def get_recommend_info(self, com_name, industry_name=''):
        ''' 从mongodb中推荐库查询企业相关产业分类下的推荐数据'''

        recommend = {}
        query = self.mongo_coll.find_one({"company_name":com_name})
        if not query:
            return {}
        if industry_name:
            return query[industry_name]
        return query


    def get_financing_coms(self, com_name):
        '''根据公司名，获取融资信息'''

        company_info = []
        #com_name = '北京唯迈医疗设备有限公司'
        #aql_arango = "FOR company IN kb_financing filter company.name=='%s' return company" % com_name
        #query = self.agdb_name.fetch_list(aql_arango)
        collection = self.agdb_name['kb_financing']
        query = collection.fetchByExample({'name':com_name},batchSize=200)
        if not query:
            return []
        for item in query:
            it = item
            financing = {}
            investor = []
            financing['name'] = item['name']
            investor = item['properties']['investors']
            financing['investor'] = investor
            financing['financing_round'] = item['properties']['finance_round']
            #financing['registered_capital'] = item['price_amount']
            financing['financing_time'] = item['properties']['finance_date']
            financing['financing_amount'] = item['properties']['price_amount']
            company_info.append(company_info)
            #print(item['name'],item['properties']['finance_round'],item['properties']['finance_date'])
        return company_info

    def get_products(self, com_name):

        products = []
        com_name = '北京唯迈医疗设备有限公司'
        aql_arango = "FOR company IN kb_product filter company.relations[0]['relation_type']=='concept_relation/100003' AND company.relations[0]['object_name']=='%s' return company" % com_name
        try:
            query = self.agdb_name.fetch_list(aql_arango)
        except AQLFetchError as e:
            return []
        for item in query:
            product = {}
            product['product_name'] = item['name']
            #product['product_desc'] = itemi['properties']['introduction']
            product['introduction'] = item['properties']['introduction']
            product['product_url'] = item['properties']['product_url']
            products.append(product)
        return products


    def pack_es_item(self, item,uuid):
        ''' 将mysql读取的数据按需组成字典'''

        if not item:
            return {}
        new_item = {
        }
        info = {}
        compet = {}
        product = {}
        shareholder = []
        #new_item['company_name'] = line_dict['name']
        new_item['id'] = uuid
        new_item['province'] = item['province']
        new_item['city'] = item['city']
        new_item['area'] = item['area']
        shareholder = item['shareholder']
        info['企业中文名称'] = item['name']
        info['企业英文名称'] = item['name_en']
        info['注册时间'] = item['establish_date']
        info['经营状态'] = item['register_status']
        info['经营范围'] = item['business_scope']
        info['营业期限'] = item['from_date']+' - '+item['to_date']
        info['所属地区'] = item['area']
        info['注册资本'] = item['reg_capital']
        info['实缴资本'] = item['paid_capital']
        info['人员规模'] = item['employee_size']
        info['法定代表人'] = item['legal_person']
        info['股权结构'] = shareholder
        info['企业地址'] = item['address']
        info['统一社会新用代码'] = item['social_credit_code']
        info['企业类型'] = item['company_type']
        info['核准日期'] = item['approve_date']
        info['成立日期'] = item['establish_date']
        info['参保人数'] = item['insured_number']
        info['工商注册号'] = item['registration_id']
        info['所属行业'] = item['profession']
        info['纳税人识别号'] = item['tax_code']
        info['登记机关'] = item['registration']
        info['组织机构代码'] = item['organization_code']
        new_item['information'] = info
        new_item['tel'] = item['phone']
        new_item['email'] = item['email']
        new_item['loc'] = item['longitude']+','+item['latitude']
        new_item['register_status'] = item['register_status']
        new_item['financeMoney'] = ''
        new_item['baseInfo'] = item['desc']
        new_item['englishName'] = item['name_en']
        new_item['source'] = "量知"
        new_item['website'] = item['website']
        new_item['industry'] = ''
        new_item['type'] = item['company_type']
        new_item['briefIntroduction'] = item['desc']
        new_item['legalRepresentative'] = item['legal_person']
        new_item['logo'] = item['logo_img']
        new_item['url'] = item['url']
        new_item['county'] = item['area']
        new_item['location'] = item['address']
        new_item["society_association"] = []
        new_item["xiehuiOrganize"] = []
        new_item["corporCompany"] = []
        new_item["concatGovenment"] = []
        new_item["highGuanJiuZhi"] = []
        new_item["famousCard"] = []
        new_item["contactPersonalMethod"] = []
        new_item["marks"] = []
        new_item["competitive_company"] = []
        new_item["financing"] = []
        new_item["productResults"] = []
        new_item["product"] = []
        new_item["isRecommend"] = 0
        new_item["together_degree"] = ''
        new_item["migrate_degree"] = ''
        new_item["quality_degree"] = ''
        new_item["recommendReason"] = ''
        new_item["recommendText"] = ''
        new_item["migrate"] = 0.0
        new_item["quality"] = 0.0
        new_item["together"] = 0.0
        new_item["dim_score"] = []
        new_item["avg_score"] = []
        new_item["fenxins"] = []

        financing_coms = self.get_financing_coms(item['name'])
        product_names = self.get_products(item['name'])
        new_item['product'] = product_names
        new_item['financing'] = financing_coms
        new_item['financeMoney'] = str(financing_coms['financing_amount'])
        #new_item.pop('update_time')
        return new_item

    def bulk_action(self, _time_site):

        #logging.info('数据库已连接[%s]，fetch:[%s],正在读取数据库' % (self.tb_name_list[_tb_num],_time_site))
        #endtime = datetime.strptime(_time_site, '%Y-%m-%d %H:%M:%S')
        num = 0
        num_ins = 0
        num_up = 0
        num_del = 0
        ACTIONS = []
        location_company = []
        concept_type_id = b"concept_entity/1001"
        self.hbase_pool = happybase.ConnectionPool( host = self.config.get("hbase","host"),
                        port = self.config.getint("hbase","port"),
                        timeout=None,
                        autoconnect=True,
                        size = self.config.getint("hbase","pool_size"))
        #colletion_name = "kb_company"
        #collection = agdb_name[colletion_name]
        with self.hbase_pool.connection() as hbase_conn:
            self.hbase_entity_table = hbase_conn.table(self.config.get("hbase","entity"))

            #with self.hbase_entity_table.batch(batch_size=200) as bat:
            #filter="SingleColumnValueFilter ('')"
            hbase_rows = self.hbase_entity_table.scan(row_prefix=concept_type_id)
            for key,val in hbase_rows:
                #print('<',key,'>',val)
                name = str(val[b'info:entity_name'],encoding='utf-8')
                type_id = str(val[b'info:entity_type_id'],encoding='utf-8')
                prop = json.loads(str(val[b'info:entity_properties'],encoding='utf-8'))
                rela = json.loads(str(val[b'info:relations'],encoding='utf-8'))
                uuid = str(val[b'info:uuid'],encoding='utf-8')
                com_id = rela[0]['object_id']
                new_item = self.pack_es_item(prop,uuid)
                print(new_item)
                return
        return
        #data_sum = len(res_cor)#sr_cor.count()
        #logging.info('总共处理%s条数据.....' % data_sum)

        for line in res_cor:
            num += 1

            num_ins += 1
            tmp_l = self.pack_es_item(line)
            action = {
                "_index": self.table_info['index_name'],
                "_type": self.table_info['type_name'],
                #"_id": res_max['_id'], #_id 也可以默认生成，不赋值
                "_source":tmp_l
            }
            loc_map = {}
            loc_map['name'] = tmp_l['company_name']
            loc_map['loc'] = tmp_l['location']
            location_company.append(loc_map)
            #if num_ins%1000==0:
            #    print('已经更新%s条！。。。。' % num_ins)
            #doc = {'update':action},{'doc':{'tag':[]}}
            #ACTIONS.append(doc)
            ACTIONS.append(action)
            print(ACTIONS)
            return
            if len(ACTIONS)%1000==0 and ACTIONS:
                success,_ = helpers.bulk(self.es, ACTIONS)
                ACTIONS = []
                print('已经插入%s条' % num_ins)
        if ACTIONS:
            success,_ = helpers.bulk(self.es, ACTIONS)
        try:
            with open('./company_loc.json','w') as dump_f:
                json.dump(location_company,dump_f, ensure_ascii=False, indent=4)
        except Exception as e:
            print('write err:%s' % e)
        #print(ACTIONS)
        print('sum:%s,插入完成%s条!!!!!!' % (num,num_ins))
        #success = self.es.bulk(body=ACTIONS,index=self.table_info['index_name'])
        #print(success)


if __name__ == '__main__':
    test = TestLink()
    test.bulk_action(sys.argv[1])
