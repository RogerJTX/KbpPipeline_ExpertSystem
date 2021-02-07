#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-23 06:03
# Filename     : park_clean.py
# Description  : res_kb_park园区信息清洗，主要是产业领域合并为数组格式和地理信息填充；目前人工梳理过的产业园区包括 人工智能、地理信息、光电信息
#******************************************************************************

from urllib.request import urlopen,quote
import json
import logging
import requests
from pymongo import MongoClient
import datetime
import re
import configparser
import sys
from dateutil import parser
import os
from logging.handlers import RotatingFileHandler
import copy

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

def set_log():
    logging.basicConfig(level=logging.INFO) 
    file_log_handler = RotatingFileHandler(os.path.join(dir_path,"log.txt"), maxBytes=1024 * 1024 * 300, backupCount=10)
    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)s - %(levelname)s - %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)

set_log()
logger = logging.getLogger(__name__)

class ParkClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_park = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_park")] 
        self.res_kb_process_park = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_park")] # 园区清洗库
        self.count_ignore = 0
        self.count_insert = 0
        self.count_dupl = 0
        self.count_inner_dupl = 0
     

    def process_str(self, s):
        '''字符串常规处理'''
        string = ""
        if s:
            string = s.replace("（","(").replace("）",")").replace("\r","")
            
        return string

    def process_name(self, s):
        '''标题中字母归一化大写'''
        if s:
            s = s.upper()
            s = self.process_str(s)
        else:
            s = ""
        return s


    def process_str_list(self, p):

        tmp_list = list( filter(lambda x:len(x)>0, re.split("[;,，；\n\t ]",p) ) )
        res_list = [ self.process_str(word) for word in tmp_list]   
        return res_list

    def process_date(self,datetime_str):
        '''
        日期清洗，将日期字符串格式转为 2020-02-03 12:23:34 格式存储
        '''
        if not datetime_str: # like ""
            return ""
        if type(datetime_str)== datetime.datetime: # some like datetime.datetime type
            datetime_str = str(datetime_str)[:19]
            
        tmp = datetime_str.split(" ")
        d = ""
        if len(tmp) == 1:
            tmp1 = re.split("[-./]",tmp[0])

            if len(tmp1) == 3: # like 2020-03-25 or 2020/03/27
                if "-" in tmp[0]:
                    d = datetime.datetime.strptime(tmp[0], "%Y-%m-%d")
                elif "." in tmp[0]:
                    d = datetime.datetime.strptime(tmp[0], "%Y.%m.%d")
                elif "/" in tmp[0]:
                    d = datetime.datetime.strptime(tmp[0], "%Y/%m/%d")

            elif len(tmp1) == 4: # like 2020-03-25-10:3
                d = datetime.datetime.strptime('-'.join(tmp1[:3]) + ' ' + tmp1[3], "%Y-%m-%d %H:%M")
            else:
                d = datetime_str
                
        elif len(tmp) == 2:
            
            if len(re.split("[-/]",tmp[0])) == 3:
                d1 = tmp[0].replace("/","-")
            else:
                d1 = tmp[0]
            if len(tmp[1].split(':')) == 2:
                d = datetime.datetime.strptime(d1 + ' ' + tmp[1], "%Y-%m-%d %H:%M")
            elif len(tmp[1].split(':')) == 3:
                d = datetime.datetime.strptime(d1 + ' ' + tmp[1], "%Y-%m-%d %H:%M:%S")
                
        else:
            d = datetime_str

        return d

            

    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于crawl_date以后的园区数据
        '''

        count = 0
        
        if crawl_date == "yesterday":
            crawl_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif crawl_date == "today":
            crawl_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(crawl_date.split("-")) == 3:
            crawl_date = crawl_date
            
        else:
            raise Exception("无效参数")
        
        iso_date_str = crawl_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        res = self.res_kb_park.find().sort([("name",1)])

        total = res.count()

        batch_data = []
        
        for doc in res:

            logging.info("正在处理园区，园区名=[{}]，ObjectId=[{}]".format(doc["name"], str(doc["_id"])))
            if not doc["name"]:
                self.count_ignore += 1
                logging.info("跳过园区，无园区名称，ObjectId=[{}]".format(str(doc["_id"])))
                count += 1
                continue
 
            clean_park = {
                "_id": str(doc["_id"]),  
                "name": self.process_name(doc["name"]),
                "alter_names":[],
                "address": self.process_str(doc["addrs"]),
                "province":"",
                "city": doc["city"] if "city" in doc else "",
                "area":doc["area"] if "area" in doc else "",
                "industry": self.process_str_list(doc["industry"]),
                "desc": "",
                "companys":[],
                "website": doc["website"] if "website" in doc else "",
                "source":"",
                "url":"",
                "html":"",
                "crawl_time":"",
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }

            addr_parser = AddressParser()
            applicant_addr = addr_parser.parse(clean_park)
            clean_park.update(applicant_addr)
            if "location" in doc and doc["location"]:
                lng, lat = doc["location"].split(",")
                clean_park.update({"longitude":lng, "latitude": lat})

            # 产业领域合并，不批量写
            exist = self.res_kb_process_park.find_one({"name":clean_park["name"]})
            if exist:
                industrys = exist["industry"]
                current_industry = doc["industry"]
                new_industrys = industrys + current_industry
                new_industrys = list(set(new_industrys))
                if new_industrys != industrys:
                    self.res_kb_process_park.update({"_id":exist["_id"]},{"$set":{"industry":new_industrys}})
                    self.count_dupl += 1
                    logger.info("ObjectId=[{}]的产业信息[{}]更新为[{}]".format(exist["_id"], industrys, new_industrys))
            else:
                self.res_kb_process_park.insert_one(clean_park)
                self.count_insert += 1




        logging.info("[{}] 园区数据清洗完毕，共找到园区[{}]条，跳过无园区号的数据[{}]条，合并已存在数据[{}]条，清洗库入库[{}]条".format(
                    crawl_date, total, self.count_ignore, self.count_dupl, self.count_insert) )


class AddressParser(object):
    
    def __init__(self):
        self.ak2 = '5kFxKa49p60ynLGDo6ZpVKw0v7w8nCiI'
        self.ak3 = '64WbEPR9jUwraj1zMoI4DBD58t32kO0n'

        # TODO：行政区划使用的是common库中的数据
        with open(os.path.join(dir_path,'prov_city_area_dict.json'),'r') as load_f:
            self.all_address = json.load(load_f)

    def get_lng_lat(self, address):

        if not address:
            return {}
        to_ak = self.ak2
        url_new = 'http://api.map.baidu.com/geocoding/v3/?address='
        url = url_new
        add = quote(address)
        url_req = url+add+"&output=json&ak="+to_ak
        res  = urlopen(url_req).read().decode()
        temp = json.loads(res)
        map_res = {}
        if 'result' in temp:
            lng = temp['result']['location']['lng']  # 获取经度
            lat = temp['result']['location']['lat']  # 获取纬度
            map_index = str(lat)+','+str(lng)
            map_res = self.get_geocode_info(map_index)
            map_res['longitude'] = lng
            map_res['latitude'] = lat
        return map_res


    def get_geocode_info(self, map_index):
        ''' 根据经纬度‘经度，纬度’获取行政区划'''

        un_ak = self.ak2
        un_url = 'http://api.map.baidu.com/geocoder?output=json&key='+un_ak+'&location=' + str(map_index)
        res_map = {}
        res_map['province'] = ''
        res_map['city'] = ''
        res_map['area'] = ''
        try:
            response = requests.get(un_url)
            un_temp = response.json()

            if 'result' in un_temp:
                res_map['province'] = un_temp['result']['addressComponent']['province']
                res_map['city'] = un_temp['result']['addressComponent']['city']
                res_map['area'] = un_temp['result']['addressComponent']['district']
        except Exception as e:
            logger.error('response:'+response)
            logger.exception("[Error in {}] msg: {}".format(__name__, str(e)))
            pass
        return res_map

    def parse(self, info):
        '''
        根据爬虫企业库的area和address字段解析企业地址
        '''
        parse_out = dict()

        # 匹配所在省、所在市、所在县
        address = ''
        if info["address"]:
            address = info["address"].strip().strip('-')
        if address:
            try:
                comp_info = self.get_lng_lat(address)
            except:
                logger.error("百度接口地址解析出错，地址=[{}]，名称=[{}]".format(address,info["name"]))
                comp_info = {}

            if comp_info:
                parse_out.update(comp_info)
            else:
                addr_res = self.match_addr(address, info['province'])
                res_prov,res_city,res_area = addr_res
                parse_out['province'] = info['province']
                parse_out['city'] = res_city
                parse_out['area'] = res_area
                parse_out['longitude'] = ""
                parse_out['latitude'] = ""
        else:
            parse_out['province'] = info['area']
            parse_out['city'] = ""
            parse_out['area'] = ""
            parse_out['longitude'] = ""
            parse_out['latitude'] = ""

        return parse_out


    def match_addr(self, _address, _area=''):
        all_dict = self.all_address

        find_prov = False
        find_city = False
        find_area = False
        index_prov = ''
        index_city = ''
        index_area = ''
        if _area:
            index_prov = _area
            find_prov = True
            if index_prov in ['北京市','天津市','上海市','重庆市']:
                index_prov = index_prov.strip('市')
        else:
            for prov in all_dict:
                if _address.find(prov)>-1:
                    find_prov = True
                    index_prov = prov
                    break
                elif prov.find('省')>-1 and _address.find(prov.strip('省'))>-1:
                    find_prov = True
                    index_prov = prov
                    break
        if not find_prov:
            if not _area:
                return ('','','')
        for city in all_dict[index_prov]:
            if _address.find(city)>-1:
                find_city = True
                index_city = city
                break
            elif city.find('市')>-1 and _address.find(city.strip('市'))>-1:
                find_city = True
                index_city = city
                break
        if not find_city:
            for n_city in all_dict[index_prov]:
                for n_area in all_dict[index_prov][n_city]:
                    if _address.find(n_area)>-1:
                        find_area = True
                        index_city = n_city
                        index_area = n_area
                        break
                    elif n_area.find('市')>-1 and _address.find(n_area.strip('市'))>-1:
                        find_area = True
                        index_city = n_city
                        index_area = n_area
                        break
        else:
            for area in all_dict[index_prov][index_city]:
                if _address.find(area)>-1:
                    find_area = True
                    index_area = area
                    break
                elif area.find('市')>-1 and _address.find(area.strip('市'))>-1:
                    find_area = True
                    index_area = area
                    break
        return (index_prov,index_city,index_area)



if __name__ == "__main__":

    # 园区最早爬虫日期为 2019-05-24
    
	cleaner = ParkClean()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")

