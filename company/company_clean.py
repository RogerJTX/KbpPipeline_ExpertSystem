#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-09 10:55
# Filename     : company_clean.py
# Description  : res_kb_company企业信息清洗，主要是添加企业的label、中英文别称、曾用名数组化和省市区经纬度地理信息
# Update_log   :
# 2020.10.26 企业别称不自动处理添加，统一由人工校验后手动添加；
#******************************************************************************
from urllib.request import urlopen,quote
import json
import logging
from logging.handlers import RotatingFileHandler
import requests
from pymongo import MongoClient
import datetime
import re
import configparser
import sys
from dateutil import parser
import uuid
import pymysql
import os

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


class CompanyClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_company = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_company")] # 企业信息，爬虫库
        self.res_kb_process_company = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_company")] # 企业信息，清洗库
        # 企业tags标签，人工整理
        self.sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                user = self.config.get("mysql","user"),
                passwd = self.config.get("mysql","passwd"),
                port = self.config.getint("mysql","port") ,
                db = self.config.get("mysql","db"),
                charset = self.config.get("mysql","charset") )
        self.sql_cur = self.sql_conn.cursor() 
        self.address_parser = AddressParser()
        self.alter_url = self.config.get("url","alter_name")
        self._init_currency_schema()
        self._init_company_type_schema()
        self._init_f_round_schema() # 融资轮次归一
        self.count_dupl_company = 0
        self.count_insert = 0
        self.count_inner_dupl = 0

    def _init_f_round_schema(self):

        self.f_round_schema = {}
        self.f_round_sort = {}

        sql_conn = pymysql.connect( host = "xxx" ,
                user = "xxx",
                passwd = "xxx",
                port = 0 ,
                db = "xxx",
                charset = "xxx" )
        sql_cur = sql_conn.cursor() 
        currency_sql = "select name,alter_names,sequence from res_invest_round where sequence is not null"
        sql_cur.execute(currency_sql)
        for c in sql_cur.fetchall():
            f_round = [c[0]]
            self.f_round_sort[c[0]] = c[2]
            if c[1]:
                f_round.extend(c[1].split("|")) 
            self.f_round_schema[c[0]] = f_round

        sql_cur.close()
        sql_conn.close()

    def _init_currency_schema(self):

        self.currency_world_code = {}
        currency_sql = "select name,alter_names,name_en from res_currency"
        self.sql_cur.execute(currency_sql)
        for c in self.sql_cur.fetchall():
            keywords = [c[0],c[2]]
            keywords.extend(c[1].split("|")) 
            keywords = list(set(keywords))
            self.currency_world_code[c[0]] = keywords

    def _init_company_type_schema(self):

        self.company_types = {}
        sql = "select name,alter_names from res_company_type"
        self.sql_cur.execute(sql)
        for c in self.sql_cur.fetchall():
            keywords = c[1].split("|")
            self.company_types[c[0]] = keywords


    def process_company_type(self,doc):
        '''公司类型mappings'''
        company_type = doc["company_type"]
        for t in self.company_types:
            if company_type in self.company_types[t]:
                return t
        return ""
        

    def process_f_round(self, labels):

        res_key = ''
        if labels:
            f_rounds = [f_round.upper() for f_round in labels]
        else:
            return res_key
        rount_num = -1
        for key in self.f_round_schema:
            for word in self.f_round_schema[key]:
                if word in f_rounds:
                    if rount_num < self.f_round_sort[key]:
                        rount_num = self.f_round_sort[key]
                        res_key = key

        # 如果没有匹配到，返回空
        return res_key

    def process_alter_name(self, doc):
        '''
        根据公司全称生成别称及英文别称
        '''

        alter_names = []

        # 可能的英文别称
        if doc["description"]:
            alter_names.extend([ alter.lower() for alter in re.findall("^[A-Za-z]+",doc["description"])])
        if alter_names:
            # 去除从介绍中抽取的一个字母的数据
            alter_names = list(filter(lambda x:len(x)>1,alter_names))

        # 中文别称生成
        name = doc["company_name"].replace('（','(').replace('）',')')
        post_data = {
            "full_name": name
        }
        try:
            res = requests.post(self.alter_url, data=json.dumps(post_data))

            if res.status_code == 200:
                alter_names.extend(res.json().get("body"))
        except Exception as e:
            logger.error("公司中文别称生成异常，公司名=[{}]".format(name),e)

        return alter_names


    def process_used_name(self,doc):
        
        used_names = []

        used_name = doc["used_name"]
        if not used_name:
            return used_names

        elif used_name == '-':
            return used_names

        elif len(used_name)>1:
            used_names.extend(list(map(lambda x: x.strip(), re.split(" ", used_name))))

        else:
            logger.warning("used_names格式异常，used_names=[{}]，公司名=[{}]".format(used_name, doc["name"]))

        # remove blank string, such as ["123","","456"]
        used_names = list(filter(lambda x: len(x)>0, used_names))
        return used_names

    def process_tags(self, doc):
        '''
        根据人工整理的公司标签表添加tags
        '''
        tags = []
        company_name = doc["company_name"].replace("（","(").replace("）",")")
        if company_name in self.company_tags_schema:
            tags = self.company_tags_schema[company_name]
        return tags

    def process_labels(self,doc):
        '''
        爬虫爬取时带的标签标记清洗
        '''
        labels = []
        label = doc["label"]
        if label:
            if "" in label:
                label.remove("")
            labels = list(map(lambda x: re.sub("[| ]","",x),label))
        return labels

    def process_publish_time(self, _doc_str):
        ''' '''

        if not _doc_str or not _doc_str.strip():
            return None
        pat = re.compile(r'(\d{4})[年-](\d{2})[月-](\d{2})[日 ]?')
        res = pat.search(_doc_str.strip())

        if not res:
            return None
        return res.group(1)+'-'+res.group(2)+'-'+res.group(3)

    def process_str(self, s):
        string = ""
        if s:
            string = re.sub("[ \t\r]","", s.strip()).replace("（","(").replace("）",")")
            string = string.replace("暂无","")
        
        return string

    def process_business_term(self, doc):
        business_terms = {}
        time_term = doc["business_term"]
        if time_term:
            seg_str = ['至']
            for seg in seg_str:
                if time_term.find(seg)>-1:
                    from_t = ''
                    to_t = ''
                    from_t,to_t = time_term.split(seg)
                    from_t, to_t = from_t.strip(), to_t.strip()
                    if not self.is_date_type(from_t):
                        from_t = None
                    if not self.is_date_type(to_t):
                        to_t = None
                    business_terms["from_date"] = from_t
                    business_terms["to_date"] = to_t
        return business_terms

    def is_date_type(self, _time_str):
        ''' 判断是否符合日期格式'''

        pat = re.search(r"^(\d{4}-\d{1,2}-\d{1,2})$",_time_str)
        if pat:
            return True
        return False

    def process_capital(self, doc):
        '''注册金额和实付金额 货币清洗'''

        capital = {
            "reg_capital": self.get_capital_amount(doc["registered_capital"]),
            "reg_capital_currency": self.get_currency_type(doc["registered_capital"]),
            "paid_capital": self.get_capital_amount(doc["paid_capital"]),
            "paid_capital_currency": self.get_currency_type(doc["paid_capital"])
        }

        return capital

    def get_capital_amount(self, _doc_str):

        if not _doc_str or not _doc_str.strip().strip('-'):
            return None
        currency_num = None
        han_to_num = {
            "千万":10000000,    
            "百万":1000000,    
            "十万":100000,    
            "万"  :10000,
            "千亿":100000000000,    
            "百亿":10000000000,    
            "十亿":1000000000,    
            "亿"  :100000000
        }
        tmp_currency = _doc_str.strip().replace(',','')
        pat = re.compile(r'\d+[\.]?\d+')#取有效数字，可含小数点
        res = pat.search(tmp_currency)

        if not res:
            for mon_key in han_to_num:
                if tmp_currency.find(mon_key)>-1:
                    currency_num = han_to_num[mon_key]
                    break
            return currency_num
        if res.group(0):
            num = res.group(0)
            if '.' == num[0]:
                return None
        currency_num = float(res.group(0))
        for mon_k in han_to_num:
            if tmp_currency.find(mon_k)>-1:
                currency_num = currency_num*(han_to_num[mon_k])
                break
        return float(currency_num)

    def get_currency_type(self, str_doc):

        if not str_doc or not str_doc.strip().strip('-'):
            return None

        # 常用货币    
        currency_type_key = {
            "人民币元":['￥','人民币','CNY'],
            "美元":['美元','美金',"美币",'USD'],
            "香港元":['香港元','港元','HKD','港币'],
            "英镑":['英镑','GBP'],
            "欧元":['欧元','EUR'],
            "澳大利亚":['澳大利亚元','澳元',"澳币","AUD"],
            "日元":['日元','日币',"JPY"]
        }


        for key in currency_type_key:
            for k_i in currency_type_key[key]:
                if str_doc.find(k_i)>-1:
                    return key
        
        # 数据库加载其他货币
        for cur_code in self.currency_world_code:
            c_key = self.currency_world_code[cur_code]
            for key in c_key:
                if str_doc.find(key)>-1:
                    return cur_code
        
        #if str_doc.find('元')>-1:
        #    return 156
        return None

    def process_leader_position(self, doc):
        ''' 获取高管职业列表'''

        leaders = doc["leader"]

        lead_keys = [
            ["代表","首席代表","一般代表"],
            ["负责人"],
            ["副董事长","执行董事","独立董事","董事长","董事"],
            ["副总经理","总经理","经理"],
            ["监事会主席","监事主席","职工监事","监事长","监事"]
        ]

        for leader in leaders:
            if not leader:
                continue
            pos = leader["position"]
            leader_pos = []    
            for lay in range(len(lead_keys)):
                for key in lead_keys[lay]:
                    if key in pos:
                        leader_pos.append(key)
                        break
            leader["position"] = leader_pos

        return leaders

    def process_register_status(self, doc):
        '''营业状态：吊销和存续状态归一'''

        reg_status = doc["register_status"]
        if reg_status:
            if "吊销" in reg_status:
                reg_status = "吊销"
            elif re.search("开业|在业|存续", reg_status):
                reg_status = "存续（在营、开业、在册）"
        else:
            reg_status = ""

        return reg_status

    def insert_batch(self,batch_data):
        '''根据公司全称去重，并写入'''

        logger.info("批量数据组装完成，准备去重...")
        company_names = []
        new_batch = []
        for data in batch_data:
            if not data["crawl_time"]: # 旧数据中有部分研究机构数据不完整，且爬虫时间均为空，过滤掉
                logger.info("跳过数据，ObjectId=[{}]，缺少必须字段crawl_time".format(data["_id"]))
                self.count_skip += 1
                continue 
            if data["name"] in company_names:

                # 关注企业简介的字段是否完整，不完整选取有值的进行替换
                exist_index = company_names.index(data["name"])
                exist_data = new_batch[exist_index]
                if data["desc"] and ( not exist_data["desc"]):
                    new_batch[exist_index] = data

                self.count_inner_dupl += 1
                continue
            elif self.res_kb_process_company.find_one({'_id':str(data['_id'])}):
                continue
            else:
                company_names.append(data["name"])
                new_batch.append(data)
        
        dupl_datas = self.res_kb_process_company.find({"name":{"$in":company_names}})
        for dupl_data in dupl_datas:
            index = company_names.index(dupl_data["name"])

            # 关注企业简介的字段是否完整，不完整选取有值的进行更新
            coming_data = new_batch[index]
            if coming_data["desc"] and (not dupl_data["desc"]):
                coming_data.pop("_id")
                self.res_kb_process_company.update_one({"_id": dupl_data["_id"]}, {'$set':coming_data})

            company_names.pop(index)
            new_batch.pop(index)
            self.count_dupl_company += 1

        if not new_batch:
            logger.info("去重完成，无数据写入")
            return

        logger.info("去重完成，剩余[{}]条，准备写入...".format(len(new_batch)))
        insert_res = self.res_kb_process_company.insert_many(new_batch)
        self.count_insert += len(insert_res.inserted_ids)
        logger.info("批量数据写入完成")

    def get_last_execute_time(self):
        sql_conn = pymysql.connect(host="xxxx",
                           user="xxxx",
                           passwd="xxxx",
                           port=0000,
                           db="xxxx",
                           charset="xxxx")
        sql_cur = sql_conn.cursor()
        currency_sql = """
        SELECT FROM_UNIXTIME(start_time/1000,'%Y-%m-%d') as start_time FROM execution_jobs
        where job_id="{}" and status=50
         order by exec_id desc limit 1
        """.format("company_clean")
        sql_cur.execute(currency_sql)
        sql_result = sql_cur.fetchall()
        return sql_result[0][0]


    def query_company(self, crawl_date):

        if crawl_date == "yesterday":
            crawl_date = self.get_last_execute_time()
            #crawl_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif crawl_date == "today":
            crawl_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(crawl_date.split("-")) == 3:
            crawl_date = crawl_date
            
        else:
            raise Exception("无效参数")
        
        self.process_date = crawl_date
        iso_date_str = crawl_date + 'T00:00:00'
        iso_date = parser.parse(iso_date_str)
        res = self.res_kb_company.find({'crawl_time': {'$gte': iso_date}}).sort([("crawl_time",1)])
        # res = self.res_kb_company.find({'create_time': {'$gte': iso_date}},no_cursor_timeout=True).sort([("crawl_time",1)])
        return res 

    def query_from_file(self):
        '''读取候选企业'''
        with open(os.path.join(kbp_path,"remain20200527.txt"), "r") as fr:
            companys_read = fr.readlines()
        
        companys = list(map(lambda x:x.strip(), companys_read))

        return companys

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

        



    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于process_date以后的企业数据
        '''
        count = 0
        
        res = self.query_company(crawl_date)
        total = res.count()
        logger.info("日期[{}]，开始清洗企业数据，共[{}]条".format(self.process_date, total))

        # 手动执行步骤
        # res = self.query_from_file()
        # total = len(res)

        batch_data = []
       
        # 手动执行步骤
        # for name in res:    

        #     doc = self.res_kb_company.find_one({"company_name": name})
        #     if not doc:
        #         doc = self.res_kb_company.find_one({"company_name":name.replace("(","（").replace(")","）")})    
        for doc in res:

            count += 1
            logger.info("正在处理企业，企业名=[{}]".format(doc["company_name"]))

            update_doc = {
                "_id": str(doc["_id"]), # 直接用ObjectId的值作为后续统一的ID值
                # "uuid": str(uuid.uuid1()),
                "name": doc["company_name"].replace('（','(').replace('）',')'),
                "name_en": doc["name_en"], 
                # update 2020.10.26 remark: 企业别称不自动添加，统一由人工校验后手动添加             
                # "alter_names": self.process_alter_name(doc),  
                "alter_names":[],
                "used_names": self.process_used_name(doc),
                "labels": self.process_labels(doc),
                # "tags": self.process_tags(doc), # 企业人工梳理标签添加，不在清洗时处理，改为relation的时候将这些人工梳理的标签添加到关系中；
                "address": self.process_str(doc["address"]),
                "establish_date": self.process_publish_time(doc["establish_date"]),
                "approve_date": self.process_publish_time(doc["approve_date"]),
                "email": self.process_str(doc["email"]),
                "website": self.process_str(doc["website"]),
                "logo_img": doc["img_logo"],
                "business_scope":self.process_str(doc["business_scope"]),
                "desc": self.process_str(doc["description"]),
                "registration": self.process_str(doc["registration"]),
                "registration_id": self.process_str(doc["registration_id"]),
                "register_status": self.process_register_status(doc),
                "profession": self.process_str(doc["profession"]),
                "social_credit_code": self.process_str(doc["social_credit_code"]),
                "organization_code": doc["organization_code"],
                "legal_person": self.process_str(doc["legal_representative"]),
                "tax_code": self.process_str(doc["taxpayer_id"]),
                "insured_number": doc["insured_number"],
                "url": doc["url"],
                "source": doc["source"],
                "html":doc["html"],
                "crawl_time": doc["crawl_time"],
                "company_type": self.process_company_type(doc),
                "leaders": self.process_leader_position(doc),
                "employee_size":self.process_str(doc["staff_size"]),
                "phone": self.process_str(doc["phone"]),
                "shareholder": doc["shareholder"],
                "financing_round":self.process_f_round(doc["label"]),
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }


            address_res = self.address_parser.parse(doc)
            update_doc.update(address_res)

            business_terms = self.process_business_term(doc)
            update_doc.update(business_terms)

            capitals = self.process_capital(doc)
            update_doc.update(capitals)          
            batch_data.append(update_doc)
            

            # MongoDB批量写入
            if count % 100 == 0 or count == total:
                logger.info("正在写入前[{}]家企业信息".format(count))
                self.insert_batch(batch_data)
                batch_data = []

        logger.info("企业爬虫数据处理完毕，找到[{}]个企业数据，其中批内去重[{}]个，清洗库已有记录[{}]个，写入清洗库[{}]个企业".format(
                        total, self.count_inner_dupl, self.count_dupl_company, self.count_insert ))
        self.close_connection()
        


class AddressParser(object):
    
    def __init__(self):
        self.ak2 = 'xxx'
        self.ak3 = 'xxx'

        # TODO：行政区划使用的是common库中的数据
        with open(os.path.join(dir_path,'prov_city_area_dict.json'),'r') as load_f:
            self.all_address = json.load(load_f)
        self.company_addr_name = []
        for prov in self.all_address:
            if prov.strip('省') not in self.company_addr_name:
                self.company_addr_name.append(prov.strip('省'))
            for city in self.all_address[prov]:
                if city.strip('市') not in self.company_addr_name:
                    self.company_addr_name.append(city.strip('市'))

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
                logger.error("百度接口地址解析出错，地址=[{}]，公司名=[{}]".format(address,info["company_name"]))
                comp_info = {}

            if comp_info:
                parse_out.update(comp_info)
            else:
                addr_res = self.match_addr(address, info['area'])
                res_prov,res_city,res_area = addr_res
                parse_out['province'] = info['area']
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

        if parse_out["province"] in ["北京市","上海市","天津市","重庆市"]:
            parse_out["province"] = parse_out["province"].strip("市")
        prov_name = parse_out["province"].strip('省').strip('市')
        city_name = parse_out["city"].strip('市')
        area_name = parse_out["area"].strip('市').strip('县').strip('区')
        place_name = ""
        if "area" in info:
            place_name = info["area"].strip('省').strip('市')
        company_name = info["company_name"].replace('（','(').replace('）',')')
        if prov_name or city_name:
            if not prov_name and not city_name:
                pass
            elif prov_name and company_name.find(prov_name)>-1:
                pass
            elif city_name and company_name.find(city_name)>-1:
                pass
            elif area_name and company_name.find(area_name)>-1:
                pass
            elif place_name and place_name not in [prov_name,city_name,area_name,"中国"]:
                parse_out['province'] = ""
                parse_out['city'] = ""
                parse_out['area'] = ""
                parse_out['longitude'] = ""
                parse_out['latitude'] = ""
            else:
                city_num = 0
                city_tmp_list = []
                find_name = False
                find_end = False
                for name in self.company_addr_name:
                    if find_name:
                        city_num += 1
                        if city_num >22:
                            find_end = True
                            city_num = 0
                    if name and company_name.find(name)>-1:#获取到公司所含省市名陈
                        city_tmp_list.append(name)
                        find_name = True
                    if find_name and find_end:
                        find_name = False
                        find_end = False
                        res_name = ""
                        if len(city_tmp_list) == 0:#公司名没有省市
                            break
                        if len(city_tmp_list) == 1:#公司名有省市一个
                            res_name = city_tmp_list[0]
                        else:
                            name0 = city_tmp_list[0]
                            name1 = city_tmp_list[1]
                            if company_name.find(name0) < company_name.find(name1):
                                res_name = name0
                            else:
                                res_name = name1
                        if res_name not in [prov_name,city_name,area_name]:
                            parse_out['province'] = ""
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

    # 最早日期  2019-06-03
    
	cleaner = CompanyClean()

	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
