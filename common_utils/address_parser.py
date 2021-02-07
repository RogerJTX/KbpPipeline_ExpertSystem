from urllib.request import urlopen,quote
import os
import logging
import json
import requests
import configparser
import pymysql


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path, "config.ini")





class AddressParser(object):
    
    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
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

        map_res = dict()
        map_res["longitude"] = ""
        map_res["latitude"] = ""
        map_res['province'] = ''
        map_res['city'] = ''
        map_res['area'] = ''
        if not address:
            return map_res
        to_ak = self.ak2
        url_new = 'http://api.map.baidu.com/geocoding/v3/?address='
        url = url_new
        add = quote(address)
        url_req = url+add+"&output=json&ak="+to_ak
        res  = urlopen(url_req).read().decode()
        temp = json.loads(res)
        if 'result' in temp:
            lng = temp['result']['location']['lng']  # 获取经度
            lat = temp['result']['location']['lat']  # 获取纬度
            map_res['longitude'] = lng
            map_res['latitude'] = lat
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
            logger.error("经纬度获取行政区划接口出错，转向行政区划匹配，出错请求地址=[{}]".format(un_url),e)

        return res_map

    def get_lng_lat_from_kb(self, location_name):
        '''人工解析出来的地点从知识库获取经纬度信息'''
        res = {
            "longitude": "",
            "latitude": ""
        }

        sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                            user = self.config.get("mysql","user") ,
                            passwd = self.config.get("mysql","passwd"),
                            port = self.config.getint("mysql","port") ,
                            db = self.config.get("mysql","db"),
                            charset = "utf8" )
        sql_cur = sql_conn.cursor() 
        # 查询地方的经纬度信息
        sql_state = self.config.get("mysql","location_query").replace("eq","=").format(location_name)
        sql_cur.execute(sql_state)
        datas = sql_cur.fetchall()
        for data in datas:
            lng, lat = data
            res["longitude"] =lng
            res["latitude"] = lat 
            break
        return res 


    def parse(self, info):
        '''
        输入参数 info 格式
        {
            "name": 正在处理的实体的名称,
            "province": 如果实体有所在地区的area属性，则为area省份信息，没有则为空,
            "address": 需要解析的地址
        }
        输出参数 map 格式
        {
            "province":"",
            "city":"",
            "area":"",
            "longitude":Double,
            "latitude":Double
        }
        '''
        parse_out = dict()
        parse_out["province"] = ""
        parse_out["city"] = ""
        parse_out["area"] = ""
        parse_out["longitude"] = ""
        parse_out["latitude"] = ""

        # 匹配所在省、所在市、所在县
        address = ''
        if info["address"]:
            address = info["address"].strip().strip('-')
        if address:
            try:
                comp_info = self.get_lng_lat(address)
            except Exception as e:
                logger.error("百度接口地址解析出错，地址=[{}]，处理名称=[{}]".format(address,info["name"]), e)
                comp_info = {}

            if comp_info:
                parse_out.update(comp_info)
            else:
                addr_res = self.match_addr(address, info['province'])
                res_prov,res_city,res_area = addr_res
                parse_out['province'] = info['province']
                parse_out['city'] = res_city
                parse_out['area'] = res_area
                if res_area:
                    lng_info = self.get_lng_lat_from_kb(res_area)
                    parse_out.update(lng_info)
                elif (not res_area) and res_city:
                    lng_info = self.get_lng_lat_from_kb(res_city)
                    parse_out.update(lng_info)
                elif (not res_area) and (not res_city) and res_prov:
                    lng_info = self.get_lng_lat_from_kb(res_prov)
                    parse_out.update(lng_info)
                
        else:
            parse_out['province'] = info['province']
            parse_out['city'] = ""
            parse_out['area'] = ""
            if info["province"]:
                lng_info = self.get_lng_lat_from_kb(info["province"])
                parse_out.update(lng_info)
            

        if parse_out["province"] in ["北京市","上海市","天津市","重庆市"]:
            parse_out["province"] = parse_out["province"].strip("市")
            parse_out["city"] = parse_out["province"]
        if "company_name" in info:
            prov_name = parse_out["province"].strip('省')
            city_name = parse_out["city"].strip('市')
            company_name = info["company_name"].replace('（','(').replace('）',')')
            if prov_name or city_name:
                if not prov_name and not city_name:
                    pass
                elif prov_name and company_name.find(prov_name)>-1:
                    pass
                elif city_name and company_name.find(city_name)>-1:
                    pass
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
                            if res_name not in [prov_name,city_name]:
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
        if "省" in _area or _area in ["北京","天津","上海","重庆"]:
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

    info = {
        "name":"测试",
        "province":"",
        "address":"北京市海淀区清华东路甲35号"
    }
    parser = AddressParser()
    res = parser.parse(info)
    print(res)
