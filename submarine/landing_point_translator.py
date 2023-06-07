import json
import os

import pandas as pd
import re
import requests
from mysql import MySQLDatabase


def _search_with_coordinate(point, language='zh-CN', city_type='', filter_type='administrative_area_level_2|administrative_area_level_3|administrative_area_level_4', debug=False):
    # 设置API密钥和城市名称
    api_key = "AIzaSyD202Xzio1yOu5DpC1vV1VlHmOVrZU09nA"
    # 发送请求并获取响应
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={point}&result_type={city_type}&language={language}&key={api_key}"
    city = iso2 = label = ''
    try:
        response = requests.get(url).json()
        # 解析响应并获取所需信息
        status = response['status']
        if status == 'OK':
            if debug:
                print(json.dumps(response, indent=4, ensure_ascii=False))
            # 先找出国家，再根据国家选择过滤方式
            for result in response['results']:
                for component in result['address_components']:
                    if 'country' in component['types']:
                        iso2 = component['short_name']

            if iso2 == 'GR' or iso2 == 'ES':
                types = ['administrative_area_level_4']
            elif iso2 == 'CL':
                types = ['administrative_area_level_3']
            elif iso2 == 'HK':
                city = '香港'
                label = 'locality'
                return city, iso2, label
            elif iso2 == 'MO':
                city = '澳门'
                label = 'locality'
                return city, iso2, label
            else:
                types = filter_type.split('|')

            found_flag = False
            target_language_flag = False
            for _type in ['locality', 'postal_town']:  # 先找locality和postal_town
                for result in response['results']:
                    for component in result['address_components']:
                        if _type in component['types']:
                            city = component['long_name']
                            label = _type
                            found_flag = True
                            cn_regex = re.compile(r'^[\u4e00-\u9fff－\-]+$')
                            if cn_regex.match(city):  # 结果不是中文就继续查找
                                target_language_flag = True
                            if language == 'en':
                                target_language_flag = True
                    if target_language_flag:
                        break
                else:  # 对for循环到底的else判断
                    continue
                break
            if not found_flag:  # 先找locality和postal_town再找别的字段
                for _type in types:  # 按照filter字段顺序对所有components进行循环查找
                    for result in response['results']:
                        for component in result['address_components']:
                            if _type in component['types']:
                                city = component['long_name']
                                label = _type
                                cn_regex = re.compile(r'^[\u4e00-\u9fff－\-]+$')
                                if cn_regex.match(city):  # 结果不是中文就继续查找
                                    target_language_flag = True
                                if language == 'en':
                                    target_language_flag = True
                        if target_language_flag:
                            break
                    else:  # 对for循环到底的else判断
                        continue
                    break
        else:
            print(f'status: {status}')
    except KeyError and requests.exceptions.RequestException as e:
        print(f"ERROR: {e}")
    # 输出结果
    return city, iso2, label


class LPTranslator:
    def __init__(self, lp_id, name, latitude, longitude):
        self. lp_id = lp_id
        self.name = name
        self.cls = ''
        self.latitude = latitude
        self.longitude = longitude
        self.city_cn = ''
        self.city_en = ''
        self.country_cn = ''
        self.country_en = ''
        self.city_type = 7
        self.note = 'auto translate'
        self.db = MySQLDatabase()
        self.city_type_dict = {'administrative_area_level_1':1, 'administrative_area_level_2':2, 'administrative_area_level_3':3, 'administrative_area_level_4':4, 'locality':5, 'postal_town':6, 'else':7}

    def translate(self):
        # cls
        self.cls = self.name.split(',')[0].strip()

        # city
        point = f'{self.latitude}, {self.longitude}'
        self.city_cn, iso2, city_type_str = _search_with_coordinate(point)
        self.city_en, iso2_2, city_type_str2 = _search_with_coordinate(point, language='en')
        cn_regex = re.compile(r'^[\u4e00-\u9fff－\-]+$')
        if not cn_regex.match(self.city_cn) or city_type_str != city_type_str2:
            self.note = f'city error: cn:[{self.city_cn}][{city_type_str}] en:[{self.city_en}][{city_type_str2}]'
            return False, self.note

        # country
        # query for database
        # sql = 'SELECT short_name_cn, short_name_en FROM base_region WHERE region_2letter_code = "{}";'.format(iso2)
        df_country = pd.read_csv('~/.submarine/country_name_final.csv')
        df_country = df_country.fillna({'region_2letter_code': "NA"})
        if iso2 in df_country['region_2letter_code'].values:
            resp = df_country[df_country['region_2letter_code'] == iso2].iloc[0]
            self.country_cn = resp['short_name_cn']
            self.country_en = resp['short_name_en']
        if self.country_en == '' or self.country_cn == '':
            return False, f'country error:[{iso2}]'

        # city_type
        self.city_type = self.city_type_dict[city_type_str]
        if not isinstance(self.city_type, int):
            return False, f'city_type error:[{self.city_type}]'

        data = {'name': self.name, 'cls': self.cls, 'city_cn': self.city_cn, 'city_en': self.city_en, 'country_cn': self.country_cn, 'country_en': self.country_en, 'city_type': self.city_type, 'note': self.note, 'iso2': iso2}
        return True, data


# print(_search_with_coordinate('12.6996,-61.339', language='zh-CN', debug=True))

