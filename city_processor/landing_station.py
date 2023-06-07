import json
import re
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
import os
from datetime import datetime

# GitHub用户名和仓库名
from tqdm import tqdm

from utility import is_point_inside_polygon, split_df


def github_url_download():
    username = "hasan-soliman"
    repository = "CABLE"
    # API URL
    url = f"https://api.github.com/repos/{username}/{repository}/commits"

    # Send GET request to get JSON data for the first page
    response = requests.get(url)

    # Parse JSON data and download each commit
    while True:
        # 解析JSON数据并下载每个commit
        for commit in tqdm(response.json(), desc='downloading', total=len(response.json())):
            commit_sha = commit["sha"]
            commit_date = datetime.strptime(commit["commit"]["author"]["date"], "%Y-%m-%dT%H:%M:%SZ").strftime(
                "%Y-%m-%d")
            commit_message = commit["commit"]["message"].replace("\n", "").replace("/", "-")
            if len(commit_message) > 200:
                commit_message = commit_message[:200]
            commit_url = f"https://github.com/{username}/{repository}/archive/{commit_sha}.zip"
            commit_path = f"data/cable_data/{commit_date}_{commit_message}.zip"

            # Download commit
            r = requests.get(commit_url, allow_redirects=True)

            # Save commit to local disk
            # print(commit_path)
            with open(commit_path, "wb") as f:
                f.write(r.content)

        # Check if there is a next page
        if "next" in response.links:
            # Get URL for next page
            url = response.links["next"]["url"]

            # Send GET request to get JSON data for next page
            response = requests.get(url)
        else:
            # No more pages, break out of loop
            break


def _search_with_coordinate(row, language='zh-CN', city_type='', filter_type='', debug=False):
    if debug:
        point = row
    else:
        point = f'{row["latitude"]}, {row["longitude"]}'
    # 设置API密钥和城市名称
    api_key = "AIzaSyD202Xzio1yOu5DpC1vV1VlHmOVrZU09nA"
    # 发送请求并获取响应
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={point}&result_type={city_type}&language={language}&key={api_key}"
    city = iso2 = label = ''
    try:
        response = requests.get(url).json()
        if debug:
            print(json.dumps(response, indent=4, ensure_ascii=False))
        # 解析响应并获取所需信息
        status = response['status']
        if status == 'OK':
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
                iso2 = 'CN'
                label = 'locality'
                row['city_cn'] = city
                row['iso2'] = iso2
                row['city_type'] = label
                return row
            elif iso2 == 'MO':
                city = '澳门'
                iso2 = 'CN'
                label = 'locality'
                row['city_cn'] = city
                row['iso2'] = iso2
                row['city_type'] = label
                return row
            else:
                types = filter_type.split('|')

            found_flag = False
            cn_flag = False
            for _type in ['locality', 'postal_town']:  # 先找locality和postal_town
                for result in response['results']:
                    for component in result['address_components']:
                        if _type in component['types']:
                            city = component['long_name']
                            label = _type
                            found_flag = True
                            cn_regex = re.compile(r'^[\u4e00-\u9fff－\-]+$')
                            if cn_regex.match(city):  # 结果不是中文就继续查找
                                cn_flag = True
                    if cn_flag:
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
                                    cn_flag = True
                        if cn_flag:
                            break
                    else:  # 对for循环到底的else判断
                        continue
                    break
        else:
            print(f'status: {status}')
    except KeyError and requests.exceptions.RequestException as e:
        print(f"ERROR: {e}")
    # 输出结果
    if debug:
        return city, iso2, label
    else:
        row['city_cn'] = city
        row['iso2'] = iso2
        row['city_type'] = label
        return row


def _search_with_IGDB(row, df_IGDB):
    point = f'({row["longitude"]}, {row["latitude"]})'
    trans_flag = False
    for j, row2 in df_IGDB.iterrows():
        if is_point_inside_polygon(eval(point), eval(row2['polygon'])):
            row['city_cn'] = row2['city_cn']
            row['city_en'] = row2['city_en']
            row['iso2'] = row2['iso2']
            if not row2['city_cn']:
                print(f'[No Chinese]: station:{row["name"]}')
                trans_flag = True
            break
    if not trans_flag:
        print(f'No result, name: {row["name"]}')
    return row


class LandingStations:
    def __init__(self, path):
        self.data = pd.read_csv(path, keep_default_na=False)
        self.data.fillna('')
        self.city_type_dict = {'administrative_area_level_1':1, 'administrative_area_level_2':2, 'administrative_area_level_3':3, 'administrative_area_level_4':4, 'locality':5, 'postal_town':6, 'else':7}

    def create_cls(self):
        print('[Create cls...]')
        self.data['cls'] = self.data['name'].apply(lambda x: x.split(',')[0].strip())

    def create_city(self, IGDB_path):
        print('[Create city...]')
        df_IGDB = pd.read_csv(IGDB_path)
        for i, row1 in tqdm(self.data.iterrows(), desc='locate a city', total=len(self.data)):
            point = f'({row1["longitude"]}, {row1["latitude"]})'
            # print(point)
            for j, row2 in df_IGDB.iterrows():
                if is_point_inside_polygon(eval(point), eval(row2['polygon'])):
                    self.data.loc[i, 'city_cn'] = row2['city_cn']
                    self.data.loc[i, 'city_en'] = row2['city_en']
                    self.data.loc[i, 'iso2'] = row2['iso2']
                    if row2['city_cn'] == '':
                        print(f'[No Chinese]: station:{row1["name"]}')
                    break
            if j == len(df_IGDB) - 1:
                print(f'No result, name: {row1["name"]}')

    def create_city_IGDB(self, IGDB_path):
        print('[Create city...]')
        df_IGDB = pd.read_csv(IGDB_path)
        rows = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for i, row1 in tqdm(self.data.iterrows(), desc='locate a city', total=len(self.data)):
                future = executor.submit(_search_with_IGDB, row1, df_IGDB)
                rows.append(future.result())
        self.data = pd.DataFrame(rows)

    def create_city_GOOGLE_MAP(self, city_type, filter_type):
        print('[Create city...]')
        rows = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for i, row in tqdm(self.data.iterrows(), desc='locate a city', total=len(self.data)):
                future = executor.submit(_search_with_coordinate, row, city_type=city_type, filter_type=filter_type)
                row = future.result()
                rows.append(row)
        self.data = pd.DataFrame(rows)

    def create_country(self, country_path='/Users/linsir/Experiments/PyCharm/ki3/data/world_country/country_name_final.csv'):
        print('[Create country...]')
        df_country = pd.read_csv(country_path, keep_default_na=False)
        self.data['country_cn'] = ''
        self.data['country_en'] = ''
        rows = []
        for i, row in self.data.iterrows():
            if row['iso2'] in df_country['region_2letter_code'].values:
                row1 = df_country[df_country['region_2letter_code'] == row['iso2']].iloc[0]
                self.data.loc[i, 'country_cn'] = row1['short_name_cn']
                self.data.loc[i, 'country_en'] = row1['short_name_en']
            else:
                print(row)
                rows.append(row)
                self.data.loc[i, 'country_cn'] = ''
                self.data.loc[i, 'country_en'] = ''
        # df = pd.DataFrame(rows)
        # df.to_csv('/Users/linsir/Experiments/PyCharm/ki3/data/world_city/result/landing_station_else.csv', index=False)

    def save_to_csv(self, path):
        print(f'[Save to path:{path}]')
        self.data = self.data.reindex(columns=['name','cls','latitude','longitude','city_cn','city_en','city_type','iso2','country_cn','country_en','note'])
        self.data.to_csv(path, index=False)

    def enumerate_city_type(self):
        self.data['city_type'] = self.data['city_type'].apply(lambda x: self.city_type_dict[x] if x in self.city_type_dict.keys() else 7)


# os.chdir('/Users/linsir/Experiments/PyCharm/ki3/')
# path1 = 'data/world_city/result/landing_station_trans_1.csv'
# path2 = 'data/world_city/result/landing_station_trans.csv'
# stations = LandingStations(path1)
# # stations.create_country()
# # stations.create_cls()
# # stations.create_city_GOOGLE_MAP(city_type='', filter_type='administrative_area_level_2|administrative_area_level_3|administrative_area_level_4')
# stations.enumerate_city_type()
# stations.save_to_csv(path2)

# path2 = 'data/world_city/result/submarine_定位后是英文再反向查询到中文.csv'
# path_v = path2.split('.')[0]+'_v.csv'
# path_x_cn = path2.split('.')[0]+'_x_cn.csv'
# path_empty = path2.split('.')[0]+'_empty.csv'
# split_df(path2, verify_path=path_v, empty_path=path_empty, non_cn_path=path_x_cn)
os.system('export https_proxy=http://127.0.0.1:7890 http_proxy=http://127.0.0.1:7890 all_proxy=socks5://127.0.0.1:7890')
# print(_search_with_coordinate('45.357,36.4774', city_type='', filter_type='administrative_area_level_2|administrative_area_level_3|administrative_area_level_4', debug=True))
station = LandingStations('~/.submarine/landing_station_trans.csv')
station.data['city_en'] = ''
station.create_country()
station.save_to_csv('~/.submarine/landing_station_trans1.csv')
