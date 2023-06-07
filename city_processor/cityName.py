import os.path
import re
import time

import pandas as pd
import requests
from tqdm import tqdm
import concurrent.futures
from utility import search_break_point, func_timer, split_df
import random
import json
from hashlib import md5
from fuzzywuzzy import fuzz


def _process_city(city, url, index):
    # print(f'# {index}: {city}')
    params = {'searchValue': city, 'pageNum': '1', 'pageSize': '1'}
    try:
        response = requests.get(url, headers=None, params=params)
        response.raise_for_status()
        datas = response.json()['data']
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        datas = None
    return city, datas


# 英文城市名翻译
def process_first(inpath, outpath):
    # Read input CSV file into a pandas dataframe
    df = pd.read_csv(inpath)

    # Drop duplicate city names in the dataframe
    df.drop_duplicates(subset=['city_full_name_en'], inplace=True)

    # Print the length of the dataframe
    # print(f'length of df: {len(df)}')

    # Create an empty pandas dataframe with columns for translated city names and related information
    df2 = pd.DataFrame(columns=['city_full_name_en', 'city_full_name_cn'])

    # Define the URL for the translation API
    url = 'https://dmfw.mca.gov.cn/foreign/getForeignList'

    # Use ThreadPoolExecutor to run process_city() concurrently with a maximum of 45 workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        # Submit a task to process_city() for each city in the input dataframe
        futures = [executor.submit(_process_city, row['city_full_name_en'], url, i) for i, row in df.iterrows()]

        # Iterate through the futures and process the results
        for i, future in tqdm(enumerate(futures), desc='Translating', total=len(futures)):
            # Get the translated city name and related information from the future result_old
            city, datas = future.result()

            # Compile a regular expression pattern to match the translated city name
            try:
                pattern = re.compile(fr"(?<![a-zA-Z\d]){city}(?![a-zA-Z\d])")
            except Exception as e:
                print(f'[Error]: {e}')

            # If translated city name data is available and matches the regular expression pattern, add it to the output dataframe
            if datas:
                if re.search(pattern, datas[0]['romanAlphabet']):
                    df2.loc[len(df2)] = {'city_full_name_en': city, 'city_full_name_cn': datas[0]['chineseName']}

            # Write the output dataframe to the output CSV file every 10000 rows
            if i % 10000 == 0:
                if i == 0:
                    df2.to_csv(outpath, index=False)
                    df2 = pd.DataFrame(columns=['city_full_name_en', 'city_full_name_cn'])
                else:
                    df2.to_csv(outpath, mode='a', index=False, header=None)
                    df2 = pd.DataFrame(columns=['city_full_name_en', 'city_full_name_cn'])

        # Write any remaining rows in the output dataframe to the output CSV file
        df2.to_csv(outpath, mode='a', index=False, header=None)


def _translate_by_google(data, source_language, target_language):
    # translate from google
    url = 'http://translate.google.com/translate_a/single?'
    param = f'client=gtx&sl={source_language}&tl={target_language}&dt=t&q={data}'
    try:
        result = requests.get(url + param).json()[0][0][0]
        # print(result_old)
    except requests.exceptions.RequestException as e:
        print(f"ERROR: {e}")
        result = ''
    return result


# Generate salt and sign
def _make_md5(s, encoding='utf-8'):
    return md5(s.encode(encoding)).hexdigest()


def _translate_by_baidu(data, source_language='en', target_language='zh'):
    # Set your own appid/appkey.
    appid = '20230424001654666'
    appkey = 'v563TX82zhg_JNzkpHlw'
    url = 'http://api.fanyi.baidu.com/api/trans/vip/translate'
    salt = random.randint(32768, 65536)
    sign = _make_md5(appid + data + str(salt) + appkey)

    # Build request
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {'appid': appid, 'q': data, 'from': source_language, 'to': target_language, 'salt': salt, 'sign': sign}

    try:
        r = requests.post(url, params=payload, headers=headers).json()
        result = r['trans_result'][0]['dst']
        # print(result_old)
        # print(json.dumps(r, indent=4, ensure_ascii=False))
    except KeyError and requests.exceptions.RequestException as e:
        print(f"ERROR: {e}")
        result = ''
    return result


def _translate_by_google_map(city_name, language):
    # 设置API密钥和城市名称
    api_key = "AIzaSyD202Xzio1yOu5DpC1vV1VlHmOVrZU09nA"
    # 发送请求并获取响应
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={city_name}&key={api_key}&language={language}"
    try:
        response = requests.get(url).json()
        # 解析响应并获取所需信息
        status = response['status']
        if status == 'OK':
            name = response['results'][0]['address_components'][0]['long_name']
            return name
            # pattern = re.compile(r'[\u4e00-\u9fff]')  # matches any Chinese character
            # for address_component in response['results'][0]['address_components']:
            #     name = address_component['long_name']
            #     if bool(pattern.search(name)):
            #         return name
        # any other case, return ''
        print()
        print(f'status: {status}')
        print(f"city_name: {city_name}")
        # print(json.dumps(response, indent=4, ensure_ascii=False))
        name = ''
    except KeyError and requests.exceptions.RequestException as e:
        print()
        print(f"ERROR: {e}")
        name = ''
    # 输出结果
    return name


@func_timer
def translate(in_path, out_path, in_column, out_column, source_language, target_language, api='google', max_workers=10):
    """
    使用google对file中某列进行翻译并保存成新的file
    """
    # resume at the break point
    append_flag, df = search_break_point(in_path, out_path, column=in_column)

    df2 = pd.DataFrame()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, row in tqdm(df.iterrows(), desc='Translating', total=len(df)):
            if api == 'google':
                future = executor.submit(_translate_by_google, row[in_column], source_language, target_language)
                result = future.result()

            elif api == 'baidu':
                result = _translate_by_baidu(row[in_column], source_language, target_language)
                # time.sleep(0.1)
            elif api == 'google_map':
                result = _translate_by_google_map(row[in_column]+','+row['iso2'], target_language)
                # cn_regex = re.compile(r'^[\u4e00-\u9fff－\-]+$')
                # if result == '' or bool(cn_regex.match(result)) == False:
                #     if row['state_name']:
                #         result = _translate_by_google_map(row[in_column] + ','+row['state_name']+ ','+row['iso2'],target_language)

            # modify row will modify df
            row[out_column] = result
            df2 = pd.concat([df2, row.to_frame().T])
            # Write the output dataframe to the output CSV file every 10000 rows
            if i % 1000 == 0:
                if append_flag:
                    df2.to_csv(out_path, mode='a', index=False, header=None)
                    df2 = pd.DataFrame()
                else:
                    df2.to_csv(out_path, index=False)
                    df2 = pd.DataFrame()
                    append_flag = True
        # save remained data
        df2.to_csv(out_path, mode='a', index=False, header=None)


def _verify_tianditu(data: str):
    # if data is not string, return False
    if not isinstance(data, str):
        return False
    url = 'http://api.tianditu.gov.cn/administrative?'
    param = 'postStr={"searchWord":"' + data + '","searchType":"1","needSubInfo":"false","needAll":"false","needPolygon":"true","needPre":"false"}'
    token = '&tk=2764ff5436b7fcae69d117b8def39e1e'
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36'}
    try:
        r = requests.get(url + param + token, headers=headers).json()
        # print(json.dumps(r, indent=4, ensure_ascii=False))
        if r['returncode'] == '100':
            return True
        else:
            print(f'msg: {r["msg"]}; DATA: {data}')
            return False
    except Exception as e:
        print(f"ERROR: {e}; DATA: {data}")
        return False


def verify(in_path, out_path, in_column):
    append_flag, df = search_break_point(in_path, out_path, column=in_column)

    df2 = pd.DataFrame()

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for i, row in tqdm(df.iterrows(), desc='Verifying', total=len(df)):
            future = _verify_tianditu(row[in_column])
            time.sleep(0.1)
            if future:
                df2 = pd.concat([df2, row.to_frame().T])
            if i % 100 == 0:
                if append_flag:
                    df2.to_csv(out_path, mode='a', index=False, header=None)
                    df2 = pd.DataFrame()
                else:
                    df2.to_csv(out_path, index=False)
                    df2 = pd.DataFrame()
                    append_flag = True
        # save remained data
        df2.to_csv(out_path, mode='a', index=False, header=None)


def classify(source_path, path1, path2):
    df1 = pd.read_csv(path1)
    df_source = pd.read_csv(source_path, dtype={'city_full_name_en': str})
    df_2 = pd.read_csv(path2)

    df_in_df1 = pd.DataFrame()

    for i, row in df_source.iterrows():
        city = row['city_full_name_en']
        for j, row1 in df1.iterrows():
            city1 = row1['city_full_name_en']
            if isinstance(city, str):
                if re.search(city, city1) or re.match(city.replace(' ', '-'), city1):
                    df_in_df1 = pd.concat([df_in_df1, row.to_frame().T])

    # # 判断df_source中的city_full_name_en和city_full_name_cn两列是否在1_cities中出现
    # # df_in_df1 = pd.merge(df_source, df_1[['city_full_name_en', 'city_full_name_cn']], on=['city_full_name_en'], how='inner')
    # in_df1 = df_source['city_full_name_en'].isin(df_1['city_full_name_en'])
    # df_in_df1 = df_source[in_df1]
    #
    # # 在df_simplemaps中存在但不在df_1中的数据
    # # df_in_df2 = pd.merge(df_source, df_2, on=['city_full_name_en'], how='inner')
    # # not_in_df1 = ~df_in_df2.isin(df_in_df1)
    # in_df2 = df_source['city_full_name_en'].isin(df_2['city_full_name_en'])
    # df_in_df2 = df_source[~in_df1 & in_df2]
    #
    # # 在两者中都不存在的数据
    # # not_in_simplemaps = ~df_source.isin(df_in_df2).all(1)
    # # not_in_df1 = ~df_source.isin(df_in_df1).all(1)
    # # df_not_in_both = df_source[not_in_simplemaps & not_in_df1]
    # # not_in_both1 = ~df_source.isin(df_1)
    # # not_in_both2 = ~df_source.isin(df_2)
    # df_not_in_both = df_source[~in_df1 & ~in_df2]
    #
    # 保存df_1_ixp_sub到city_ixp_sub_verified.csv
    df_in_df1.to_csv('city_IGDB_verified.csv', index=False)
    #
    # # 保存df_simplemaps_ixp_sub到city_ixp_sub_sim.csv
    # df_in_df2.to_csv('city_IGDB_sim.csv', index=False)
    #
    # # 保存df_else_ixp_sub到city_ixp_sub_else.csv
    # df_not_in_both.to_csv('city_IGDB_else.csv', index=False)


# 定义模糊匹配函数
def fuzzy_match(city_name, name, score=80):
    if fuzz.token_sort_ratio(city_name, name) > score:
        return True
    return False


# 定义部分匹配函数
def partial_match(city_name, name):
    if not isinstance(city_name, str):
        return False
    # 去掉城市名中多余的字符
    if 'city' in city_name or 'City' in city_name:
        city_name = re.split(' ', city_name)[0]
    if 'city' in name or 'City' in name:
        name = re.split(' ', name)[0]
    if city_name == name:
        # print(f'city1:{city_name}, city2:{name}')
        return True
    # 使用正则表达式将字符串按照,(拆分，针对有逗号和括号的匹配
    pattern = r'[,()]'
    list1 = re.split(pattern, city_name)
    list2 = re.split(pattern, name)
    if len(list1[0]) > 5:
        if list1[0] == list2[0]:
            print(f'city1:{city_name}, city2:{name}')
            return True
    return False


def csv_match(df1_path, df2_path, match_path, not_match_path, column, method='partial'):
    # 读取df1和df2
    df1 = pd.read_csv(df1_path)
    df2 = pd.read_csv(df2_path)

    # 选择出能够匹配的行
    matched_rows = []
    if method == 'partial':
        for index, row in tqdm(df1.iterrows(), desc='matching', total=len(df1)):
            # 对于df1中的每一项，模糊匹配df2中的结果
            city_list = df2[column].tolist()
            for name in city_list:
                if partial_match(row[column], name):
                    name_cn = df2[df2['city_full_name_en'] == name]['city_full_name_cn'].reset_index(drop=True)[0]
                    row.loc['city_full_name_cn'] = name_cn
                    matched_rows.append(row)
                    break
    elif method == 'fuzz':
        for index, row in tqdm(df1.iterrows(), desc='classify', total=len(df1)):
            city_list = df2[column].tolist()
            for name in city_list:
                if fuzzy_match(row[column], name):
                    matched_rows.append(row)
                    break

    # 将匹配的行保存在df_match中
    df3_match = pd.DataFrame(matched_rows)
    df3_match.to_csv(match_path, index=False)

    # 保存不匹配的部分
    df_merged = pd.merge(df1, df3_match, on=df1.columns.tolist(), how='outer')
    df_remain = pd.concat([df_merged, df3_match], axis=0, join='outer', ignore_index=True).drop_duplicates(keep=False)
    df_remain.to_csv(not_match_path, index=False)


cn_google = 'zh-CN'
cn_baidu = 'zh'

# matching
# csv_match(df1_path='data/world_city/ours/submarine_location_city.csv',
#           df2_path='data/world_city/result/city_simplemaps_verify.csv',
#           match_path='data/world_city/result/submarine_partial.csv',
#           not_match_path='data/world_city/result/submarine_not_partial.csv',
#           column='city_full_name_en')

# translating
# translate(in_path='data/world_city/city_IGDB.csv',
#           out_path='data/world_city/translate/city_IGDB_trans_baidu.csv',
#           in_column='city_full_name_en', out_column='city_full_name_cn', source_language='en',
#           target_language=cn_baidu, api='baidu', max_workers=50)
translate(in_path='data/world_city/result/submarine_trans_all_x_cn_v_empty.csv',
          out_path='data/world_city/result/submarine_trans_all_x_cn_v2.csv',
          in_column='city_full_name_en', out_column='city_full_name_cn', source_language='en',
          target_language=cn_google, api='google', max_workers=100)
# translate(in_path='data/world_city/result/submarine_定位后是英文再谷歌翻译.csv',
#           out_path='data/world_city/result/submarine_定位后是英文再反向查询到中文.csv',
#           in_column='city_full_name_en', out_column='city_full_name_cn', source_language='en',
#           target_language=cn_google, api='google_map', max_workers=1)

# print(_translate_by_google_map('Artemovsk,RU', language='zh-CN'))


# find_verify_else(in_path='data/world_city/city_IGDB_trans_2.csv', verify_path='data/world_city/city_IGDB_verify_2.csv', else_path='data/world_city/city_IGDB_else.csv')
