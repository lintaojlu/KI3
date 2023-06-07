import datetime

import pandas as pd
import re
import crawler
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


# import requests
# url = 'http://translate.google.com/translate_a/single?'
# param = 'client=at&sl=en&tl=zh-CN&dt=t&q=google'
# # from urllib.parse import urlencode
# # param = urlencode(param)
# r = requests.get(url+param)
# print(r.text)


# import pandas as pd
#
# # define the input file name
# input_file = 'data/world_city/chineseCity.xlsx'
#
# # read the input XLSX file into a pandas DataFrame
# df = pd.read_excel(input_file)
#
# # count the unique values in the 'name' column
# name_counts = df['市name'].unique()
#
# # print the number of unique names
# print(f"Number of unique names: {len(name_counts)}")
#
# df = pd.DataFrame({'city_full_name_cn': name_counts})
#
# df.to_csv('data/world_city/city_name_CN_diji_chinese.csv', index=False)
#
# import pandas as pd
# #
# df2 = pd.read_csv('data/world_city/city_name_CN_diji_chinese.csv')
# # df2 = pd.read_csv('data/world_city/city_github.csv', encoding='gbk')
# df1 = pd.read_csv('data/world_city/city_name_Chinese_xianji.csv')
# # df2.drop(['City', 'Tier', 'Region'], inplace=True, axis=1)
# #
# # df3 = pd.merge(df1, df2, how='left', on='city_full_name_cn')
# # df3.to_csv('data/world_city/city_github_diji.csv', index=False)
# # #
# # create a list of cities from df1 and df2
# df1_cities = df1['city_full_name_cn'].tolist()
# df2_cities = df2['city_full_name_cn'].tolist()
#
# # select rows from df1 whose city is not in df2
# df3 = df1[~df1['city_full_name_cn'].isin(df2_cities)]
# df4 = df1[df1['city_full_name_cn'].isin(df2_cities)]
#
# # save the result_old to a new dataframe df3
# df3.to_csv('data/world_city/city_name_Chinese_no.csv', index=False)
# df4.to_csv('data/world_city/city_name_Chinese_du.csv', index=False)

# 列重命名
# name = '/city_simplemaps_trans_baidu_verify.csv'
# df = pd.read_csv(city_trans_path+name)
# df = df.reindex(columns=['city_full_name_en', 'city_full_name_cn', 'lat','lng', 'iso2','admin_name','capital','population'])
# df.to_csv(city_trans_path+name, index=False)

# 挑重叠数据
# df1 = pd.read_csv(city_trans_path + '/city_simplemaps_trans_baidu.csv')
# df2 = pd.read_csv(city_trans_path + '/city_simplemaps_trans_google.csv')
# df_merged = pd.merge(df1, df2, on=['city_full_name_en', 'city_full_name_cn', 'lat', 'lng', 'iso2', 'admin_name', 'capital', 'population'], how='inner')
# df_remain = pd.concat([df1, df_merged], axis=0, join='outer', ignore_index=True).drop_duplicates(keep=False)
# df_merged.to_csv(city_trans_path+'/city_simplemaps_trans_overlap.csv', index=False)
# df_remain.to_csv(city_trans_path+'/city_simplemaps_trans_non_overlap.csv', index=False)

# proxies = {
#     "http": "127.0.0.1:7890",
#     "https": "127.0.0.1:7890",
#     "socks5": "127.0.0.1:7890"
# }


# verify(os.path.join(city_trans_path, 'city_simplemaps_trans_baidu.csv'), city_trans_path+'/city_simplemaps_trans_baidu_verify.csv', 'city_full_name_cn')

# 合并验证的数据
# df1 =pd.read_csv('data/world_city/translate/city_simplemaps_trans_baidu_verify.csv')
# df2 =pd.read_csv('data/world_city/translate/city_simplemaps_trans_google_verify.csv')
# df3 = pd.read_csv('data/world_city/translate/city_simplemaps_trans_overlap.csv')
# df = pd.merge(df2, df1, how='outer')
# df = df.sort_values(by=['population'], ascending=False)
# df = pd.merge(df, df3, how='outer')
# df = df[df['iso2'] != 'CN']
#
# df.to_csv('data/world_city/result_old/city_simplemaps_verify.csv', index=False)
# classify(source_path='data/world_city/ours/ixp_submarine.csv', path1='data/world_city/result_old/city_simplemaps_verify.csv', path2='data/world_city/city_simplemaps.csv')
# classify(source_path='data/world_city/city_IGDB.csv', path1='data/world_city/result_old/city_simplemaps_verify.csv', path2='data/world_city/city_simplemaps.csv')

# df_source = pd.read_csv('data/world_city/result_old/city_ixpsub_else.csv')
# df_1 = pd.read_csv('data/world_city/ours/submarine_location_city.csv')
# in_df1 = df_source['city_full_name_en'].isin(df_1['city_full_name_en'])
# df_in_df1 = df_source[in_df1]
# print(len(df_in_df1))

# 删掉逗号中间
# with open('/Users/linsir/Experiments/PyCharm/ki3/data/world_city/ours/submarine_location_city.csv', 'r') as file:
#     with open('data/world_city/ours/submarine_location_city.csv', 'w') as output_file:
#         for line in file:
#             if line.count(',') >= 2:
#                 parts = line.strip().split(',')
#                 new_row1 = f"{parts[0]},{parts[-1]}"
#                 # new_row2 = f"{parts[1]},{parts[2]}"
#                 output_file.write(new_row1 + '\n')
#                 # output_file.write(new_row2 + '\n')
#             else:
#                 output_file.write(line)
#
# print("Done!")
#
# # 画地图多边形
# from shapely.geometry import Point, Polygon
# path = 'data/world_city/city_IGDB_trans.csv'
# from mpl_toolkits.basemap import Basemap
# import matplotlib.pyplot as plt
#
# # 读取 csv 文件
# df = pd.read_csv(path)
#
# # 将 POLYGON_WKT 列转换为 Polygon 对象
# df['polygon'] = df['polygon'].apply(lambda x: Polygon(eval(x)))
#
# # 创建一个新的图形
# fig = plt.figure(figsize=(10, 10))
#
# # 创建一个地图投影
# m = Basemap(projection='merc', llcrnrlon=-180, llcrnrlat=-90, urcrnrlon=180, urcrnrlat=90, resolution='c')
#
# # 绘制海岸线和国界线
# m.drawcoastlines()
# m.drawcountries()
#
# # 绘制每个多边形
# for polygon in df['polygon']:
#     xs, ys = polygon.exterior.xy
#     xs, ys = m(xs, ys)  # 将经纬度坐标转换为地图投影坐标
#     m.plot(xs, ys, 'r-', linewidth=2)
#
# # 显示图像
# plt.savefig('data/world_city/city_IGDB_polygons.png')
# plt.show()
#

import os
import zipfile
import os
import shutil


# def process_commits(in_path):
#     """
#     unzip all the xxx.zip files, then extract submarine cable data
#     :param in_path:
#     :return:
#     """
#     # # Loop through each file in the directory
#     # for file_name in os.listdir(in_path):
#     #     if file_name.endswith(".zip"):
#     #         # Extract the zip file to a new directory
#     #         with zipfile.ZipFile(os.path.join(in_path, file_name), 'r') as zip_ref:
#     #             try:
#     #                 zip_ref.extractall(path=os.path.join(in_path, file_name[:-4]))
#     #             except KeyError as e:
#     #                 print(e)
#     # extract valuable data and delete others
#     for commit_dir in os.listdir(in_path):
#         commit_path = os.path.join(in_path, commit_dir)
#         if os.path.isdir(commit_path):
#             # print(f'path: [{commit_path}]')
#             # Initialize a variable to keep track of whether the directory was found
#             found = False
#             # Recursively search for the v123 directory
#             for root, dirs, files in os.walk(commit_path):
#                 for v_name in dirs:
#                     if v_name == 'javascripts' or v_name == 'v2' or v_name == 'v3':
#                         # Set the path to the v3 directory
#                         v_path = os.path.join(root, v_name)
#                         print(f'found: [{v_path}]')
#                         # Move all files in v3 to the parent directory
#                         for file_name in os.listdir(v_path):
#                             shutil.move(os.path.join(v_path, file_name), os.path.join(commit_path, file_name))
#                         # Delete the v3 directory
#                         shutil.rmtree(v_path)
#                         found = True
#                         # Delete all the old files and directories
#                         for file_name in os.listdir(commit_path):
#                             if 'CABLE-' in file_name:
#                                 file_path = os.path.join(commit_path, file_name)
#                                 if os.path.isdir(file_path):
#                                     shutil.rmtree(file_path)
#                                 else:
#                                     os.remove(file_path)
#             # If the directory was not found, print "empty"
#             if not found:
#                 print(f"not found: [{commit_path}]")


# Set the path to the parent directory
# path = "data/cable_data"
#
# process_commits(path)


