import os
import re
import time
from functools import wraps
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon


def add_quotation_in_csv():
    """
    将文件中的引号处理
    :return:
    """
    with open('data/world_city/city_github_diji_google_trans_reflect.csv', 'r') as f:
        lines = f.readlines()
        modified_lines = []
        for line in lines:
            if line.count(',') > 3:
                split_line = re.split(",", line)
                # add \" after the second ,
                split_line[2] = '\"' + split_line[2]
                modified_line = ','.join(split_line)
                # add \" at the end
                modified_line = re.sub('\n', '\"\n', modified_line)
                print(modified_line)
                modified_lines.append(modified_line)
            else:
                modified_lines.append(line)

    with open('data/world_city/geodatasource_city.csv', 'w') as f:
        f.writelines(modified_lines)


def search_break_point(inpath, outpath, column='city_full_name_en'):
    """
    找到file1和file2的column列的断点，方便断点续处理
    :param inpath: 需要处理的文件路径
    :param outpath: 处理输出的文件路径
    :param column: 用于判断的列
    :return: bool, 去除已处理的输入数据
    """
    # Read input CSV file into a pandas dataframe
    df = pd.read_csv(inpath)
    df = df.fillna('')
    if os.path.isfile(outpath):
        print(f'File existed: [{outpath}]')
        df_out = pd.read_csv(outpath)
        if not df_out.empty and df_out[column][len(df_out) - 1] in df[column].values:
            last_data = df_out[column][len(df_out) - 1]
            bp_index = df[df[column] == last_data].index[0]
            print(f'Resume from the break point: {last_data}({bp_index + 1}/{len(df)})')
            return True, df.loc[bp_index + 1:]
    print(f'Start(0/{len(df)})')
    return False, df


def func_timer(function):
    """
    用装饰器实现函数计时
    :param function: 需要计时的函数
    :return: None
    """

    @wraps(function)
    def function_timer(*args, **kwargs):
        print('[Function: {name} start...]'.format(name=function.__name__))
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        print('[Function: {name} finished, spent time: {time:.2f}s]'.format(name=function.__name__, time=t1 - t0))
        return result

    return function_timer


def split_df(in_path, verify_path, empty_path, non_cn_path):
    """
    将原文件city_full_name_cn中文、非中文、空的的数据找出来
    :param in_path:
    :param verify_path:
    :param non_cn_path:
    :param empty_path:
    :return:
    """
    df = pd.read_csv(in_path)

    # Find all rows with empty 'city_full_name_cn'
    empty_mask = df['city_full_name_cn'].isna()
    # Find all rows with non-Chinese 'city_full_name_cn'
    cn_regex = re.compile(r'^[\u4e00-\u9fff－\-]+$')
    non_cn_mask = ~empty_mask & df['city_full_name_cn'].astype(str).apply(lambda x: bool(cn_regex.match(x)) == False)
    cn_mask = df['city_full_name_cn'].astype(str).apply(lambda x: bool(cn_regex.match(x)) == True)

    df_empty = df[empty_mask]
    df_non_cn = df[non_cn_mask]
    df_verify = df[cn_mask]
    df_verify.to_csv(verify_path, index=False)
    df_empty.to_csv(empty_path, index=False)
    df_non_cn.to_csv(non_cn_path, index=False)
    print('Done!')


# 处理POLYGON_WKT列
def process_POLYGON_WKT(path):
    # 读取csv文件
    df = pd.read_csv(path)
    df['polygon'] = df['POLYGON_WKT'].str.split('\(\(|\)\)').str[1].str.split(',').apply(
        lambda x: ', '.join([f'({i.strip().replace(" ", ", ")})' for i in x]))
    df['polygon'] = df['polygon'].apply(lambda x: f'({x})')
    df.drop(['POLYGON_WKT'], axis=1, inplace=True)
    df.to_csv(path, index=False)


# 绘制多边形
def show_polygons(path):
    # 读取csv文件
    df = pd.read_csv(path)
    # 将POLYGON_WKT列转换为Polygon对象
    df['polygon'] = df['polygon'].apply(lambda x: Polygon(eval(x)))

    # 创建一个新的图形
    fig, ax = plt.subplots()

    # 绘制每个城市的轮廓
    for polygon in df['polygon']:
        xs, ys = polygon.exterior.xy
        ax.plot(xs, ys)

    # 显示图形
    plt.savefig('data/world_city/city_IGDB_polygons.png')
    plt.show()


def is_point_inside_polygon(point, polygon_points):
    """
    判断一个坐标点是否在一个多边形内部。

    Args:
        point: tuple，表示一个坐标点，例如(0.5, 0.5)
        polygon_points: list of tuples，表示多边形的顶点坐标，例如[(0, 0), (0, 1), (1, 1), (1, 0)]

    Returns:
        bool，表示坐标点是否在多边形内部，True为在，False为不在。
    """
    polygon = Polygon(polygon_points)
    point = Point(point)
    return polygon.contains(point)


def com_sim(in_path, out_path):
    import opencc
    # 创建简繁转换器
    converter = opencc.OpenCC('t2s.json')

    # 打开输入文件和输出文件
    with open(in_path, 'r', encoding='utf-8') as fin, \
            open(out_path, 'w', encoding='utf-8') as fout:
        # 逐行读取输入文件
        for line in fin:
            # 使用转换器将繁体汉字转换为简体汉字
            line = converter.convert(line)
            # 将转换后的行写入输出文件
            fout.write(line)
