import pandas as pd
from pymysql.converters import escape_string

import mysql

df = pd.read_csv('data/world_country/iso3166_baidu.csv', keep_default_na=False)
df = df.drop(['备注', '数字代码'], axis=1)

df2 = pd.read_csv('data/world_country/iso3166_wiki.csv', dtype={'数字代码': int}, keep_default_na=False)
df2 = df2.drop(['ISO 3166-2', '是否独立主权'], axis=1)
# print(df2)


df3 = pd.read_csv('data/world_country/country_name_custom.csv', keep_default_na=False)
df3 = df3.drop(['数字代码'], axis=1)

# select countries from wiki
df4 = pd.merge(df, df2, on=['二位代码', '三位代码'], how='right')
# base in baidu, if doesn't exist, fill wiki
df4['英文简称'] = df4['英文简称'].fillna(df4['英文短名称'])
df4['中文简称'] = df4['中文简称'].fillna(df4['中文名称'])
df4['英文全称'] = df4['英文全称'].fillna(df4['英文简称'])
df4 = df4.drop(['英文短名称', '中文名称'], axis=1)

# drop countries don't in wiki and baidu
df4 = pd.merge(df4, df3, on=['二位代码', '三位代码'], how='left')
print(df4)

df4['中文简称'] = df4['中文名称']
df4 = df4.drop(['中文名称'], axis=1)

print(df4)
df4.to_csv('data/country_name_1.csv', index=False)

# version 2, add Chinese full name
df1 = pd.read_csv('data/world_country/country_name_1.csv', keep_default_na=False)
df2 = pd.read_excel('data/country_names_and_codes_of_the_world.xls', sheet_name=0, header=1)
df2 = df2.drop(['中文名称', '英文名称', '英文全称'], axis=1)
df3 = pd.merge(df1, df2, on=['二位代码'], how='left')
df3['中文全称'] = df3['中文全称'].fillna(df3['中文简称'])
header = ['中文简称', '英文简称', '中文全称', '英文全称', '二位代码', '三位代码', '数字代码']
df3 = df3[header]
df3.to_csv('data/country_name_2.csv', index=False)

# version 3
# fill 3_digit code
df3 = pd.read_csv('data/world_country/country_name_2.csv', keep_default_na=False)
print(df3)
df3['数字代码'] = df3['数字代码'].astype(str).str.zfill(3)
print(df3)
# change attributes
df3 = df3.rename(columns={'中文简称': 'short_name_cn', '英文简称': 'short_name_en', '中文全称': 'full_name_cn', '英文全称': 'full_name_en',
                   '二位代码': 'region_2letter_code', '三位代码': 'region_3letter_code', '数字代码': 'region_3digit_code'})
df3.to_csv('data/country_name_3.csv', index=False)


# version 4 add region_continent
class DBProcess:
    def __init__(self):
        self.mydb = mysql.MySQLDatabase()

    def get_df(self):
        # Fetch all rows from the table
        sql = "SELECT * FROM base_region_info"
        succ, result = self.mydb.fetch(sql)
        if not succ:
            print(result)
            return False
        else:
            data = result[0]
            columns = result[1]

        # Create a DataFrame with the retrieved data and column names
        df = pd.DataFrame(data, columns=columns)
        df = df.drop(['region_code_cn', 'region_code_en', 'create_time', 'update_time', 'id'], axis=1)
        df = df.rename(columns={'region_short_code': 'region_2letter_code'})
        return df


dbp = DBProcess()
df1 = dbp.get_df()

df2 = pd.read_csv('data/world_country/country_name_3.csv', keep_default_na=False)
col_list = ['short_name_cn', 'short_name_en', 'full_name_cn', 'full_name_en', 'region_2letter_code', 'region_3letter_code', 'region_3digit_code']
res_list = df2.values.tolist()

df3 = pd.merge(df2, df1, on='region_2letter_code', how='left')
df3.to_csv('data/country_name_4.csv', index=False)


def insert_DB():
    df4 = pd.read_csv('data/world_country/country_name_4.csv', keep_default_na=False)
    col_list = df4.columns
    res_list = df4.values.tolist()
    # escape any special characters in all string type data
    for i in range(len(res_list)):
        res_list[i] = [escape_string(data) if type(data) is str else data for data in res_list[i]]
    print(res_list)

    mydb = mysql.MySQLDatabase()
    succ, resp = mydb.batch_insert('base_region_info_copy1', res_list, col_list)
    if not succ:
        print(resp)
