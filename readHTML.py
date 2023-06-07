import pandas as pd
import requests

# URL of the webpage containing the table
url = 'https://zh.wikipedia.org/zh-cn/ISO_3166-1'
url = 'http://changchun.customs.gov.cn/customs/302249/zfxxgk/2799825/302274/tjfwzn/2363290/index.html'
url = 'https://baike.baidu.com/item/ISO%203166-1/5269555?fromModule=search-result_lemma-recommend'


# Define the user-agent header
headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}

# Send the HTTP request with the custom user-agent header
response = requests.get(url, headers=headers)

# Read the table from the webpage
tables = pd.read_html(response.text)

# Select the table you want (in this case, the first table on the page)
df = tables[0]

# Print the contents of the table
print(df)
df.to_csv('data/iso3166_baidu.csv', index=False)
