# -*- coding: utf-8 -*-
"""
Created on Sun Sep  2 12:50:11 2018

@author: Annie
"""

#补充数据：data_single_resblock中只存储了链家网上爬取的经纬度，为百度的经纬度
#在获取路径规划时已将百度经纬度转成过高德经纬度，但未存在数据库中
#从data_gaode_resblock的html数据中找出高德经纬度，并更新到数据库中

import pymongo
import pandas as pd
import json

def parse_direction(html_str):
    answer = json.loads(html_str)
    if answer['status']=='1':  #status=1，说明有路线数据route
        route=answer['route']
        result={'origin':route['origin'],'destination':route['destination']}
    else:
        result={'origin':'','destination':''}
    return result

#数据库存储方式设置：
client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
database = client['LianJia'] # 数据库的名字（mongo中没有这个数据库就创建）
table_input = database['data_gaode_resblock']   
html_dict=pd.DataFrame(list(table_input.find()))

for i in range(11302,len(html_dict)):
    html_str1=html_dict.loc[i,'html_str1']
    location1=parse_direction(html_str1)

    html_str2=html_dict.loc[i,'html_str2']
    location2=parse_direction(html_str2)
    
    mongo_id=html_dict.loc[i,'_id']
    
    table_input.update_one({"_id":mongo_id},{"$set":{
                         "resblock_location":location1['origin'],
                         "dest1_location":location1['destination'],
                         "dest2_location":location2['destination']}})
    
    print(i,end=',')

#关闭数据库
client.close()