# -*- coding: utf-8 -*-
"""
Created on Sun Sep  9 22:19:28 2018

@author: Annie
"""

import pandas as pd
import datetime
import pymongo
from get_html_district_sechand import save_df_to_csv

#对比不同城市的房源数据
def sechand_house_describe(house,city,datestr):
    df1=pd.DataFrame({'city':[city],'date':[datestr],'house_num':[len(house)]})
    df2=pd.DataFrame({'total_price_median':[house['total_price'].median()],
                    'total_price_mean':[house['total_price'].mean()],
                    'total_price_min':[house['total_price'].min()],
                    'total_price_max':[house['total_price'].max()]})
    df3=pd.DataFrame({'unit_price_median':[house['unit_price'].median()],
                    'unit_price_mean':[house['unit_price'].mean()],
                    'unit_price_min':[house['unit_price'].min()],
                    'unit_price_max':[house['unit_price'].max()]})
    df4=pd.DataFrame({'area_median':[house['area'].median()],
                    'area_mean':[house['area'].mean()],
                    'area_min':[house['area'].min()],
                    'area_max':[house['area'].max()]})
    df=pd.concat([df1,df2,df3,df4],axis=1)
    return df


class Para:
    city_datestr_list=[['sh','20180826'],['hz','20180826'],['sz','20180826'],
                       ['gz','20180826'],['nj','20180826'],['wh','20180826'],
                       ['cd','20180826'],['hui','20180826'],['cs','20180826'],
                       ['xa','20180826'],['xm','20180826']]
    district_path='./data_district' #小区域列表所在文件夹
    client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
    database = client['LianJia'] # 数据库的名字（mongo中没有这个数据库就创建）
    table = database['data_district_sechand']   #保存二手房数据的数据表
    save_file='../LianJiaSaveData/save_analyze_sechand'

if __name__ == '__main__':
    para=Para()
    for i in range(0,len(para.city_datestr_list)):
        print(i)
        city=para.city_datestr_list[i][0]
        datestr=para.city_datestr_list[i][1]
        
        house=pd.DataFrame(list(para.table.find({'city':city,'datestr':datestr}) ))       
        df=sechand_house_describe(house,city,datestr)
        
        if i==0:
            result=df
        else:
            result=result.append(df)
        #指定顺序
        result=result.loc[:,['city','date','house_num',
                             'total_price_median','total_price_mean','total_price_min','total_price_max',
                             'unit_price_median','unit_price_mean','unit_price_min','unit_price_max',
                             'area_median','area_mean','area_min','area_max']]
        print('%s/%s,%s,%s分析完成'%(i,len(para.city_datestr_list),city,datestr))
        
    todaystr=datetime.datetime.now().strftime('%Y%m%d')
    save_df_to_csv(result,'多城市分析结果对比'+todaystr,para.save_file)
