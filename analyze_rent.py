# -*- coding: utf-8 -*-
"""
Created on Wed Aug 29 22:51:50 2018

@author: Annie
"""

import pandas as pd
import pymongo
import matplotlib.pyplot as plt
import os

#低于均价的房源
def low_price_rent(rent,less_pct):
    low_price_rent=pd.DataFrame()
    for group in rent.groupby('resblock'):
        house_resblock0=group[1]
        house_resblock=house_resblock0.copy()
        house_resblock['avg_unit_price']=house_resblock['unit_price'].mean()
        house_resblock['unit_price_pct']=house_resblock['unit_price']/house_resblock['unit_price'].mean()-1
        low_price=house_resblock[house_resblock['unit_price_pct']<-less_pct]
        if len(low_price)>0:
            if len(low_price_rent)==0:
                low_price_rent=low_price
            else:
                low_price_rent=low_price_rent.append(low_price, ignore_index=True)
    return low_price_rent   

#不同户型的特征
def figure_house_type(rent,save_file,city,datestr):
    house_type_head=rent['house_type'].value_counts().head(10)
    fig=plt.figure()
    plt.bar(house_type_head.index,house_type_head)
    plt.title("不同户型房源数（出现最多的10种）%s"%city)
    plt.ylabel('房源数')
    plt.xlabel('户型')
    for x, y in zip(house_type_head.index,house_type_head):
        plt.text(x, y+2, str(int(y)), ha='center')
    fig.savefig('%s/%s_%s_不同户型房源数.jpg'%(save_file,city,datestr))
    
    rent_price_house_type=rent.groupby('house_type').mean()['unit_price']
    rent_price_house_type=rent_price_house_type[house_type_head.index]
    fig=plt.figure()
    plt.bar(rent_price_house_type.index,rent_price_house_type)
    plt.title("不同户型平均每平米月租（出现最多的10种）%s"%city)
    plt.ylabel('平均每平米月租')
    plt.xlabel('户型')
    for x, y in zip(rent_price_house_type.index,rent_price_house_type):
        plt.text(x, y+2, str(int(y)), ha='center')
    fig.savefig('%s/%s_%s_不同户型平均每平米月租.jpg'%(save_file,city,datestr))
    
    area_house_type=rent.groupby('house_type').mean()['area']
    area_house_type=area_house_type[house_type_head.index]
    fig=plt.figure()
    plt.bar(area_house_type.index,area_house_type)
    plt.title("不同户型平均面积（出现最多的10种）%s"%city)
    plt.ylabel('平均面积')
    plt.xlabel('户型')
    for x, y in zip(area_house_type.index,area_house_type):
        plt.text(x, y+2, str(int(y)), ha='center')
    fig.savefig('%s/%s_%s_不同户型平均面积.jpg'%(save_file,city,datestr))    

#不同面积的单价
def figure_area(rent,save_file,city,datestr):
    fig=plt.figure(figsize=(20,10))
    area_group_count,bins,patches=plt.hist(rent['area'],range=(0,350),bins=35,rwidth=0.8) #大部分面积在350平米以下，小部分太大的面积会影响作图的分布效果
    plt.title("不同面积房源数%s"%city)
    plt.ylabel('房源数')
    plt.xlabel('面积（平方米）')
    for x, y in zip(bins[:-1]+5,area_group_count):
        plt.text(x, y+5, str(int(y)), ha='center')
    fig.savefig('%s/%s_%s_不同面积房源数.jpg'%(save_file,city,datestr))
    
    #不同面积组的平均每平月租
    area_group = pd.cut(rent['area'], bins, right=False)
    unit_price_area_group=rent.groupby(area_group).mean()['unit_price']
    unit_price_area_group=unit_price_area_group[area_group_count>10]
    
    fig=plt.figure(figsize=(20,10))
    plt.bar(unit_price_area_group.index.map(str),unit_price_area_group)
    plt.title("不同面积的平均每平米月租（房源数>10）%s"%city)
    plt.ylabel('平均每平米月租')
    plt.xlabel('面积分组')
    plt.xticks(rotation=-90)
    for x, y in zip(unit_price_area_group.index.map(str),unit_price_area_group):
        plt.text(x, y+2, str(int(y)), ha='center')
    fig.savefig('%s/%s_%s_不同面积的平均每平米月租.jpg'%(save_file,city,datestr))    


#设置参数
class Para:
    city='sh'
    datestr='20180916'
    
    district_path='./data_district' #小区域列表所在文件夹
    
    client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
    database = client['LianJia'] # 数据库的名字（mongo中没有这个数据库就创建）
    table_input = database['data_district_rent']   #保存租房数据的数据表
    save_file='../LianJiaSaveData/save_analyze_rent'
    less_pct=0.2 

if __name__ == '__main__':
    para=Para()
    city=para.city
    datestr=para.datestr
    plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
    
    #从数据库读入数据
    rent=pd.DataFrame(list(para.table_input.find({'city':city,'datestr':datestr,'ziru':''})))  #非自如房
    rent=rent[['rent_id','link', 'rent_name','area','house_type','rent_price','unit_price',
               'direction','floor','resblock','built_year','small_district_cn','update_date']]
    
    #不同小区域的房租均价
    district_group=rent.groupby('small_district_cn')
    district_house_num=district_group.size().sort_values() #ascending=False
    district_avg_price=district_group.mean()['unit_price'].sort_values()
    
    #不同居民区的房租均价
    resblock_group=rent.groupby('resblock')
    resblock_house_num=resblock_group.size().sort_values() #ascending=False
    resblock_avg_price=resblock_group.mean()['unit_price'].sort_values()

    # 每平米价格低于小区平均单价
    low_price_rent=low_price_rent(rent,para.less_pct)
    
    #若不存在保存文件的文件夹，则创建
    folder = os.path.exists(para.save_file)
    if not folder:
        os.makedirs(para.save_file)
        
    #保存数据在一个excel文件中
    writer = pd.ExcelWriter('%s/%s_%s_链家租房分析结果.xlsx'%(para.save_file,city,datestr))
    low_price_rent.to_excel(writer,sheet_name='单价低于小区平均单价%s%%'%(para.less_pct*100))
    district_avg_price.to_excel(writer,sheet_name='小区域平均单价')
    resblock_avg_price.to_excel(writer,sheet_name='居民区平均单价')
    writer.save()
    
    #画分布图
    figure_house_type(rent,para.save_file,city,datestr)
    figure_area(rent,para.save_file,city,datestr)