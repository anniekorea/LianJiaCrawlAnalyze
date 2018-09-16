# -*- coding: utf-8 -*-
"""
Created on Mon Sep  3 18:56:25 2018

@author: Annie
"""

import pandas as pd
import matplotlib.pyplot as plt
import csv
import pymongo
import os

def save_analyze_result(fh,result_txt,result):
    fh.write(result_txt)
    if isinstance(result,pd.core.series.Series):
        result=result.to_frame()
    result.insert(0,'index',result.index)
    writer = csv.writer(fh)#,dialect='excel')
    writer.writerow(result.columns)
    writer.writerows(result.values)
    #for i in range(0,len(result)):
    #    writer.writerow(result.iloc[i].values)
    fh.write('\r\n')
    #fh.close()

def analyze_summary(house):
    #总值
    txt = '%s共有%d套房源\r\n'%(city,len(house))
    #平均值
    txt = txt + '二手房平均面积为：%.2f平方米\r\n'%house['area'].mean()
    txt = txt + '二手房平均总价为：%.2f万元\r\n'%house['total_price'].mean()
    txt = txt + '二手房平均单价为：%.2f万元每平米\r\n\r\n'%house['unit_price'].mean()
    return txt
    

def analyze_total_price(house):
    txt1='总价最高的10套二手房:\r\n'
    price_head=house.sort_values('total_price',ascending=False).head(10)
    price_head=price_head[['id','area','house_type','total_price',
                          'unit_price','decration','direction','elevator',
                          'floor','resblock','district_cn','release_time',
                          'looked_times','follow','house_name']]
    txt1=txt1+price_head.to_string(line_width=100)+'\r\n\r\n'

    txt2='总价最低的10套二手房:\r\n'
    price_tail=house.sort_values('total_price').head(10)
    price_tail=price_tail[['id','area','house_type','total_price',
                          'unit_price','decration','direction','elevator',
                          'floor','resblock','district_cn','release_time',
                          'looked_times','follow','house_name']]
    txt2 = txt2 + price_tail.to_string(line_width=100)+'\r\n\r\n'
    
    txt = txt1 + txt2 +'\r\n'
    return txt

    
def analyze_house_num(house):
    txt='二手房源最多的10个小区:\r\n'
    house_num_head=house['resblock'].value_counts().head(10)
    txt=txt+house_num_head.to_string()+'\r\n\r\n'
    return txt
    
def analyze_house_type(house):    
    txt1='出现最多的10种户型:\r\n'
    house_type_head=house['house_type'].value_counts().head(10)
    txt1=txt1+house_type_head.to_string()+'\r\n\r\n'
    
    txt2='关注最多的10种户型:\r\n'
    follow_house_type=house.groupby('house_type').sum()['follow'].sort_values(ascending=False).head(10)
    txt2=txt2+follow_house_type.to_string()+'\r\n\r\n'
    
    txt=txt1+txt2
    return txt

def analyze_area(house):
    txt1='面积最大的二手房:\r\n'
    area_biggest=house.sort_values('area',ascending=False).iloc[0,:]
    txt1=txt1+area_biggest.to_string()+'\r\n\r\n'
    
    txt2='面积最小的二手房:\r\n'
    area_smallest=house.sort_values('area').iloc[0,:]
    txt2=txt2+area_smallest.to_string()+'\r\n\r\n'
    
    txt3='最普遍的二手房面积:\r\n'
    bins = range(0, 351, 10) # [0, 50, 100, 150, 200, 250, 300, 350]
    area_group = pd.cut(house['area'], bins, right=False)
    area_group = area_group.value_counts().head(5)
    area_group = area_group.to_frame(name='counts')
    area_group['percent']=area_group/len(house)*100
    txt3 = txt3 + area_group.to_string()+'\r\n\r\n'
    
    txt=txt1+txt2+txt3
    return txt

def analyze_follow(house):
    txt='关注人数最多的10套二手房:\r\n'
    follow_head=house.sort_values('follow',ascending=False).head(10)
    txt=txt+follow_head.to_string(line_width=100)+'\r\n\r\n'
    return txt
    
def analyze_resblock(house):
    txt1='按小区平均总价排序，总价最高的10个小区:\r\n'
    total_price_head=house.groupby('resblock').mean()['total_price'].sort_values(ascending=False).head(10)
    txt1 = txt1 + total_price_head.to_string()+'\r\n\r\n'

    txt2='按小区平均总价排序，总价最低的10个小区:\r\n'
    total_price_tail=house.groupby('resblock').mean()['total_price'].sort_values().head(10)
    txt2 = txt2 + total_price_tail.to_string()+'\r\n\r\n'
    
    txt3='按小区平均单价排序，单价最高的10个小区:\r\n'
    unit_price_head=house.groupby('resblock').mean()['unit_price'].sort_values(ascending=False).head(10)
    txt3 = txt3 + unit_price_head.to_string()+'\r\n\r\n'
      
    txt4='按小区平均单价排序，单价最低的10个小区:\r\n'
    unit_price_tail=house.groupby('resblock').mean()['unit_price'].sort_values().head(10)
    txt4 = txt4 + unit_price_tail.to_string()+'\r\n\r\n'
    
    txt=txt1+txt2+txt3+txt4
    return txt
    

def figure_total_price(house,save_file,city,datestr):
    fig=plt.figure()
    house['total_price'].hist(range=(0,1000),bins=40,rwidth=0.8) #range指定数据范围，超出范围的数据被忽略
    plt.title("二手房总价分布%s"%city)
    plt.ylabel('房源数')
    plt.xlabel('总价（万）')
    fig.savefig('%s/%s_%s_总价分布.jpg'%(save_file,city,datestr))

def figure_unit_price(house,save_file,city,datestr):
    fig=plt.figure()
    house['unit_price'].hist(range=(0,20),bins=40,rwidth=0.8) #range指定数据范围，超出范围的数据被忽略
    plt.title("二手房单价分布%s"%city)
    plt.ylabel('房源数')
    plt.xlabel('单价（万）')
    fig.savefig('%s/%s_%s_单价分布.jpg'%(save_file,city,datestr))
     
def figure_house_type(house,save_file,city,datestr):
    house_type_head=house['house_type'].value_counts().head(10)
    fig=plt.figure()
    plt.bar(house_type_head.index,house_type_head)
    plt.title("户型分布（出现最多的10种）%s"%city)
    plt.ylabel('房源数')
    plt.xlabel('户型')
    fig.savefig('%s/%s_%s_户型分布.jpg'%(save_file,city,datestr))

def figure_area(house,save_file,city,datestr):
    fig=plt.figure()
    house['area'].hist(range=(0,350),bins=35,rwidth=0.8) #大部分面积在350平米以下，小部分太大的面积会影响作图的分布效果
    plt.title("二手房面积分布%s"%city)
    plt.ylabel('房源数')
    plt.xlabel('面积（平方米）')
    fig.savefig('%s/%s_%s_面积分布.jpg'%(save_file,city,datestr))

def figure_unit_price_area(house,save_file,city,datestr):
    area_group_count,bins,patches=plt.hist(house['area'],range=(0,350),bins=35,rwidth=0.8)
    
    area_group = pd.cut(house['area'], bins, right=False)
    unit_price_area_group=house.groupby(area_group).mean()['unit_price']
    unit_price_area_group=unit_price_area_group[area_group_count>10]
    
    fig=plt.figure(figsize=(20,10))
    plt.bar(unit_price_area_group.index.map(str),unit_price_area_group)
    plt.title("不同面积的平均单价（房源数>10）%s"%city)
    plt.ylabel('平均单价')
    plt.xlabel('面积分组')
    plt.xticks(rotation=-90)
    for x, y in zip(unit_price_area_group.index.map(str),unit_price_area_group):
        plt.text(x, y+0.1, str(int(y*10)/10), ha='center')
    fig.savefig('%s/%s_%s_不同面积的平均单价.jpg'%(save_file,city,datestr))

def low_price_house_filter(house,less_pct):
    low_price_house=pd.DataFrame()
    for group in house.groupby('resblock'):
        house_resblock0=group[1]
        house_resblock=house_resblock0.copy()
        house_resblock['avg_unit_price']=house_resblock['unit_price'].mean()
        house_resblock['unit_price_pct']=house_resblock['unit_price']/house_resblock['unit_price'].mean()-1
        low_price=house_resblock[house_resblock['unit_price_pct']<-less_pct]
        if len(low_price)>0:
            if len(low_price_house)==0:
                low_price_house=low_price
            else:
                low_price_house=low_price_house.append(low_price, ignore_index=True)
    return low_price_house   

class Para:
    city_datestr_list=[['sh','20180826'],['hz','20180826']]
    #district_path='./data_district' #小区域列表所在文件夹
    client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
    database = client['LianJia'] # 数据库的名字（mongo中没有这个数据库就创建）
    table = database['data_district_sechand']   #保存二手房html数据的数据表
    save_file='../LianJiaSaveData/save_analyze_sechand'
    less_pct=0.2 #筛选低价房源，单价低于均价的10%
    
if __name__ == '__main__':
    #参数实例化
    para=Para()
    #%matplotlib qt4
    plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
      
    len_city=len(para.city_datestr_list)
    for k in range(0,len_city):
        city=para.city_datestr_list[k][0]
        datestr=para.city_datestr_list[k][1]

        
        house=pd.DataFrame(list(para.table.find({'city':city,'datestr':datestr}) ))       
        house.drop(['_id','city','datestr'],axis=1,inplace=True)
        house=house[['id','house_name','area','house_type','total_price','unit_price',
                     'decration','direction','elevator','floor','resblock','district_cn',
                     'release_time','looked_times','follow']]
        
        txt1=analyze_summary(house)
        txt2=analyze_total_price(house)
        txt3=analyze_house_num(house)
        txt4=analyze_house_type(house)
        txt5=analyze_area(house)
        txt6=analyze_follow(house)
        txt7=analyze_resblock(house)
        txt=txt1+txt2+txt3+txt4+txt5+txt6+txt7
        
        #若不存在保存文件的文件夹，则创建
        folder = os.path.exists(para.save_file)
        if not folder:
            os.makedirs(para.save_file)
        
        filename='%s/%s_%s_二手房分析结果.txt'%(para.save_file,city,datestr)
        fh = open(filename, 'w', encoding='utf-8',newline='')
        fh.write(txt)
        fh.close()
        
        figure_total_price(house,para.save_file,city,datestr)
        figure_unit_price(house,para.save_file,city,datestr)
        figure_house_type(house,para.save_file,city,datestr)
        figure_area(house,para.save_file,city,datestr)
        figure_unit_price_area(house,para.save_file,city,datestr)
        
        low_price_house=low_price_house_filter(house,para.less_pct)
        low_price_house=low_price_house.sort_values('unit_price_pct')
        low_price_house.to_csv('%s/%s_%s_单价低于均值%s%%的二手房.csv'%(para.save_file,city,datestr,int(para.less_pct*100)),
                      index=False,encoding='utf_8_sig')
        
        
        
