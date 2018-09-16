# -*- coding: utf-8 -*-
"""
Created on Sun Jul 15 21:09:57 2018

@author: Annie
"""

import webbrowser
import pandas as pd
import urllib
import json

#http://lbs.amap.com/api/webservice/reference/staticmaps/#t6
#945ae54516979776ab3ee717012c24d4     #key

df = pd.read_table('WWT_Facility.txt')     #读取目标文件
df['lon']=6.0                               #插入lon列
df['lat']=7.0                               #插入lat列
# print df.head(5)                           #显示开始5行

def getHtml(url):          #<---获取网页信息--->
    page = urllib.urlopen(url_amap)                #访问网页
    data = page.readline()                         #读入数据
    data_dic = json.loads(data)                    #转换成python->字典
    data_dic_geocodes = data_dic['geocodes'][0]   #获取geocodes信息，也是以字典存储
    data__dic_location = data_dic_geocodes['location']  # 获取location信息
    location = str(data__dic_location).split(",")   #处理locaiton成为List
    #print location
    return location                                 #返回信息

def getStaticAmap(lonlat_str,str_city_center):      #<---获取静态高德地图--->
    # sh = '121.472644,31.231706'  # 上海中心点
    #高德地图-->静态地图API地址
    url = r'http://restapi.amap.com/v3/staticmap?location=%s&zoom=10&size=1024*768&key=<YOURKEY>'
    url_1 = url % str_city_center                                       #加入城市
    url_amap=url_1+'&markers=mid,0xFF0000,A:'+lonlat_str            #增加marker点
    print(url_amap)                                                 #
    webbrowser.open(url_amap)                                          #打开

def formatLocation(str,location):    #<---格式化Location-->
#格式是lon1,lat1;lon2,lat2;lon3,lat3....
    strTemp=','.join(location)                #lon,lat
    if len(str)==0:                           #判断是否是初始字符串
        strLocation=strTemp
    else:                                     #不是初始字符串
        strLocation=str+';'+strTemp           #合并字符串
    return strLocation                        #返回值

if __name__=='__main__':         #<---主程序--->
    countdf=len(df)                                 #df的行号,用以循环
    lonlat_str=''                                   #定义空白定符串
    str_city_center = '121.472644,31.231706'     #城市中心点lonlat
    i=0                                             #初始化i
    while i<15:                                    #循环,高德地图有<50个点限制
          df_1=df.ix[i,1]                           #名称
          df_2=df.ix[i,2]                           #地址
          df_3=df.ix[i,5]                           #处理量
          amap = r'http://restapi.amap.com/v3/geocode/geo?address=%s&city=上海&output=JSON&key=945ae54516979776ab3ee717012c24d4'
          url_amap=amap % df_2
          getLocation=getHtml(url_amap)             #获取数据,返回List[lon,lat]
          df.ix[i,6]=getLocation[0]                 #lon信息
          df.ix[i,7]=getLocation[1]                 #lat信息
          lonlat_str=formatLocation(lonlat_str,getLocation)   #定义marker点格式--->静态地图用
          # print lonlat_str                        #显示marker点
          # print df[i:i+1]                         #显示相关行数据
          i+=1
    # print df                                       #观察一下数据
    getStaticAmap(lonlat_str,str_city_center)        #打开静态地图
    df.to_csv('WWT_lonlat.csv', index=False, sep=',', encoding='utf-8')   #存储为csv格式