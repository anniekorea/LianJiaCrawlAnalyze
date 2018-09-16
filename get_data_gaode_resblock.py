# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 09:29:37 2018

@author: Administrator
"""
import pandas as pd
import pymongo
import requests
import json
from get_html_district_sechand import save_df_mongodb

#其他经纬度转换为高德经纬度
#链家用的是baidu的经纬度，要消除偏移，必须先转换成高德的经纬度
def transform(location,key):
    parameters = {'coordsys':'baidu','locations': location, 'key': key}
    base = 'http://restapi.amap.com/v3/assistant/coordinate/convert'
    response = requests.get(base, parameters)
    answer = response.json()
    return answer['locations']

#逆地理编码:通过经纬度获取地址
def regeocode(location,key):
    parameters = {'location': location, 'key': key}
    base = 'http://restapi.amap.com/v3/geocode/regeo'
    response = requests.get(base, parameters)
    answer = response.json()
    return answer['regeocode']['addressComponent']['district'],answer['regeocode']['formatted_address']

#地理编码:地址获取经纬度                    
def geocode(address,city,key):
    parameters = {'key': key,'address': address, 'city':city}
    base = 'http://restapi.amap.com/v3/geocode/geo'
    response = requests.get(base, parameters)
    answer = response.json()
    result={'location':answer['geocodes'][0]['location'],'address':answer['geocodes'][0]['formatted_address']}
    return result

#公交路线规划，获得两地点间距离和时间
def direction(origion,destination,city_cn,key):
    parameters = {'key': key,'origin': origion,'destination':destination,'city':city_cn}
    base = 'https://restapi.amap.com/v3/direction/transit/integrated'
    response = requests.get(base, parameters)
    html=response.content
    html_str=html.decode('utf-8')
    return html_str

def parse_direction(html_str):
    answer = json.loads(html_str)
    transits=answer['route']['transits']
    if transits==[]:
        if answer['route']['distance']==[]:
             result={'distance':0,'duration':0}
        else:
             result={'distance':int(answer['route']['distance'])/1000,'duration':0}    
    else:       
        result={'distance':int(transits[0]['distance'])/1000,'duration':int(int(transits[0]['duration'])/60)}
    return result



#x=transform('121.530817,31.224111')
#y=regeocode('121.530817,31.224111')
#home=geocode('竹园新村','上海')


class Para:
    keylist=['cb649a25c1f81c1451adbeca73623251',
             '7ec25a9c6716bb26f0d25e9fdfa012b8'] #可以添加自己申请的key，最多可以申请10个key
    city='sh'
    city_cn='上海'
    datestr='20180830'
    place1_cn='虹口星外滩'
    place2_cn='环球金融中心'
    
    #数据库存储方式设置：
    client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
    database = client['LianJia'] # 数据库的名字（mongo中没有这个数据库就创建）
    table_input = database['data_single_resblock']    
    table_output = database['data_gaode_resblock']

if __name__ == '__main__':
    para=Para()
    keylist=para.keylist
    
    single_resblock=pd.DataFrame(list(para.table_input.find({'city':para.city,'datestr':para.datestr})))
    
    location1=geocode(para.place1_cn,para.city_cn,key=keylist[0])
    location2=geocode(para.place2_cn,para.city_cn,key=keylist[0])
    
    location_list=single_resblock[['resblock_id','lon','lat']]
    len_list=len(location_list)
    print('共%s个小区数据待获取....'%len_list)
    
    route_result=pd.DataFrame(columns=['resblock_id','distance1','duration1','distance2','duration2'])
    
    k=0
    i=0
    error_log=''
    while i<len_list:
        print(i,end=',')
        resblock_id=location_list.iloc[i,0]
    
        location_str='%s,%s'%(location_list.loc[i,'lon'],location_list.loc[i,'lat'])
        location_gaode=transform(location_str,key=keylist[k])
        
        html_str1=direction(location_gaode,location1['location'],para.city_cn,key=keylist[k])
        html_str2=direction(location_gaode,location2['location'],para.city_cn,key=keylist[k])
        
        #返回值若正常
        if json.loads(html_str1)['info']=='OK' and json.loads(html_str2)['info']=='OK':
            route1=parse_direction(html_str1)
            distance1=route1['distance']
            duration1=route1['duration']
    
            route2=parse_direction(html_str2)
            distance2=route2['distance']
            duration2=route2['duration']
        
            route_both=pd.DataFrame({'resblock_id':location_list.iloc[i,0],
                                     'origion':location_gaode,
                                     'destination1':location1['location'],
                                     'destination2':location2['location'],
                                     'distance1':distance1,'duration1':duration1,
                                     'distance2':distance2,'duration2':duration2,
                                     'html_str1':html_str1,'html_str2':html_str2},index=[i])
        
            route_result=route_result.append(route_both)
            i=i+1
        #返回值缺参数（可能是经纬度有误，造成跨城市）    
        elif json.loads(html_str1)['info']=='MISSING_REQUIRED_PARAMS' or \
             json.loads(html_str2)['info']=='MISSING_REQUIRED_PARAMS':
            route_both=pd.DataFrame({'resblock_id':location_list.iloc[i,0],
                                     'origion':location_gaode,
                                     'destination1':location1['location'],
                                     'destination2':location2['location'],
                                     'distance1':0,'duration1':0,
                                     'distance2':0,'duration2':0,
                                     'html_str1':html_str1,'html_str2':html_str2},index=[i])
            
            print('%s未解析成功:MISSING_REQUIRED_PARAMS'%i)
            error_log=error_log+'%s未解析成功:MISSING_REQUIRED_PARAMS\n'%i
            route_result=route_result.append(route_both)
            i=i+1    
        #返回值显示超限额，可能是key超出当日限额了，换下一个key
        elif json.loads(html_str1)['info']=='DAILY_QUERY_OVER_LIMIT' or \
             json.loads(html_str2)['info']=='DAILY_QUERY_OVER_LIMIT':
                if k<len(keylist)-1:
                    k=k+1
                    print('转换key[%d]'%k)
                    continue
                else:
                    print('key已用完！')
                    break
    
    #最后汇总提示出错的数据项
    print(error_log)
    
    #保存数据到数据库
    save_df_mongodb(route_result,para.database,para.table_output,para.city,para.datestr)
                
    #关闭数据库
    para.client.close()



