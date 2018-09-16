# -*- coding: utf-8 -*-
"""
Created on Sun Sep  2 22:46:39 2018

@author: Annie
"""

#从已爬取下来的html文件中获取需要的房源信息
from lxml import etree
import pandas as pd
from get_html_district_sechand import save_df_mongodb
import pymongo

#提取需要的信息
#北京的数据格式不同，不适用
def parse_html_sechand_house(html):
    #使用lxml库的xpath方法对页面进行解析
    link=etree.HTML(html,parser=etree.HTMLParser(encoding='utf-8'))
        
    #1、提取房源总价（相同）
    priceInfo=link.xpath('//div[@class="priceInfo"]')  
    tp=[]
    for a in priceInfo:
        totalPrice=a.xpath('.//span/text()')[0]
        tp.append(float(totalPrice))

    #2、提取房源信息（与上海不同）
    #20180815:北京的houseInfo格式与其他不同，单独处理
    #上海的格式为'共富二村 | 2室2厅 | 81.01平米 | 南 | 精装 | 无电梯'(整体)
    #北京的格式为‘流星花园三区 /4室2厅/118.32平米/南 西 北/精装/有电梯’（5-6部分）
    houseInfo=link.xpath('//div[@class="houseInfo"]')   
    hi=[]
    for b in houseInfo:
        h=b.xpath('.//text()')
        house=h[0]
        for j in list(range(2,len(h),2)):
            house=house+'|'+h[j]
        hi.append(house) 
        
    #3、提取房源关注度(与上海不同)
    followInfo=link.xpath('//div[@class="followInfo"]')    
    fi=[]
    for c in followInfo:
        follow=c.xpath('./text()')[0]+'/'+c.xpath('./text()')[1]
        fi.append(follow)      
    
    #4、提取房源位置信息（与上海不同）
    positionInfo=link.xpath('//div[@class="positionInfo"]')
    pi=[]
    for d in positionInfo:
        if len(d.xpath('.//text()'))==5:
            position=d.xpath('.//text()')[0]+'/'+d.xpath('.//text()')[2]+'/'+d.xpath('.//text()')[4]
        elif len(d.xpath('.//text()'))==4:
            position=''+'/'+d.xpath('.//text()')[1]+'/'+d.xpath('.//text()')[3]    
        pi.append(position)
        
    #5、提取房源ID和名称信息（相同）
    #housecode=link.xpath('//div[@class="info clear"]/div[@class="title"]/a/@data-housecode')
    housecode=link.xpath('//div[@class="priceInfo"]/div[@class="unitPrice"]/@data-hid')
    housename=link.xpath('//div[@class="info clear"]/div[@class="title"]/a/text()')
    
    #创建数据表
    house=pd.DataFrame({'id':housecode,'total_price':tp,'house_info':hi,'follow_info':fi,'position_info':pi,
                        'house_name':housename})
    
    return house


#信息分列
def split_data_sechand_house(house):
    #1、对房源信息进行分列（相同）
    #一般是6列，但独栋别墅是7列，多出第二列“独栋别墅”
    houseinfo_replace=house['house_info'].copy()
    for x in range(len(houseinfo_replace)):
        houseinfo_replace[x]=houseinfo_replace[x].replace('|独栋', '独栋') #别墅会多出一列，单独处理
        houseinfo_replace[x]=houseinfo_replace[x].replace('|联排', '联排')
        houseinfo_replace[x]=houseinfo_replace[x].replace('|双拼', '双拼')
        houseinfo_replace[x]=houseinfo_replace[x].replace('|叠拼', '叠拼')
        houseinfo_replace[x]=houseinfo_replace[x].replace('|暂无数据别墅', '')
   
    try:    
        houseinfo_split = pd.DataFrame((x.split('|') for x in houseinfo_replace),
                                   columns=['resblock','house_type','area',
                                            'direction','decration','elevator'])
    except:
        houseinfo_split = pd.DataFrame((x.split('|') for x in houseinfo_replace),
                                   columns=['resblock','house_type','area',
                                            'direction','decration']) 
        houseinfo_split['elevator']=None
    
    #2、对房源关注度进行分列（与上海不同，少了release_time）
    followinfo_split = pd.DataFrame((x.split('/') for x in house['follow_info']),
                                    columns=['follow','looked_times'])  
    
    
    #3、对房源位置信息进行分列(与上海不同，分隔符是/)
    positioninfo_split=pd.DataFrame((x.split('/') for x in house['position_info']),
                                    columns=['floor','built_year','district_cn']) 
    positioninfo_split['floor']=positioninfo_split['floor']+positioninfo_split['built_year']
    del positioninfo_split['built_year']    
    
    #将分列后的关注度信息拼接回原数据表
    house_split=pd.concat([house,houseinfo_split,followinfo_split,positioninfo_split],axis=1)
    
    #然后再删除原先的列
    house_split=house_split.drop(['house_info','follow_info','position_info'],axis=1)
    
    house_split1=house_split.copy()
    #去除字符串的头尾空格
    for j in range(0,house_split1.columns.size):
        try:
            colname=house_split1.columns[j]
            house_split1.loc[house_split1.loc[:,colname].isnull()==False,colname]=\
            house_split1.loc[house_split1.loc[:,colname].isnull()==False,colname].map(str.strip)
        except:
            pass
    
    house_split1['area']=house_split1['area'].str[:-2].astype(float)
    house_split1['follow']=house_split1['follow'].str[:-3].astype(int)
    house_split1['unit_price']=house_split1['total_price']/house_split1['area']
    
    #print("共采集"+str(len(house))+"条房源信息")        
    return house_split1

#设置参数
class Para:
    city_datestr_list=[['bj','20180826']]

    district_path='./data_district' #小区域列表所在文件夹
    html_sechand_path='../LianJiaSaveData/html_district_sechand' #保存二手房html数据的文件夹
    #data_sechand_path='../LianJiaSaveData/data_sechand_house' #保存二手房整理后数据的文件夹
    
    client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
    database = client['LianJia'] # 数据库的名字（mongo中没有这个数据库就创建）
    table_input = database['html_district_sechand']   #保存二手房html数据的数据表
    table_output = database['data_district_sechand']   #保存二手房整理后数据的数据表
    
    
#准备工作：运行get_html_sechand_house，得到各小区域的二手房html
#解析html数据，并保存整理后的二手房数据

if __name__ == '__main__':
    #参数实例化
    para=Para()
    len_city=len(para.city_datestr_list)
    
    for k in range(0,len_city):
        city=para.city_datestr_list[k][0]
        datestr=para.city_datestr_list[k][1]
        
        if para.table_output.find_one({'city':city,'datestr':datestr}):
            print('%s,%s,数据已存在，跳过'%(city,datestr))
            continue
        else:         
            district_df=pd.read_csv('%s/链家二手房小区域列表%s.csv'%(para.district_path,city),engine='python',encoding='utf_8_sig')
            
            district_link_list=district_df['link']
            district_list=district_df['name_pinyin']
            
            print('%s,%s,共%s个小区域，读取中......'%(city,datestr,len(district_link_list)))
            
            #按小区域逐个处理其html文件
            for i in range(0,len(district_link_list)):
                if i<len(district_link_list)-1:
                    print(i,end=',')
                else:
                    print(i)
                
                district=district_list[i]
                
                #从数据库读取html文件
    
                try:
                    html_dict= para.table_input.find_one({'city':city,'datestr':datestr,'district':district})
                except Exception as ex:
                    print(ex)
                    break
                
                #解析html文件
                if not(html_dict==None):
                    html=html_dict['html']
                    if not(html==''):
                        house=parse_html_sechand_house(html)
                        house_split=split_data_sechand_house(house)
                        house_split['district']=district
                        
                        if i==0:
                            data_sechand=house_split
                        else:
                            data_sechand=data_sechand.append(house_split,ignore_index=True)
                
            
            
            #去重
            data_sechand_unique_id = data_sechand[~data_sechand['id'].duplicated()]
            
            #保存数据到csv文件
            #filename='house_%s_%s'%(para.city,para.datestr)
            #save_df_to_csv(file=data_sechand_unique_id,file_name=filename,save_path=para.data_sechand_path)
            #保存数据到数据库
            save_df_mongodb(data_sechand_unique_id,para.database,para.table_output,city,datestr)
            
            print('%s,%s已处理完！！！！！！'%(city,datestr))

    #关闭数据库
    para.client.close()
