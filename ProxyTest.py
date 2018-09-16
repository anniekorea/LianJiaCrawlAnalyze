# -*- coding: utf-8 -*-
"""
Created on Thu Aug 16 09:57:41 2018

@author: Administrator
"""

# 仅爬取西刺代理首页IP地址
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.request import Request

def get_ip_list(url = 'http://www.xicidaili.com/'):
    headers = {'User-Agent': 'User-Agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}
    request = Request(url, headers=headers)
    response = urlopen(request)
    bsObj = BeautifulSoup(response, 'lxml')     # 解析获取到的html
    ip_text = bsObj.findAll('tr', {'class': 'odd'})   # 获取带有IP地址的表格的所有行
    ip_list = []
    for i in range(len(ip_text)):
        ip_tag = ip_text[i].findAll('td')   
        ip_port = ip_tag[1].get_text() + ':' + ip_tag[2].get_text() # 提取出IP地址和端口号
        ip_list.append(ip_port)
    print("共收集到了{}个代理IP".format(len(ip_list)))
    print(ip_list)
    return ip_list

ip_list=get_ip_list()


#筛选代理列表中对于指定网址可用的IP
#注意:指定网址为hhttps，则代理IP也必须为https；指定网址为hhttp，则代理IP也必须为http
import requests
#ip_list=['101.236.54.166:8866','114.113.126.83:80','101.132.122.230:3128']

url='https://sh.lianjia.com/ershoufang/'
#proxies={"http":"219.141.153.41:80","https":"118.24.150.126:3128"}
for i in range(0,len(ip_list)):
    proxies={"https":ip_list[i]}
    try:
        r=requests.get(url=url,proxies=proxies,timeout=2)
    except:
        #print('connect failed')
        pass
    else:
        print(proxies)
print('Done!')



#返回使用的代理IP是否生效
import requests
import re

proxies = {'https': '106.14.184.6:3128'}

if list(proxies.keys())==['http']:
    url = "http://txt.go.sohu.com/ip/soip"
elif list(proxies.keys())==['https']:
    url = "https://txt.go.sohu.com/ip/soip"

html1=requests.get(url,timeout=5).text
print('本机IP:'+str(re.findall(r'\d+.\d+.\d+.\d+',html1)))

html2=requests.get(url,proxies=proxies,timeout=5).text
print('代理IP:'+str(re.findall(r'\d+.\d+.\d+.\d+',html2)))
