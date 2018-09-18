#!/usr/bin/python
#coding=utf-8

import os
import datetime
import sys


#此脚本用于填充历史数据,就是根据输入的日期执行shell脚本
#注:第一个参数是开始日期,第二个参数是结束日期,为 前闭后开 规则
beginDay=sys.argv[1]
endDay=sys.argv[2]
#将字符串转换为日期
begin=datetime.datetime.strptime(beginDay,"%Y-%m-%d")
end=datetime.datetime.strptime(endDay,"%Y-%m-%d")
temp=begin
print "开始日期:",begin
print "结束日期:",end

while temp < end:
    tempStr=temp.strftime("%Y-%m-%d")
    os.system("sh 131_history.sh " + tempStr)
    os.system("sh 149_history.sh " + tempStr)
    print "成功运行的日期:",tempStr
    temp+=datetime.timedelta(days=1)




print "success"






