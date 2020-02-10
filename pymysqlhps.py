#!/usr/bin/env python3

# License GNU Public License GPL-2.0 http://opensource.org/licenses/gpl-2.0
# Created by Eugene K., 2019

import pymysql
import datetime
import os
import time
#variables
interval=15 #seconds
mycnf="/root/.my.cnf"
mysqlpasswd=""
mysqluser="root"
mysqlhost="::1"
monitorquery='select ID,TIME,COMMAND,State,MAX_MEMORY_USED,info from INFORMATION_SCHEMA.PROCESSLIST WHERE Info IS NOT NULL AND Info NOT LIKE "%PROCESSLIST%" ORDER BY TIME ASC;'
snapdirectory="/var/log/sqlstats/"
#read passowrd from .my.cnf
mycnffile = open(mycnf, 'r')
lines = mycnffile.readlines()
for l in lines:
    if 'password' in l:
        mysqlpasswd=l.split('=')[1].replace('"', '').rstrip("\n\r")
mycnffile.close()
#establish mysql connection, it will NOT be closed while script is running
connection = pymysql.connect(host=mysqlhost, user=mysqluser, password=mysqlpasswd)
#open brief statistics file containint timestamp, number or running queries and time to fetch processlist
stfile=snapdirectory+"sqlprocesses.briefs.log"
stf=open(stfile,"a")
#get my PID ant print header
me=os.getpid()
print("#monitor PID:%i"%me,file=stf)
print("#Date-time\t\ttime to fetch, uS\tnumber of rows\tlongest running query time, S",file=stf)
try:
  while True:
#get time...
    hrtstamp=datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    fname=snapdirectory+"sqlprocesses-"+hrtstamp+".log"
    sname=snapdirectory+"sqlprocesses-"+hrtstamp+".short.log"
    t1=datetime.datetime.now()
#execute SQL query
    cursor=connection.cursor()
    cursor.execute(monitorquery)
    connection.commit()
#measire elapsed time
    t2=datetime.datetime.now()
    td=t2-t1
    nr=cursor.rowcount
#open STAMP files, print results...
    fsf=open(fname,"a")
    ssf=open(sname,"a")
    for row in cursor.fetchall():
         for i in range(0,len(row)-1):
             print("%s\t" % row[i], end = '',file=fsf)
             print("%s\t" % row[i], end = '',file=ssf)
         print("\t\t%s" % row[len(row)-1],file=fsf)
         print("",file=ssf)
         maxtime=row[1]
    fsf.close()
    ssf.close()
    print("%s\t%s\t\t\t%i\t\t%s" % (hrtstamp,str(round(1000000*td.total_seconds())),nr,maxtime),file=stf)
    stf.flush()
    time.sleep(interval)
    print(hrtstamp)
except:
    print("could not query sql processes or user interrupt received...")
    connection.close()
    stf.close()
    exit
