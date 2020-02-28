#Simple configuration file for pymysqlhps utility
interval=15 #seconds between processlist requerying
mysqlpasswd=""
mysqluser="root"
mysqlhost="127.0.0.1"
#my.cnf file to read pasword from, it can be used for obtaining password if this variable is not empty
#password will be taen from first 'password=' line in this file. Overrides mysqlpasswd variable
mycnf="/root/.my.cnf"
#query used to check processlist
#Warning. This query is written for Mariadb 10.4, please adjust it for other versions if required
#monitorquery='select ID,TIME,COMMAND,State,MAX_MEMORY_USED,info from INFORMATION_SCHEMA.PROCESSLIST WHERE Info IS NOT NULL AND Info NOT LIKE "%PROCESSLIST%" ORDER BY TIME ASC;'
monitorquery='select ID,TIME,COMMAND,State,MAX_MEMORY_USED,info from INFORMATION_SCHEMA.PROCESSLIST ORDER BY TIME ASC;'
snapdirectory="/var/log/sqlstats/"
#viewer will try to process files with following mask:
filemask=".*[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}.*.log"
