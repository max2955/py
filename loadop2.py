import sys
import os
import datetime
from datetime import timedelta
from dateutil.rrule import rrule, DAILY
import pymysql
from sqlalchemy import create_engine


sqlengine = create_engine('mysql+pymysql://max:root@localhost/stock?charset=utf8mb4',echo=False)

try:
  sqlengine.execute('select 1 as is_alive')
except Exception as e:
  print("Something wrong, tring to start MySQL")
  os.system('net start mysql')
  pass



today = datetime.date.today()
print("today:"+str(today))

sqlengine.execute('CALL stock.trunc_before_cot_load();')

maxm = ( sqlengine.execute('select max(moment) from openpos2') ).fetchone()[0]
print("max in DB:"+str(maxm))


if maxm == today:
  print ("already filled")
  exit()

delta = timedelta(days=1)
maxm = maxm+delta



s1="c:/git/bin/curl -o C:/stock/data/"
s2=".txt --url \"https://www.moex.com/ru/derivatives/open-positions-csv.aspx?d="
s3="&t=2\""


d_start = maxm
d_end = today

d_i=d_start

while (d_i<=d_end):
   s_i=d_i.strftime("%Y%m%d")
   s_curl=s1+s_i+s2+s_i+s3
   print (s_curl)
   os.system(s_curl)
   d_i=d_i+delta


def processfile(fn): 
  with open("c:/Stock/tmp/"+fn+".ctl","w") as f:
    print(fn)
    f.write("LOAD DATA LOCAL INFILE \'c:/stock/data/"+fn+"'\n")
    f.write(" INTO TABLE stock.openpos \n")
    f.write(" FIELDS TERMINATED BY \';\'\n")
    f.write(" LINES TERMINATED BY \';\\n\'\n")
    f.write(" IGNORE 1 ROWS;\n")
  cmd="\"C:/MySQL/bin/mysql -umax -proot --local-infile stock < c:/Stock/tmp/"+fn+".ctl"   
  print(cmd) 
  os.system(cmd)


print ("Loading data into database") 

from os import listdir
from os.path import isfile, join
mypath="c:/Stock/data"
#onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
#for f in onlyfiles:
#	processfile(f)

for d_i in rrule(DAILY, dtstart=d_start, until=d_end):
    fn=d_i.strftime("%Y%m%d")+".txt"
    print ("Processing "+fn)
    processfile(fn)


print ("Updating loaded data")
sqlengine.execute('CALL stock.upd_cot_load();')
print ("Moving loaded data")
sqlengine.execute('CALL stock.move_cot_load();')

sqlengine.dispose()
