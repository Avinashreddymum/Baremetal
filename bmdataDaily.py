import csv
import time
import pymysql
import datetime
import warnings
from config import DailyDb
warnings.filterwarnings('ignore')
ts=time.time()
now=datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')




mydb = pymysql.connect(host=DailyDb.host,
    user=DailyDb.username,
    passwd= DailyDb.password,
    db=DailyDb.database)
cursor = mydb.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS kw1_daily(
TIME_STAMP DATE ,
GROUP_ VARCHAR(255),
HOSTNAME VARCHAR(255) UNIQUE,
flavor_name TEXT,
flavour_version VARCHAR(255),
status TEXT,
WORK_FLOW_NAME TEXT)""")
mydb.commit()

cursor.execute("""CREATE TABLE IF NOT EXISTS fd2_daily(
TIME_STAMP DATE ,
GROUP_ VARCHAR(255),
HOSTNAME VARCHAR(255) UNIQUE,
flavor_name TEXT,
flavour_version VARCHAR(255),
status TEXT,
WORK_FLOW_NAME TEXT)""")
mydb.commit()

cursor.execute("""CREATE TABLE IF NOT EXISTS ff1_daily(
TIME_STAMP DATE,
GROUP_ VARCHAR(255),
HOSTNAME VARCHAR(255) UNIQUE,
flavor_name TEXT,
flavour_version VARCHAR(255),
status TEXT,
WORK_FLOW_NAME TEXT)""")
mydb.commit()

cursor.execute("""CREATE TABLE IF NOT EXISTS sg2_daily(
TIME_STAMP DATE,
GROUP_ VARCHAR(255),
HOSTNAME VARCHAR(255) UNIQUE,
flavor_name TEXT,
flavour_version VARCHAR(255),
status TEXT,
WORK_FLOW_NAME TEXT)""")
mydb.commit()


cursor.execute("""CREATE TABLE IF NOT EXISTS hh3_daily(
TIME_STAMP DATE,
GROUP_ VARCHAR(255),
HOSTNAME VARCHAR(255) UNIQUE,
flavor_name TEXT,
flavour_version VARCHAR(255),
status TEXT,
WORK_FLOW_NAME TEXT)""")
mydb.commit()


cursor.execute("""CREATE TABLE IF NOT EXISTS lo8_daily(
TIME_STAMP DATE,
GROUP_ VARCHAR(255),
HOSTNAME VARCHAR(255) UNIQUE,
flavor_name TEXT,
flavour_version VARCHAR(255),
status TEXT,
WORK_FLOW_NAME TEXT)""")
mydb.commit()

cursor.execute("""CREATE TABLE IF NOT EXISTS sy1_daily(
TIME_STAMP DATE,
GROUP_ VARCHAR(255),
HOSTNAME VARCHAR(255) UNIQUE,
flavor_name TEXT,
flavour_version VARCHAR(255),
status TEXT,
WORK_FLOW_NAME TEXT)""")
mydb.commit()

cursor.execute("""CREATE TABLE IF NOT EXISTS os5_daily(
TIME_STAMP DATE,
GROUP_ VARCHAR(255),
HOSTNAME VARCHAR(255) UNIQUE,
flavor_name TEXT,
flavour_version VARCHAR(255),
status TEXT,
WORK_FLOW_NAME TEXT)""")
mydb.commit()


cursor.execute("""CREATE TABLE IF NOT EXISTS va1_daily(
TIME_STAMP DATE,
GROUP_ VARCHAR(255),
HOSTNAME VARCHAR(255) UNIQUE,
flavor_name TEXT,
flavour_version VARCHAR(255),
status TEXT,
WORK_FLOW_NAME TEXT)""")
mydb.commit()

import glob
import os

list_of_files = glob.glob('bmData/*.csv') # * means all if need specific format then *.csv
latest_file = max(list_of_files, key=os.path.getctime)
latest_file = latest_file.split("/")
file = (latest_file[-1])

with open('bmData/'+file, encoding = "utf-8") as csvfile:
    spamreader = csv.reader(csvfile)
    next(spamreader)
    next(spamreader)
    for row in spamreader:
        a=(" ".join(row).split('|'))
        if "name" ==a[0].strip() or "id" ==a[0].strip():
            pass
        else:
            next(spamreader)
            #zone =a[0][:3]
            server =a[0]
            server=server.replace("[admin only]", "")
            server = server.replace("+", "")
            server = server.replace("-", "")
            zone = server[:3]
            group_code = server[3]
            hostname =server[:5]+"-"+server[5:].strip()
            if len(a)<4:
                a.insert(1,"General Purpose 3 v1 ")
                a.insert(2,"DEPLOYDONE")
                a.insert(3,"StartActionServerWorkflow")
            flavor_name=a[1].replace("v1","")
            flavor_name=flavor_name.replace(" ","")
            #print(flavor_name)

            status = a[2].strip()
            workflow_name = a[3].strip()
            if flavor_name=="WorkloadOptimized1" or flavor_name=="WorkloadOptimized2":
                flavor_version = 0
            else:
                flavor_version = a[1].strip()[-1]

            a.insert(0, now)
            a.insert(1, group_code)
            a.insert(2, hostname)
            a.insert(3, flavor_name)
            a.insert(4, flavor_version)
            a.insert(5, status)
            a.insert(6,workflow_name)
            a.pop()
            a.pop()
            a.pop()
            a.pop()
            if a[2][:3] == "kw1":
                cursor.execute(r'INSERT IGNORE INTO kw1_daily (TIME_STAMP,GROUP_, HOSTNAME ,flavor_name,flavour_version,status,WORK_FLOW_NAME)VALUES(%s, %s, %s,%s, %s, %s,%s)',a)
                print("kw1 data inserted........")
                mydb.commit()
            if a[2][:3] == "fd2":
                cursor.execute(r'INSERT IGNORE INTO fd2_daily (TIME_STAMP,GROUP_, HOSTNAME ,flavor_name,flavour_version,status,WORK_FLOW_NAME)VALUES(%s, %s, %s,%s, %s, %s,%s)',a)
                print("fd2 data inserted........")
                mydb.commit()
            if a[2][:3] == "ff1":
                cursor.execute(r'INSERT IGNORE INTO ff1_daily (TIME_STAMP,GROUP_, HOSTNAME ,flavor_name,flavour_version,status,WORK_FLOW_NAME)VALUES(%s, %s, %s,%s, %s, %s,%s)',a)
                print("ff1 data inserted........")
                mydb.commit()
            if a[2][:3] == "sg2":
                cursor.execute(r'INSERT IGNORE INTO sg2_daily (TIME_STAMP,GROUP_, HOSTNAME ,flavor_name,flavour_version,status,WORK_FLOW_NAME)VALUES(%s, %s, %s,%s, %s, %s,%s)',a)
                print("sg2 data inserted........")
                mydb.commit()
            if a[2][:3] == "hh3":
                cursor.execute(r'INSERT IGNORE INTO hh3_daily (TIME_STAMP,GROUP_, HOSTNAME ,flavor_name,flavour_version,status,WORK_FLOW_NAME)VALUES(%s, %s, %s,%s, %s, %s,%s)',a)
                print("hh3 data inserted........")
                mydb.commit()
            if a[2][:3] == "lo8":
                cursor.execute(r'INSERT IGNORE INTO lo8_daily (TIME_STAMP,GROUP_, HOSTNAME ,flavor_name,flavour_version,status,WORK_FLOW_NAME)VALUES(%s, %s, %s,%s, %s, %s,%s)',a)
                print("lo8 data inserted........")
                mydb.commit()
            if a[2][:3] == "sy1":
                cursor.execute(r'INSERT IGNORE INTO sy1_daily (TIME_STAMP,GROUP_, HOSTNAME ,flavor_name,flavour_version,status,WORK_FLOW_NAME)VALUES(%s, %s, %s,%s, %s, %s,%s)',a)
                print("sy1 data inserted........")
                mydb.commit()
                pass
            if a[2][:3] == "os5":
                cursor.execute(r'INSERT IGNORE INTO os5_daily (TIME_STAMP,GROUP_, HOSTNAME ,flavor_name,flavour_version,status,WORK_FLOW_NAME)VALUES(%s, %s, %s,%s, %s, %s,%s)',a)
                print("os5 data inserted........")
                mydb.commit()

            if a[2][:3] == "va1":
                cursor.execute(r'INSERT IGNORE INTO va1_daily (TIME_STAMP,GROUP_, HOSTNAME ,flavor_name,flavour_version,status,WORK_FLOW_NAME)VALUES(%s, %s, %s,%s, %s, %s,%s)',a)
                print("va1 data inserted........")
                mydb.commit()

