from pymongo import *
from datetime import date, datetime, time
from email import parser
from dateutil.relativedelta import *
from pymongo import Connection
import os, re, sendMailUtil, string, ftplib, traceback, pymongo, MySQLdb, subprocess, smtplib, socket
import pdb
import sys
import csv

todayDate=date.today()
previousDate = todayDate + relativedelta(days = -1)
day1 = datetime.combine(previousDate, time(0, 0)) 
day2 = datetime.combine(todayDate, time(0, 0))
Month = previousDate.strftime("%b-%Y")

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
    	connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
	connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

def getDataFromCandidateMatch():
    directory = "/data/Shine/Shine_AdHoc/Output/DailyApplications/" + str(Month) + "/"
    if os.path.isdir(directory):
        print("Directory Exists")
    else:
        os.mkdir(directory)
    
    Outfile = directory + "Dailyapplications_" + str(previousDate)+".csv"
    ofile = open(Outfile,"w")
    writer = csv.writer(ofile,lineterminator = '\n')
    writer.writerow(['a', 'fjj', 'aa', 'fcu', 'md', 'h', 'ob', 'ds', 'r','im', 'at', 'rc', 'sb', '_id', 'fm', 'ad'])
    #mongo_conn = getMongoConnection('172.22.65.59', 27017, 'recruiter_master', True)
    mongo_conn = getMongoConnection('172.22.65.58', 27017, 'recruiter_master', True)	
    collection = getattr(mongo_conn, 'CandidateMatch')
    Applicationsdoc = collection.find({'ad':{'$gte':day1,'$lt':day2}})
    
    
    count = 0
    for app in Applicationsdoc:
        count +=1
        
        if count%10000 == 0:
            print count
        a = app.get('a',None)
        fjj = app.get('fjj',None)
        aa = app.get('aa',None)
        fcu = app.get('fcu',None)
        md = app.get('md',None)
        h = app.get('h',None)
        ob = app.get('ob',None)
        ds = app.get('ds',None)
        r = app.get('r',None)
        im = app.get('im',None)
        at = app.get('at',None)
        rc = app.get('rc',None)
        sb = app.get('sb',None)
        id = app.get('_id',None)
        fm = app.get('fm',None)
        ad = app.get('ad',None)
        writer.writerow([a,fjj,aa,fcu,md,h,ob,r,im,at,rc,sb,id,fm,ad])
    ofile.close()   
    
def main():
    print datetime.now()
    getDataFromCandidateMatch()
    print datetime.now()
	
if __name__=='__main__':
    main() 
	
