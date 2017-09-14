#!/usr/bin/python
import pymongo, os, string, os.path
import time
import datetime
from datetime import datetime, date, time
from dateutil.relativedelta import *
from pymongo import Connection
import unicodedata
#from pymongo.bson import BSON
import MySQLdb
import sys, traceback
import logging
import re
import bson

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
todayDate=date.today()
#todayDate=todayDate+relativedelta(days=-1)
previousDate=todayDate+relativedelta(days=-1)
#day1 = datetime.combine(previousDate, time(0, 0))
#day2 = datetime.combine(todayDate, time(0, 0))

def getDataFromMongo1(DB, COLLECTION, Type):#WhoViewedActivityMailer Data Extraction
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = True)
        mongo_conn = connection[DB]
        collection = getattr(mongo_conn,COLLECTION)
        print collection.find({'sent':True}).count()
        doclist = collection.find({'sent':True})
        emailDump = '/data/JobAlert/JobAlertDumps/'+previousDate.strftime("%b-%Y")+'/'+Type+'_'+str(previousDate)+'.csv'
        fp = open(emailDump,'w')
        for doc in doclist:
                try:
			if 'user_id' in doc.keys():
	                        UserId = doc['user_id']
			if 'email' in doc.keys():
				Email = doc['email']
			if 'rec_count' in doc.keys():
				RecCount = doc['rec_count']
			if 'view_count' in doc.keys():
				ViewCount = doc['view_count']
			if 'w3c_ts' in doc.keys():
                        	SentDate = doc['w3c_ts'][:10]
			#print UserId, Email, RecCount, ViewCount, SentDate
                        fp.write(UserId+','+Email+','+str(RecCount)+','+str(ViewCount)+','+SentDate+'\n')
                except Exception, err:
                        print 'Error at doc: '+str(doc)
                        sys.stderr.write('ERROR: %s\n' % str(err))
                        return
        fp.close()
        connection.drop_database(DB)

def getDataFromMongo(DB, COLLECTION, Type):#JobAlert Mailer Data Extraction
	connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = True)
        mongo_conn = connection[DB]
	collection = getattr(mongo_conn,COLLECTION)
	print collection.find({'s':True}).count()
	doclist = collection.find({'s':True})
	PATH = '/data/JobAlert/JobAlertDumps/'+previousDate.strftime("%b-%Y")
	if os.path.isdir(PATH):
        	print "destination directory present"
        else:
                os.mkdir(PATH)
	emailDump = '/data/JobAlert/JobAlertDumps/'+previousDate.strftime("%b-%Y")+'/JASent_'+Type+'_'+str(previousDate)+'.csv'
	fp = open(emailDump,'w')
	for doc in doclist:
		try:
			UserId = doc['c']
			sentDate = doc['t']
			sentDate = sentDate.strftime("%d-%m-%Y")
			mj = len(doc['mj'])
			oj = len(doc['oj'])
			fp.write(UserId+','+sentDate+','+str(mj)+','+str(oj)+','+Type+'\n')
		except Exception, err:
			if 't' not in doc.keys():#sentDate Not IN DB
				continue
			print 'Error at doc: '+str(doc)
	        	sys.stderr.write('ERROR: %s\n' % str(err))
			return
	fp.close()
	connection.drop_database(DB)
	command = 'cat '+emailDump+' >> /data/JobAlert/JobAlertDumps/'+previousDate.strftime("%b-%Y")+'/JASent_'+previousDate.strftime("%b")+'.csv'
	print command
	os.system(command)
	command = 'cat '+emailDump+' >> /data/JobAlert/JobAlertDumps/JASent.csv'
	print command
	os.system(command)

def main():
	#print previousDate
	#return
	starttime = datetime.now()
        print "\nExecution Started at: "+str(starttime)+" For "+str(previousDate)
	#dateFormat = '15_09_11'
	#dateFormat = '13sep'
	#dateFormat = previousDate.strftime("%d-%m-%y")
	#dateFormat = dateFormat.replace('-','_')
	#dateFormat = previousDate.strftime("%d%b")
	#dateFormat = dateFormat.lower()
	#dateFormat = previousDate.strftime("%b%d_%Y")
	dateFormat = previousDate.strftime("%d%b%Y")
	Month = previousDate.strftime("%B-%Y")
	print dateFormat, Month
	PATH = '/data/JobAlert/'+Month+'/'
	if os.path.isdir(PATH):
        	print "destination directory present"
        else:
                os.mkdir(PATH)
	#command = 'mv /data/JobAlert/mailer_*_run_'+dateFormat+'.tar.gz '+PATH
	command = 'mv /data/JobAlert/mailer_*MsgQueue_'+dateFormat+'_sent.tar.bz2 '+PATH
	print command
	os.system(command)
	type = [1, 1, 1, 0]
	if previousDate.weekday() == 0:
		type = [1, 1, 1, 1]
		command = 'mv /data/JobAlert/ActivityMailer_run_'+dateFormat+'.tar.gz '+PATH
        	print command
	        os.system(command)
	
	#Daily Mailer
	if type[0] == 1:
		Type = 'Daily'
		DB = 'mailer_daily'
		COLLECTION = 'DailyMsgQueue'
		#DB_NAME = DB+'_'+dateFormat
		#DB_NAME = DB+'_run_'+dateFormat
		DB_NAME = DB+'_'+dateFormat
		#print 'tar xvzf '+PATH+DB_NAME+'.tgz --directory '+PATH
		#os.system('tar xvzf '+PATH+DB_NAME+'.tgz --directory '+PATH)
	        #os.system('tar xvzf '+PATH+DB_NAME+'.tar.gz --directory '+PATH)
	        os.system('tar xvjf '+PATH+DB+'_'+COLLECTION+'_'+dateFormat+'_sent.tar.bz2 --directory '+PATH)
		#os.system('mongorestore -db '+DB_NAME+' '+PATH+DB_NAME+'/'+DB_NAME)
		#os.system('mongorestore -db '+DB_NAME+' '+PATH+DB_NAME+'/'+DB)
		os.system('mongorestore -db '+DB_NAME+' '+PATH+DB+'_'+COLLECTION+'_'+dateFormat+'_sent/'+DB)
		#os.system('mongorestore -db '+DB_NAME+' '+PATH+DB_NAME)
		getDataFromMongo(DB_NAME, COLLECTION, Type)
		os.system('rm -rf '+PATH+DB+'_'+COLLECTION+'_sent')

	#Weekly Mailer
	if type[1] == 1:
	        Type = 'Weekly'
        	DB = 'mailer_weekly'
	        COLLECTION = 'WeeklyMsgQueue'
		DB_NAME = DB+'_'+dateFormat
	        os.system('tar xvjf '+PATH+DB+'_'+COLLECTION+'_'+dateFormat+'_sent.tar.bz2 --directory '+PATH)
		os.system('mongorestore -db '+DB_NAME+' '+PATH+DB+'_'+COLLECTION+'_'+dateFormat+'_sent/'+DB)
	        getDataFromMongo(DB_NAME, COLLECTION, Type)
		os.system('rm -rf '+PATH+DB+'_'+COLLECTION+'_'+dateFormat+'_sent')

	#Monthly Mailer
	if type[2] == 1:
	        Type = 'Monthly'
        	DB = 'mailer_monthly'
	        COLLECTION = 'MonthlyMsgQueue'
		DB_NAME = DB+'_'+dateFormat
	        os.system('tar xvjf '+PATH+DB+'_'+COLLECTION+'_'+dateFormat+'_sent.tar.bz2 --directory '+PATH)
		os.system('mongorestore -db '+DB_NAME+' '+PATH+DB+'_'+COLLECTION+'_'+dateFormat+'_sent/'+DB)
	        getDataFromMongo(DB_NAME, COLLECTION, Type)
		os.system('rm -rf '+PATH+DB+'_'+COLLECTION+'_'+dateFormat+'_sent')

	#Activity Mailer (WhoViewed)
        if type[3] == 1:
                Type = 'WhoViewed'
                DB = 'ActivityMailer'
                COLLECTION = 'DailyActivityMailer'
                #DB_NAME = DB+'_'+dateFormat
                DB_NAME = DB+'_run_'+dateFormat
                #os.system('tar xvzf '+PATH+DB_NAME+'.tgz --directory '+PATH)
                os.system('tar xvzf '+PATH+DB_NAME+'.tar.gz --directory '+PATH)
                #os.system('mongorestore -db '+DB_NAME+' '+PATH+DB_NAME+'/'+DB_NAME)
                os.system('mongorestore -db '+DB_NAME+' '+PATH+DB_NAME+'/'+DB)
                #os.system('mongorestore -db '+DB_NAME+' '+PATH+DB_NAME)
                getDataFromMongo1(DB_NAME, COLLECTION, Type)
		os.system('rm -rf '+PATH+DB+'_'+COLLECTION+'_'+dateFormat+'_sent')
	finishtime = datetime.now()
	print "Total Time Taken: "+str(finishtime-starttime)+"\n"

if __name__ == '__main__':
	main()
