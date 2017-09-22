import poplib, sys,  datetime, email, csv
import os, re, sendMailUtil, string, ftplib, traceback, pymongo, MySQLdb, subprocess, smtplib, socket#, paramiko
from email import parser
from pymongo import Connection
from pymongo.objectid import ObjectId
from datetime import date, datetime, time
from dateutil.relativedelta import *
import datetime
from datetime import timedelta
todayDate=date.today()
import pandas as pd
import numpy as np
import pdb

#application_df = pd.read_csv(application_type)
#application_df.fillna(0)
#application_pivot = application_df.pivot_table(index = "Job_Id",values = "Candidate_Id",aggfunc = len,fill_value = 0)
#application_pivot.to_csv('/data/Shine/Shine_AdHoc/Output/tcs_applications.csv')

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
    	connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
	connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

mongo_conn = getMongoConnection('172.22.65.88',27018, 'sumoplus',True, True)
collection = getattr(mongo_conn, 'CandidateStatic')
counter = 0 
date1 = datetime.datetime.now() - datetime.timedelta(days= 1)	

def mail_files(*args):
    mailing_list=[]
    #mailing_list = []
    cc_list = ['jitender.kumar@hindustantimes.com','prateek.agarwal1@hindustantimes.com','akshay.gulati@hindustantimes.com']
    #cc_list = []
    bcc_list = ['himanshu.solanki@hindustantimes.com']
    #bcc_list = ['himanshu.solanki@hindustantimes.com']
    subject = "Registration Completion Report"
    content = "PFA.\n\nRegards,\nHimanshu\n\n\n\n*system generated email*"
    attachment = []
    for arg in args:
        attachment.append(arg)
    print 'Mailing file......'
    sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)

def getDataFromRecMongo(file1):
    ofile = open(file1,'a')
    cand_det = collection.find({'red':{'$gt':date1},'rsd':{'$gt':date1}},{'svi':1,'evi':1,'rsd':1,'red':1})    	            	  
    for cand1 in cand_det: 
        try:            
            Start_Vendor = str(cand1['svi'])
            End_Vendor = str(cand1['evi'])
            Registration_Start_Date = cand1['rsd']
            Registration_End_Date = cand1['red']
        except:
            Start_Vendor = 'not available'
	    End_Vendor = 'not available'
            Registration_Start_Date = 'not available'
            Registration_End_Date = 'not available'
        #Time_of_Registration = (Registration_End_Date - Registration_Start_Date).seconds
        #print Time_of_Registration
        try:
            Time_of_Registration = ((Registration_End_Date - Registration_Start_Date).seconds/60.0)
            if Time_of_Registration < 1 :
                Time_of_Registration = '<1min'
            elif Time_of_Registration > 1 and Time_of_Registration < 2 :
                Time_of_Registration = '1-2min'
            elif Time_of_Registration > 2 and Time_of_Registration < 3 :
                Time_of_Registration = '2-3min'
            else :
                Time_of_Registration = '3+'
            #print Time_of_Registration
        except:
            Time_of_Registration = 'not available'
        
        try:
            ofile.write(Start_Vendor+','+ End_Vendor + ',' + str(Registration_Start_Date) + ',' + str(Registration_End_Date) + ',' + str(Time_of_Registration) + '\n')        
        except:
            pass
    ofile.close()

def main():
    counter = 0 
    
    file1 = '/data/Shine/Shine_AdHoc/Output/fake_profile_identification_1.csv' 
    ofile = open(file1,'wb+')
    ofile.write('Start_Vendor,End_Vendor,Registration_Start_Date,Registration_End_Date,Time_of_Registration_Minutes' + '\n')
    ofile.close()
    getDataFromRecMongo(file1)       
    vendor_df = pd.read_csv(file1)
    vendor_df.fillna(0)
    vendor_pivot = vendor_df.pivot_table(index = "End_Vendor",values = "Start_Vendor",columns = ["Time_of_Registration_Minutes"],aggfunc = len,fill_value = 0)
    vendor_pivot.to_csv('/data/Shine/Shine_AdHoc/Output/fake_profile_identification.csv')    
    mail_files('/data/Shine/Shine_AdHoc/Output/fake_profile_identification.csv')
 
if __name__=='__main__':
    main()  

