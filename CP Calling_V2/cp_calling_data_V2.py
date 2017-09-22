import poplib, sys, datetime, email, math, operator, copy, itertools, random
import os, re, string, ftplib, traceback, pymongo, MySQLdb, subprocess, smtplib, socket
import multiprocessing
#from pymongo import MongoClient, ReadPreference
from datetime import date, datetime, time, timedelta
from dateutil.relativedelta import *
from bson.objectid import ObjectId
from multiprocessing import Pool, Lock
from multiprocessing.sharedctypes import Value, Array
import pandas as pd
#import scipy.sparse as sp
import numpy as np
#from sklearn.metrics.pairwise import cosine_similarity
from __builtin__ import range
from string import join
from logging import exception
import pdb
import math
import sendMailUtil
import csv

day1 = date.today()
day2 = day1 + relativedelta(days=-1)

def getMySqlConnection(HOST, PORT, DB_USER, DB_PASSWORD, DB):
    return MySQLdb.connect(host=HOST, port=PORT, user=DB_USER, passwd=DB_PASSWORD, db=DB)
    
def executeMySQLQuery(query):
    host = '172.22.65.63'
    port = 3306
    user = 'shinecrm_prod'
    pwd = 'Kgp28Za88CoU566'
    database = 'shinecrm_prod'
    dbconn = getMySqlConnection (host, port, user, pwd, database)
    cursor = dbconn.cursor()
    cursor.execute(query)
    rs = cursor.fetchall()
    dbconn.close()
    return rs

def createSlugDict():
    print "Calling Slug Dict"
    global slugDict
    q1 = "select name, slug from shinecp.cart_product"
    print "called q1"
    qfile = "/data/Shine/CP_Click_Report/Model/SQL/cp_lead_clickerReport_V2.sql"
    print "called qfile"
    ifile = open(qfile,'w')
    print "opened ifile"
    ifile.write(q1)
    print "written in ifile"
    ifile.close()
    print "ifile closed"
    outFile = "/data/Shine/CP_Click_Report/Output/cp_lead_clickerReport_V2.csv"
    options = ""
    DB = "shinecp"
    command = "mysql -uroot -h172.22.65.170 -P3311 -S /var/lib/cp_mysql/mysql5.sock "+options+" "+DB+" < "+str(qfile)+" |sed 's/\t/,/g'  > "+str(outFile)
    os.system(command)
    slugDict = {}
    with open(outFile) as inputFile:
        reader = csv.DictReader(inputFile,delimiter=',')
        for line in reader:
            slugDict[str(line['slug'])] = str(line['name'])
    inputFile.close()
    print "slugDict done"
    
def getEnquireNowLeads():
    global slugDict
    sqlFile = "/data/Shine/Shine_AdHoc/Model/SQL/enquire_now_everyday.sql"
    outFile1 = "/data/Shine/Shine_AdHoc/Output/enquire_now_everyday_temp.csv"
    options = ""
    DB = "shinecp"
    command = "mysql -uroot -h172.22.65.170 -P3311 -S /var/lib/cp_mysql/mysql5.sock "+options+" "+DB+" < "+str(sqlFile)+" |sed 's/\t/,/g'  > "+str(outFile1)
    print command
    os.system(command)
    outFile = "/data/Shine/Shine_AdHoc/Output/enquire_now_everyday.csv"
    ofile = open(outFile,'w')
    ofile.write('email,product\n')
    with open(outFile1) as inputFile:
        reader = csv.DictReader(inputFile,delimiter=',')
        for line in reader:
            try:
                email = str(line['email'])
                slug = str(line['url'])
                slug = slug.split(".htm")[0].split("/")[-2]
                slug = slugDict.get(slug,slug)
                ofile.write(email+','+str(slug)+'\n')
            except:
                pass
    inputFile.close()
    ofile.close()
    enquireDF = pd.read_csv(outFile)
    enquireDF = enquireDF.fillna("")
    #pdb.set_trace()
    enqDF = enquireDF.groupby('email',sort=False)['product'].apply('|||'.join).reset_index()
    enqDF.to_csv(outFile,encoding='utf-8',index=False)
    print "Enquire now done"
    
def getSRTlists():
    srt_source = pd.read_csv('/data/Shine/Shine_AdHoc/Input/Cold_Calling_Courses/srt_Source.csv')
    srt_source[srt_source.columns[0]] = srt_source[srt_source.columns[0]].apply(lambda x:str(x))
    srt_source = srt_source.set_index(srt_source.columns[0]).to_dict()[srt_source.columns[1]]
    srt_specialization = pd.read_csv('/data/Shine/Shine_AdHoc/Input/Cold_Calling_Courses/srt_Specialization.csv')
    srt_specialization = srt_specialization.set_index(srt_specialization.columns[0]).to_dict()[srt_specialization.columns[1]]
    srt_subfa = pd.read_csv('/data/Shine/Shine_AdHoc/Input/Cold_Calling_Courses/srt_Subfunction.csv')
    srt_subfa = srt_subfa.set_index(srt_subfa.columns[0]).to_dict()[srt_subfa.columns[1]]
    srt_industry = pd.read_csv('/data/Shine/Shine_AdHoc/Input/Cold_Calling_Courses/srt_Industry.csv')
    srt_industry = srt_industry.set_index(srt_industry.columns[0]).to_dict()[srt_industry.columns[1]]
    print "srts done"
    return srt_source, srt_specialization, srt_subfa, srt_industry
    
def getAtributesFromSAS():
    sas_program = "/data/Shine/Shine_AdHoc/Model/SASCode/enquire_now_everyday.sas"
    sas_log_file = "/data/Shine/Shine_AdHoc/Model/SASCode/enquire_now_everyday.log"
    if os.path.isfile(sas_log_file):
		os.system('rm '+sas_log_file)
    sas_temp_file="/data/Analytics/temp"
    sas_query="/data/SAS/sasinstall_STAT/SASFoundation/9.2/sas -work "+str(sas_temp_file)+" -log "+str(sas_log_file)+" -SYSIN "+str(sas_program)+" -nonews -noterminal"
    print str(sas_query)
    os.system(str(sas_query))
    leadsDF = pd.read_csv('/data/Shine/Shine_AdHoc/Output/enquire_now_everyday_data.csv')
    print "Total number of leads : " + str(leadsDF.shape[0]) 
    leadsDF = leadsDF.drop_duplicates().reset_index(drop = True)
    print "Unique leads for calling : " + str(leadsDF.shape[0])
    print "sas attribute done"
    return leadsDF
    
def getAttributesFromCRM(DF):
    #usernames = str(list(DF['sa_Email'])).strip('[]')
    #query = "select a.id, a.username,b.source, a.leadinsertdate from shinecrm_prod.crm_leads as a, shinecrm_prod.crm_leadsfile as b where "
    #query += "replace(replace(replace(replace(replace(replace(a.username,\" \",\"\"),\"\\\\\",\"\"),char(13),\"\"),char(10),\"\"),\"'\",\"\"),'\"','') in ("
    #query += usernames + ") and a.fileid = b.id group by a.id"
    #rows = executeMySQLQuery(query)
    #attribs = [(str(row[0]),str(row[1]),str(row[2]),row[3]) for row in rows]
    #attribsDF = pd.DataFrame(attribs,columns = ['id','sa_Email','source','leadinsertdate'])
    #attribsDF['leadinsertdate'] = attribsDF['leadinsertdate'].apply(lambda x:x.date())
    DF['source'] = 14
    DF['leadinsertdate'] = day2
    DF['da_DateOfBirth'].fillna('01JAN1900:00:00:00',inplace=True)
    DF['da_DateOfBirth'] = DF['da_DateOfBirth'].apply(lambda x:datetime.strptime(x,'%d%b%Y:%H:%M:%S').date())
    DF['da_DateOfBirth1'] = DF['leadinsertdate'] - DF['da_DateOfBirth']
    DF['da_DateOfBirth1'] = DF['da_DateOfBirth1'].apply(lambda x:x.astype('timedelta64[D]')/np.timedelta64(1,'D'))
    DF['da_DateOfBirth1'] = DF['da_DateOfBirth1'].apply(lambda x:34.56 if x > 36500 else float(x/365))
    DF['da_DateOfBirth1'].fillna(34.56,inplace=True)
    print "crm attributes"
    return DF
    
def calcProbabilities(leadsDF,srt_source,srt_specialization,srt_subfa,srt_industry,outFile):
    leadsDF['sa_SubFunction'] = leadsDF['sa_SubFunction'].apply(lambda x:float(69.82439164*srt_subfa.get(str(x),0)))
    leadsDF['sa_Specialization'] = leadsDF['sa_Specialization'].apply(lambda x:float(50.90754742*srt_specialization.get(str(x),0)))
    leadsDF['source'] = leadsDF['source'].apply(lambda x:float(24.88341952*srt_source.get(str(x),0)))
    leadsDF['sa_Industry'] = leadsDF['Industry'].apply(lambda x:float(48.18090886*srt_industry.get(str(x),0)))
    leadsDF['flag_source_40'] = leadsDF['source'].apply(lambda x:float(0.89408110) if str(x)=='40' else 0)
    leadsDF['flag_source_44'] = leadsDF['source'].apply(lambda x:float(-7.47872134) if str(x)=='44' else 0)
    leadsDF['TotalJobs'].fillna(0,inplace=True)
    leadsDF['TotalJobs'] = leadsDF['TotalJobs'].apply(lambda x:float(0.27512459*x))
    leadsDF['da_DateOfBirth1'] = leadsDF['da_DateOfBirth1'].apply(lambda x:float(0.02914829*x))
    leadsDF['intercept'] = float(-8.99282893)
    leadsDF['exponentialpower'] = leadsDF['sa_SubFunction']+leadsDF['sa_Specialization']+leadsDF['source']+leadsDF['sa_Industry']+leadsDF['flag_source_40']+leadsDF['flag_source_44']+leadsDF['TotalJobs']+leadsDF['da_DateOfBirth1']+leadsDF['intercept']
    leadsDF['probability'] = leadsDF['exponentialpower'].apply(lambda x:float(math.exp(x)/(1+math.exp(x))))
    outputDF = leadsDF[['EmailId','FirstName','LastName','ContactNumber','City','TotExperience','Salary','Industry','CourseName','probability']]
    outputDF = outputDF.sort(['probability'],ascending=[0]).reset_index(drop=True)
    outputDF = outputDF.drop('probability',1)
    outputDF.to_csv(outFile,encoding='utf-8',index=False)
    print "calc done"
    
def mailFiles(outFile):
    mailing_list = ['vinod@shine.com']
    cc_list = ['vicky.sarin@hindustantimes.com','akash.verma@hindustantimes.com']
    bcc_list = []
    subject = "Yesterday's Leads Calling Priority"
    content = "PFA.\n\nRegards,\nAnalytics Team\n\n\n\n*system generated email*"
    attachment = [outFile]
    print 'Mailing file......'
    sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)
    
def main():
    print datetime.now()
    st = datetime.now()
    print "Probability calculations for lead started at : "+ str(st)
    createSlugDict()
    print "createSlugDict Method Executed Successfully"
    getEnquireNowLeads()
    srt_source, srt_specialization, srt_subfa, srt_industry = getSRTlists()
    leadsDF = getAtributesFromSAS()
    leadsDF = getAttributesFromCRM(leadsDF)
    outFile = '/data/Shine/Shine_AdHoc/Output/cold_calling_courses'+date.today().strftime("%d-%m-%Y")+'.csv'
    calcProbabilities(leadsDF,srt_source,srt_specialization,srt_subfa,srt_industry,outFile)
    mailFiles(outFile)
    print "Probability calculations completed at : "+ str(datetime.now())
    print "Time Taken : " + str(datetime.now()-st)
    
if __name__=='__main__':
    main()
