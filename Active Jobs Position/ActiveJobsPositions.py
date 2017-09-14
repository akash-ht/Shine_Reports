import os, sys, glob, time, zipfile, os.path, re
from datetime import date,datetime,timedelta
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
from dateutil.relativedelta import *
from pymongo import Connection
import smtplib
import ftplib
import socket
import traceback
import string
import xlrd
import unicodedata
import sendMailUtil


JobIdFile = "/data/Analytics/Utils/MarketingReports/Model/Intermediate/ActiveJobsId.csv"
LiveJobsApplications = "/data/Analytics/Utils/MarketingReports/Model/Intermediate/LiveJobsApplications.csv"

def generateReport(sql,outFile):
	command="mysql -uanalytics -pAnal^tics@11 -h172.22.65.157 -P3308 SumoPlus < "+str(sql)+"| sed 's/^/\"/;s/$/\"/' > "+str(outFile)
	#command="mysql -uanalytics -pAnal^tics@11 -h172.22.66.204 -P3306 SumoPlus < "+str(sql)+"| sed 's/^/\"/;s/$/\"/' > "+str(outFile)
        print command
        ret = os.system(command)
        fp = open(outFile,"r+")
        result = fp.read()
        fp.close()
        result = result.replace('\t','","')
        fp = open(outFile,"w")
        fp.write(result)
        fp.close()
        return ret

def copyData(sourceFile,destination):
	if os.path.isfile(sourceFile):
		print "ok , file found"
		if os.path.isdir(destination):
			print "destination directory present"
		else:
			os.mkdir(destination)
		copyCommand="cp "+sourceFile+" "+destination
		os.system(copyCommand)
		fileName=sourceFile[sourceFile.rfind("/")+1:]
		destFile=destination+"/"+fileName
		print destFile
		if os.path.isfile(destFile):
			print "file copied successfull"
			return 1
		else:
			return 0

#def getMongoConnection(MONGO_HOST,MONGO_PORT,DB,slave_okay=False):
def getMongoConnection(MONGO_HOST,MONGO_PORT,DB,IsSlave=False):
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay=IsSlave)
        return connection[DB]

def UpdateApplicationFromMongo():
        print "Getting Application From Mongo"
        #mongo_conn = getMongoConnection('172.22.65.59', 27017, 'recruiter_master', True)
	mongo_conn = getMongoConnection('172.22.65.58', 27017, 'recruiter_master', True)
        #mongo_conn = getMongoConnection('172.22.65.60', 27017, 'recruiter_master', True)
        #mongo_conn = getMongoConnection('172.22.65.170', 27017, 'recruiter_master')
        collection = getattr(mongo_conn, 'CandidateMatch')
        collection2 = getattr(mongo_conn, 'myparichayjobapply')
        ifile = open(JobIdFile, "r")
        ofile = open(LiveJobsApplications, "w")
        for JobId in ifile:
                Job_Id = int(JobId.strip('\n'))
                MatchedApplications = collection.find({'fjj':Job_Id,'im':True}).count()
                 
                Applications = collection.find({'fjj':Job_Id}).count()
                 
                FBApplications = collection2.find({'j':Job_Id}).count()

                ofile.write(str(Job_Id)+","+str(MatchedApplications)+","+str(Applications)+","+str(FBApplications)+"\n")
        print MatchedApplications
	print Applications 
	print FBApplications
	ifile.close()
        ofile.close()
        print "Got Application From Mongo"
        #command = "mysql -uanalytics -p@n@lytics -h172.16.66.64 -e\"TRUNCATE TABLE ShineReport.LiveJobsApplications;\""
	command = "mysql -uanalytics -pAnal^tics@11 -h172.22.65.157 -P3308 -e\"TRUNCATE TABLE ShineReport.LiveJobsApplications;\""
        print command
        os.system(command)
        #command = "mysqlimport -uanalytics -p@n@lytics -h172.16.66.64 --local ShineReport "+LiveJobsApplications+" --fields-terminated-by=',' --lines-terminated-by='\n'"
	command = "mysqlimport -uanalytics -pAnal^tics@11 -h172.22.65.157 -P3308 --local ShineReport "+LiveJobsApplications+" --fields-terminated-by=',' --lines-terminated-by='\n'"
        print command
        os.system(command)

def main():
	starttime = datetime.now()
	print '\n'+str(starttime)
	todayDate = date.today()
	previousDate = todayDate+relativedelta(days = -1)
	'''
	tokenFile3 = "/data/Analytics/Utils/token/replication/recruiter_"+str(todayDate)+".txt"
        tokenFp3 = open(tokenFile3, "r")
        token3 = tokenFp3.read()
        print token3
        if token3 != "1":# or token2 != "1":
                print "DB Not Updated at "+str(starttime)+"Quitting..\n"
                return
	'''

	#uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/Active_Jobs_Position/"+previousDate.strftime("%b-%Y")
	uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/Active_Jobs_Position/"+todayDate.strftime("%b-%Y")
	if os.path.isdir(uploadDir):
		print "Directory present"
	else:
		os.mkdir(uploadDir)

	#command="mysql -uanalytics -p@n@lytics -h172.16.66.64 --skip-column-names -e\"SELECT JobId FROM SumoPlus.recruiter_job WHERE jobstatus IN (3, 9);\"|sed 's/\\t/,/g' > "+JobIdFile
	command="mysql -uanalytics -pAnal^tics@11 -h172.22.65.157 -P3308 --skip-column-names -e\"SELECT JobId FROM SumoPlus.recruiter_job WHERE jobstatus IN (3, 9);\"|sed 's/\\t/,/g' > "+JobIdFile
        print command
        os.system(command)
        UpdateApplicationFromMongo()
	#ActiveJobPositionSql="/data/Analytics/Utils/MarketingReports/Model/SQL/ActiveJobsPosition2.sql"
	#ActiveJobPositionSql="/data/Analytics/Utils/MarketingReports/Model/SQL/ActiveJobsPosition3.sql"
	ActiveJobPositionSql="/data/Analytics/Utils/MarketingReports/Model/SQL/ActiveJobsPosition5.sql"
	ActiveJobPosition="/data/Analytics/Utils/MarketingReports/Output/ActiveJobsPosition/"#+todayDate.strftime("%d-%m-%Y")+"/"
	if os.path.isdir(ActiveJobPosition):
		print "Directory present"
	else:
		os.mkdir(ActiveJobPosition)
	ActiveJobPosition=ActiveJobPosition+todayDate.strftime("%Y-%m-%d")+"_ActiveJobsPosition.csv"
	ret=generateReport(ActiveJobPositionSql,ActiveJobPosition)
	print "ret:"+str(ret)

	if os.path.isfile(ActiveJobPosition):
		#transferFile_mum([dailyLoginReport],uploadDir)
		status=copyData(ActiveJobPosition,uploadDir)#destination)
		os.system('rm '+ActiveJobPosition)
	else:
		print "Error creating data reports"
	finishTime = datetime.now()
	print "TimeTaken: "+str(finishTime-starttime)
if __name__ == '__main__':
	main()
