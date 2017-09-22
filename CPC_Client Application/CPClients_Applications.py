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

JobIdFile = "/data/Analytics/Utils/MarketingReports/Output/CPClients_ActiveJobsId_adhoc.csv"
LiveJobsApplications = "/data/Analytics/Utils/MarketingReports/Output/CPClients/CPClients_Applications.csv"

def generateReport(sql,outFile):
	#command="mysql -uanalytics -pAnal^tics@11 -h172.22.66.204 -P3306 SumoPlus < "+str(sql)+"| sed 's/^/\"/;s/$/\"/' > "+str(outFile)
	command="mysql -uanalytics -pAnal^tics@11 -h172.22.65.157 -P3308 SumoPlus < "+str(sql)+"| sed 's/^/\"/;s/$/\"/' > "+str(outFile)
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
        mongo_conn = getMongoConnection('172.22.65.59', 27017, 'recruiter_master', True)
        #mongo_conn = getMongoConnection('172.22.65.60', 27017, 'recruiter_master', True)
        #mongo_conn = getMongoConnection('172.22.65.170', 27017, 'recruiter_master')
        collection = getattr(mongo_conn, 'CandidateMatch')
        collection2 = getattr(mongo_conn, 'myparichayjobapply')
        ifile = open(JobIdFile, "r")
        ofile = open(LiveJobsApplications, "w")
	ofile.write("Company Name"+","+"Account Id"+","+"Job_Id"+","+"Matched Applications"+","+"Applications"+","+"FBApplications"+"\n");
        for line in ifile:
		line = line.split(',')
		Job_Id = line[0]
		Job_Id = int(Job_Id)
		Company_name = line[2]
		Company_name = Company_name.strip('\n')
		Account_id = line[1]
                MatchedApplications = collection.find({'fjj':Job_Id,'im':True}).count()
                Applications = collection.find({'fjj':Job_Id}).count()
                FBApplications = collection2.find({'j':Job_Id}).count()
                ofile.write(str(Company_name)+","+str(Account_id)+","+str(Job_Id)+","+str(MatchedApplications)+","+str(Applications)+","+str(FBApplications)+"\n")
        ifile.close()
        ofile.close()
        print "Got Application From Mongo"
	'''
        command = "mysql -uanalytics -p@n@lytics -h172.16.66.64 -e\"TRUNCATE TABLE temp.LiveJobsApplications_adhoc;\""
        print command
        os.system(command)
        command = "mysqlimport -uanalytics -p@n@lytics -h172.16.66.64 --local temp "+LiveJobsApplications+" --fields-terminated-by=',' --lines-terminated-by='\n'"
        print command
        os.system(command)
	'''
def main():
	starttime = datetime.now()
	print '\n'+str(starttime)
	todayDate = date.today()
	previousDate = todayDate+relativedelta(days = -1)
	#uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/Active_Jobs_Position/"+previousDate.strftime("%b-%Y")
	uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/Active_Jobs_Position/CPClients_Applications/"
	'''
	if os.path.isdir(uploadDir):
		print "Directory present"
	else:
		os.mkdir(uploadDir)
	'''

	#command="mysql -uanalytics -p@n@lytics -h172.16.66.64 --skip-column-names -e\"SELECT A.JobId,A.companyid_id,B.company_name FROM SumoPlus.recruiter_job A LEFT JOIN SumoPlus.backoffice_companyaccount B ON A.companyid_id = B.id WHERE A.companyid_id IN (4033,57766,170746,153131,190302,58941,184070,196438);\"|sed 's/\\t/,/g' > "+JobIdFile
	#command="mysql -uanalytics -pAnal^tics@11 -h172.22.66.204 --skip-column-names -e\"SELECT A.JobId,A.companyid_id,B.company_name FROM SumoPlus.recruiter_job A LEFT JOIN SumoPlus.backoffice_companyaccount B ON A.companyid_id = B.id WHERE A.companyid_id IN (4033,57766,170746,153131,190302,58941,184070,196438);\"|sed 's/\\t/,/g' > "+JobIdFile
	command="mysql -uanalytics -pAnal^tics@11 -h172.22.65.157 --skip-column-names -e\"SELECT A.JobId,A.companyid_id,B.company_name FROM SumoPlus.recruiter_job A LEFT JOIN SumoPlus.backoffice_companyaccount B ON A.companyid_id = B.id WHERE A.companyid_id IN (4033,57766,170746,153131,190302,58941,184070,196438);\"|sed 's/\\t/,/g' > "+JobIdFile
        print command
        os.system(command)
        UpdateApplicationFromMongo()	
	if os.path.isdir(uploadDir):
                print "Directory present"
	else:		
		print "Directory not present"
		os.mkdir(uploadDir)
	os.system('mv '+LiveJobsApplications+" "+uploadDir)
	finishTime = datetime.now()
	print "TimeTaken: "+str(finishTime-starttime)
if __name__ == '__main__':
	main()
