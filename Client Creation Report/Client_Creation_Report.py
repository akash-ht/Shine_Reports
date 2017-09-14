import os, sys, glob, time, zipfile, os.path, re
from datetime import date,datetime,timedelta
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
from dateutil.relativedelta import *
import smtplib
import ftplib
import socket
import traceback
import string
import xlrd
import unicodedata
import sendMailUtil

def generateReport(sql,outFile):
	command="mysql -uanalytics -pAnal^tics@11 -h172.22.65.157 -P3308 SumoPlus < "+str(sql)+"| sed 's/\t/,/g' > "+str(outFile)
	print command
	ret = os.system(command)
	return ret

def moveData(sourceFile,destination):
	if os.path.isdir(destination):
		print "destination directory present"
	else:
		os.mkdir(destination)
	moveCommand="mv "+sourceFile+" "+destination
	os.system(moveCommand)

todayDate=date.today()
previousDate = todayDate+relativedelta(days = -1)

def main():
	starttime = datetime.now()
	print '\n'+str(starttime)
	print 'SQL Code will Start Now'
	sql='/data/Analytics/Utils/FinanceReports/Model/SQL/client_creation.sql'
	report='/data/Analytics/Utils/FinanceReports/Output/Client_Creation_Inception_To_Date_'+str(previousDate)+'.csv'
	generateReport(sql, report)
	print 'Report Genertated From SQL Query'
	uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/Client_creation_report/"+previousDate.strftime("%b-%Y")
	status=moveData(report,uploadDir)

	finishTime = datetime.now()
	print "TimeTaken: "+str(finishTime-starttime)
if __name__ == '__main__':
	main()
