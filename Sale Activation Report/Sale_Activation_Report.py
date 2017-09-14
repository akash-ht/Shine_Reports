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
	#command="mysql -uanalytics -p@n@lytics -h172.16.66.64 -P3306 SumoPlus < "+str(sql)+"| sed 's/\t/,/g' > "+str(outFile)
	#command="mysql -uanalytics -p@n@lytics -h172.16.66.64 -P3306 SumoPlus < "+str(sql)+"| sed 's/\t/,/g; s/\"//g;' > "+str(outFile)
        try:
	    command="mysql -uanalytics -pAnal^tics@11 -h172.22.66.204 -P3306 SumoPlus < "+str(sql)+"| sed 's/\t/,/g; s/\"//g;' > "+str(outFile)
        except:
            command="mysql -uanalytics -pAnal^tics@11 -h172.22.65.157 -P3308 SumoPlus < "+str(sql)+"| sed 's/\t/,/g; s/\"//g;' > "+str(outFile)
	##command="mysql -uanalytics -pAnal^tics@11 -h172.22.65.157 -P3308 SumoPlus < "+str(sql)+"| sed 's/\t/,/g; s/\"//g;' > "+str(outFile)
	##command="mysql -uanalytics -pAnal^tics@11 -h172.22.65.157 -P3308 SumoPlus < "+str(sql)+"| sed 's/\t/,/g; s/\"//g;' > "+str(outFile)
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

	sql='/data/Analytics/Utils/FinanceReports/Model/SQL/sale_activation.sql'
	report='/data/Analytics/Utils/FinanceReports/Output/Sales_Activation_Data_Inception_To_Date_'+str(previousDate)+'.csv'
	generateReport(sql, report)
	uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/Sale_Activation_Report/"+previousDate.strftime("%b-%Y")
	status=moveData(report,uploadDir)

	finishTime = datetime.now()
	print "TimeTaken: "+str(finishTime-starttime)
if __name__ == '__main__':
	main()
