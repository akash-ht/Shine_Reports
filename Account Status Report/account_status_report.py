import os, sys, glob, time, zipfile, os.path, re
from datetime import date,datetime,timedelta
from dateutil.relativedelta import *
import ftplib
import string
import sendMailUtil

todayDate=date.today()
previousDate = todayDate+relativedelta(days = -1)

def generateReport(host, username, password, sql, outFile, DB, options=''):
	#command="mysql -u"+username+" -p"+password+" "+options+" "+DB+" < "+str(sql)+"| sed 's/\t/,/g' > "+str(outFile)
	command="mysql -h"+host+" -u"+username+" -p"+password+" "+options+" "+DB+" < "+str(sql)+"| sed 's/\t/,/g' > "+str(outFile)
	print command
	ret = os.system(command)
	return ret

def copyData(sourceFile,destination):
	if os.path.isfile(sourceFile):
		print "ok , file found"
		checkDir(destination)
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

def checkDir(dir):	
	if not os.path.isdir(dir):	os.mkdir(dir) 

def account_status_report():
	uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/Account_Status_Report/"+previousDate.strftime("%b-%Y")
	checkDir(uploadDir)

        Sql="/data/Shine/SaleReports/Model/SQL/account_status.sql"
        PATH="/data/Shine/SaleReports/Output/Account_Status_Report"	#+todayDate.strftime("%d-%m-%Y")+"/"
        report=PATH+'/account_status_report_'+previousDate.strftime("%Y-%m-%d")+".csv"
        #ret=generateReport('172.16.66.64', 'analytics', '@n@lytics', Sql, report, 'SumoPlus')
	#ret=generateReport('172.22.66.204', 'analytics', 'Anal^tics@11', Sql, report, 'SumoPlus')
	ret=generateReport('172.22.65.157', 'analytics', 'Anal^tics@11', Sql, report, 'SumoPlus')
        print "ret:"+str(ret)

        if os.path.isfile(report):
                status=copyData(report,uploadDir)
		os.system('rm '+report)
        else:
                print "Error creating resume offer leads reports"

def main():
	starttime = datetime.now()
	print '\n'+str(starttime)
	account_status_report()

	finishTime = datetime.now()
	print "TimeTaken: "+str(finishTime-starttime)
if __name__ == '__main__':
	main()
