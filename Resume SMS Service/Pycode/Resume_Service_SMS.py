#SMS
import os, sys, glob, time, zipfile, os.path, re, zipfile
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
import xlrd, xlwt
import unicodedata
import sendMailUtil

host = '172.22.65.61'
username = 'shineftp'
password = '0B1u6st71S'
todayDate = date.today()
#todayDate = todayDate+relativedelta(days = -1)
previousDate = todayDate+relativedelta(days = -1)
projHome = '/data/Analytics/Utils/MarketingReports'

def generateReport(sql,outFile):
	command="mysql -uAnalytics -pAn@lytics -P3306 INDExportDB < "+str(sql)+"| sed 's/\t/,/g' > "+str(outFile)
	print command
	ret = os.system(command)

def run_sas_stmnt(sas_program, sas_log_file):
        sas_temp_file="/data/Analytics/temp"
        sas_query="/data/SAS/sasinstall_STAT/SASFoundation/9.2/sas -work "+str(sas_temp_file)+" -log "+str(sas_log_file)+" -SYSIN "+str(sas_program)+" -nonews -noterminal"
        print str(sas_query)
        os.system(str(sas_query))

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

def transferFile(host, username, password, files ,uploadLoc):
        r = 0
        isdir = 0
        ftp1 = ftplib.FTP()
        ftp1.connect(host)
        #print ftp1.getwelcome()
        try:
                print "Logging in..."
                ftp1.login(username, password)
        	for i in range(1):
                        directory=uploadLoc[uploadLoc.rfind("/")+1:]
                        loc=uploadLoc[:uploadLoc.rfind("/")]
                        #print "loc: "+loc, directory: "+directory
                        ftp1.cwd(loc)
                        filelist=files
                        ftp1.retrlines('LIST',filelist.append)
                        for file1 in filelist:
                            #print file, file1.split()[-1]
                            if file1.split()[-1]==directory or directory == '':
                                print "directory exist"
                                isdir = 1
                                break
                        print isdir
                        if isdir == 0:
                                ftp1.mkd(str(directory))
                        ftp1.cwd(str(directory))#move to the desired upload directory
                        #print "Currently in:", ftp1.pwd()

                        print "Uploading..."
                        for file in filelist:
				print file
                        	f = open(str(file), "rb")
	                        name = os.path.split(file)[1]
        	                #name=file[file.rfind("\\")+1:]#same as above
                	        ftp1.storbinary('STOR ' + name, f)
                        	f.close()

                        #print "All Files saved OK"
                        r = 1
                        #print ftp.retrlines('LIST')
        except:
              	traceback.print_exc()
		return
        finally:
                #print "Quitting..."
                ftp1.quit()
        #print "ok"
        del ftp1
        return r

def writeXLSfromCSV(infile,infile1):
	book = xlwt.Workbook()
        sheet1 = book.add_sheet('SMS')
        fp = open(infile, 'r')
        fileCount = 0
        count = 0
        limit = 65536
        for line in fp:
                #print line, line.strip('\n')
		line=line.strip('\n').split(',')
		Cellphone = line[0]; Email = line[1]
		if len(Email) > 19:	Email = Email[:17]+'..'
		#print Cellphone, Email
                sheet1.write(count - limit * fileCount, 0, Cellphone)
                sheet1.write(count - limit * fileCount, 1, "Dear "+Email+", Your Profile is registered with Shine.com & recruiters are viewing your CV. SMS RESUME to 54242 & get a professional resume by experts")
                count += 1
                if count % limit == 0:
                        fileCount += 1
                        book.save(infile.split('.')[0]+"_"+str(fileCount)+".xls")
                        book = xlwt.Workbook()
                        sheet1 = book.add_sheet('SMS')
	
        fileCount += 1
        book.save(infile.split('.')[0]+"_"+str(fileCount)+".xls")
        fp.close()
	'''
	sheet2 = book.add_sheet('SMS_1_4yr')
	file = open(infile1, 'r')
        fileCount = 0
        count = 0
        limit = 65536
        for line in file:
                #print line, line.strip('\n')
                line=line.strip('\n').split(',')
                Cellphone = line[0]; Email = line[1]
                if len(Email) > 19:     Email = Email[:17]+'..'
                #print Cellphone, Email
                sheet2.write(count - limit * fileCount, 0, Cellphone)
                sheet2.write(count - limit * fileCount, 1, "Dear "+Email+", Hiring season is up again! Increase your job opportunity with Shine.com's Paid Services, to avail type CV and send to 54242.") 
		count += 1
                if count % limit == 0:
                        fileCount += 1
                        book.save(infile.split('.')[0]+"_"+str(fileCount)+".xls")
                        book = xlwt.Workbook()
                        sheet2 = book.add_sheet('SMS_1_4yr')

        fileCount += 1
	file.close()
	book.save(infile.split('.')[0]+"_"+str(fileCount)+".xls")
	print infile.split('.')[0]+"_"+str(fileCount)+".xls"
	'''
	#zip
	

def main():
	starttime = datetime.now()
	print starttime
	
	CSV = '/data/Analytics/Utils/MarketingReports/Output/ResumeService/rServiceReport_'+previousDate.strftime("%Y-%m-%d")+'.csv'
	CSV_1_4 = '/data/Analytics/Utils/MarketingReports/Output/ResumeService/rServiceReport_1_4_'+previousDate.strftime("%Y-%m-%d")+'.csv'
	
	#os.system('cat /home/rService/SMS/Exclude/* > /data/Analytics/Utils/MarketingReports/Input/ResumeServiceExcludeList.csv')
	#sas_program=projHome+'/Model/SASCode/Resume_Service_SMS.sas'
	#sas_program=projHome+'/Model/SASCode/Resume_Service_SMS_v2.sas'
	sas_program=projHome+'/Model/SASCode/Resume_Service_SMS_v3.sas'
	#sas_program=projHome+'/Model/SASCode/Resume_Service_SMS_bk2.sas'
	#sas_program=projHome+'/Model/SASCode/Resume_Service_SMS_temp2.sas'
	sas_log_file=projHome+'/log/Resume_Service_SMS'+str(previousDate)+'.log'
	run_sas_stmnt(sas_program, sas_log_file)
	
	os.system('cp /data/Analytics/Utils/MarketingReports/Output/ResumeService/rServiceReport.csv '+CSV)
	os.system('cp /data/Analytics/Utils/MarketingReports/Output/ResumeService/rServiceReport_1_4yrs.csv '+CSV_1_4)
	writeXLSfromCSV(CSV,CSV_1_4)
	#return
	files = glob.glob(CSV.split('.')[0]+"*.xls")+glob.glob('/home/rService/SMS/*.xls')
	#print "files:",files
	#return
	transferSuccess = transferFile(host, username, password, files, '/')
	print 'file uploaded'
	uploadDir = '/data1/apacheRoot/html/shine/ReportArchieve/Resume_Service_SMS/'+previousDate.strftime("%b-%Y")
	for file in files:
		status=copyData(file, uploadDir)
		if transferSuccess and status:
			os.system('rm '+file)
	os.system('rm '+CSV)
	finishTime = datetime.now()
	print "TimeTaken: "+str(finishTime-starttime)
if __name__ == '__main__':
	main()
#r$erV!ce
