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

todayDate=date.today()
previousDate = todayDate+relativedelta(days = -1)

def run_sas_query(sas_program, sas_log_file):
        sas_temp_file="/data/Analytics/temp"
        sas_query="/data/SAS/sasinstall_STAT/SASFoundation/9.2/sas -work "+str(sas_temp_file)+" -log "+str(sas_log_file)+" -SYSIN "+str(sas_program)+" -nonews -noterminal"
        print str(sas_query)
        os.system(str(sas_query))

def generateReport(username, password, sql, outFile, DB, options='',host='localhost'):
	command="mysql -h"+host+" -u"+username+" -p"+password+" "+options+" "+DB+" < "+str(sql)+"| sed 's/\t/,/g' > "+str(outFile)
	print command
	ret = os.system(command)
	return ret

def uploadData(sourceFile,destination):
	checkDir(destination)
        if os.path.isfile(sourceFile):
                print "ok , file found"
                checkDir(destination)
                copyCommand="mv "+sourceFile+" "+destination
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

def internal_sem_report():
        sas_program = '/data/Shine/MarketingReports/Model/SASCode/internal_sem.sas'
        sas_log_file = '/data/Shine/MarketingReports/log/internal_sem_'+str(previousDate)+'.log'
        run_sas_query(sas_program, sas_log_file)

        report = '/data/Shine/MarketingReports/Output/'+previousDate.strftime("%Y-%m-%d")+'_internal_SEM.csv'
	uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/Internal_SEM_Report/"+previousDate.strftime("%b-%Y")
        uploadData(report,uploadDir)

def welcome_call_report():
	uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/Welcome_call_report/"+previousDate.strftime("%b-%Y")
        checkDir(uploadDir)

        WelcomeSql="/data/Analytics/Utils/MarketingReports/Model/SQL/welcomeCall.sql"
        Welcome="/data/Analytics/Utils/MarketingReports/Output/WelcomeCallReport/"#+todayDate.strftime("%d-%m-%Y")+"/"
        Welcome=Welcome+previousDate.strftime("%Y-%m-%d")+"_welcomeCallReport.csv"
        ret=generateReport('Analytics', 'An@lytics', WelcomeSql, Welcome, "shine")
        print "ret:"+str(ret)

        if os.path.isfile(Welcome):
                status=uploadData(Welcome,uploadDir)#destination)
                mailing_list=['Gurpreet.Shine@gmail.com']#,'raja.gupta@temmas.in']#'Rajagupta1@hotmail.com']
                cc_list=['Divya.Saxena@teammas.in','Dinesh.Arora@hindustantimes.com','Abhineet.Sonkar@hindustantimes.com']
                bcc_list = ['himanshu.solanki@hindustantimes.com','akash.verma@hindustantimes.com']
                #sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,'shine_Data_reports:Welcome Call Report generated successfully',"PFA Welcome report generated on "+todayDate.strftime("%d-%m-%Y")+". You can also download it from 172.22.65.170 (report Interface)",[Welcome])
                os.system('rm '+Welcome)
        else:
                print "Error creating welcome call reports"

def resume_offer_report():
	uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/Resume_Offer_Leads/"+previousDate.strftime("%b-%Y")
	checkDir(uploadDir)

        Sql="/data/Analytics/Utils/MarketingReports/Model/SQL/resume_offer_leads.sql"
        PATH="/data/Analytics/Utils/MarketingReports/Output/Resume_Offer_Leads"#+todayDate.strftime("%d-%m-%Y")+"/"
        report=PATH+'/resume_offer_leads_'+previousDate.strftime("%Y-%m-%d")+".csv"
        ret=generateReport('analytics', 'AnalyTicS\@879', Sql, report, 'SumoPlus', '-S /var/lib/mysql/mysql3.sock')
        print "ret:"+str(ret)

        if os.path.isfile(report):
                status=uploadData(report,uploadDir)
		os.system('rm '+report)
        else:
                print "Error creating resume offer leads reports"

def Convergys_Lead_Report():
	uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/Convergys_Lead_Report/"+previousDate.strftime("%b-%Y")
	checkDir(uploadDir)

        Sql="/data/Analytics/Utils/MarketingReports/Model/SQL/Convergys_Lead_Report.sql"
        PATH="/data/Analytics/Utils/MarketingReports/Output/Convergys_Lead_Report"#+todayDate.strftime("%d-%m-%Y")+"/"
        report=PATH+'/Convergys_Lead_Report_'+previousDate.strftime("%Y-%m-%d")+".csv"
        ret=generateReport('analytics', 'AnalyTicS\@879', Sql, report, 'SumoPlus', '-S /var/lib/mysql/mysql3.sock')
        print "ret:"+str(ret)

        if os.path.isfile(report):
                status=uploadData(report,uploadDir)
        else:
                print "Error creating Convergys lead reports"

def Registration_Tracker():
        sas_program = '/data/Shine/MarketingReports/Model/SASCode/registration_tracker.sas'
        sas_log_file = '/data/Shine/MarketingReports/log/registration_tracker/registration_tracker_'+str(previousDate)+'.log'
        run_sas_query(sas_program, sas_log_file)

        report = '/data/Shine/MarketingReports/Output/Registration_Tracker_'+str(previousDate)+'.xml'
        uploadDir = '/data1/apacheRoot/html/shine/ReportArchieve/Registration_tracker/'+previousDate.strftime("%b-%Y")
        uploadData(report,uploadDir)

def Fresher_DesiredFunction_Report():
	sas_program = '/data/Shine/MarketingReports/Model/SASCode/Fresher_DesiredFunction_Report.sas'
	sas_log_file = '/data/Shine/MarketingReports/log/Fresher_DesiredFunction_Report_'+str(previousDate)+'.log'
	run_sas_query(sas_program, sas_log_file)

	report = '/data/Shine/MarketingReports/Output/Fresher_DesiredFunction_'+str(previousDate)+'.csv'
	uploadDir = '/data1/apacheRoot/html/shine/ReportArchieve/Fresher_Desired_Function/'+previousDate.strftime("%b-%Y")
        uploadData(report,uploadDir)

def Vendor_Pivot():
	sas_program = '/data/Shine/MarketingReports/Model/SASCode/VendorPivot_v2.sas'
	sas_log_file = '/data/Shine/MarketingReports/log/VendorPivot/VendorPivot_'+str(previousDate)+'.log'
	run_sas_query(sas_program, sas_log_file)

	report = '/data/Shine/MarketingReports/Output/'+str(previousDate)+'_vendor_pivot.csv'
	uploadDir = '/data1/apacheRoot/html/shine/ReportArchieve/Vendor_Pivot/'+previousDate.strftime("%b-%Y")
        uploadData(report,uploadDir)

def ITExp1plus():
	sas_program = '/data/Shine/Shine_AdHoc/Model/SASCode/ITExpGT1Yr_Ankur.sas'
	sas_log_file = '/data/Shine/Shine_AdHoc/Model/SASCode/ITExpGT1Yr_Ankur.log'
	run_sas_query(sas_program, sas_log_file)

	report = '/data/Shine/Shine_AdHoc/Output/'+str(previousDate)+'_ITExp1plus.csv'
	uploadDir = '/data1/apacheRoot/html/shine/ReportArchieve/ITExp1plus/'+previousDate.strftime("%b-%Y")
        uploadData(report,uploadDir)

DayOfWeek = {0:'Mon',1:'Tue',2:'Wed',3:'Thu',4:'Fri',5:'Sat',6:'Sun'}
def RecruiterLeads_ShineMyParichay():
        global todayDate
        dayOfWeek = DayOfWeek[todayDate.weekday()]
        if dayOfWeek not in ['Tue', 'Fri']:
                return
        uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/RecruiterLeads_ShineMyParichay/"+todayDate.strftime("%b-%Y")
        checkDir(uploadDir)

        RecruiterLeads_ShineMyParichay="/data/Shine/MarketingReports/Output/RecruiterLeads_ShineMyParichay/"+todayDate.strftime("%Y-%m-%d")+"_"+dayOfWeek+"_RecruiterLeads_ShineMyParichay.csv"
        RecruiterLeads_ShineMyParichaySql="/data/Shine/MarketingReports/Model/SQL/RecruiterLeads_ShineMyParichay_"+dayOfWeek+".sql"
        generateReport('analytics', '@n@lytics', RecruiterLeads_ShineMyParichaySql, RecruiterLeads_ShineMyParichay, "SumoPlus",'','172.16.66.64')
        uploadData(RecruiterLeads_ShineMyParichay, uploadDir)

def main():
	starttime = datetime.now()
	print '\n'+str(starttime)
	#resume_offer_report()
	#Convergys_Lead_Report()
	#welcome_call_report()
	#last_login_report()
	Registration_Tracker()
	internal_sem_report()
	Vendor_Pivot()
	ITExp1plus()
	RecruiterLeads_ShineMyParichay()
	#Fresher_DesiredFunction_Report()
	#Resume_midout_Report()

	finishTime = datetime.now()
	print "TimeTaken: "+str(finishTime-starttime)
if __name__ == '__main__':
	main()
