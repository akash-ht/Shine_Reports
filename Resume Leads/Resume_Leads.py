import os, re, string, ftplib, traceback,sendMailUtil
from datetime import date, datetime
from dateutil.relativedelta import *

todayDate=date.today()
td = date.today()
yd = td+relativedelta(days=-1)

def main():
#Function call to generate resume leads for status=1 i.e, UnPaid
	sqlQuery1="/data/Harsh_singal/Shine/CareerPlus/Model/SQL/Resume_Leads.sql"
	outPut1="/data/Harsh_singal/Shine/CareerPlus/Output/Resume_Leads_"+yd.strftime("%d-%m-%Y")+".csv"
	GetLeads(sqlQuery1,outPut1,"shinecp")
#Function call to mail the files if need be
	mailFiles()
#Function call to upload files
	sourceFile="/data/Harsh_singal/Shine/CareerPlus/Output/Resume_Leads_"+yd.strftime("%d-%m-%Y")+".csv"
	destination="/data1/apacheRoot/html/shine/ReportArchieve/Resume_Leads/"+(todayDate+relativedelta(days=-1)).strftime("%b-%Y")
	moveData(sourceFile,destination)

def GetLeads(sqlFile, outFile, DB, options = ""):
	command="mysql -uroot -h172.22.65.170 -P3311 -S /var/lib/cp_mysql/mysql5.sock "+options+" "+DB+" < "+str(sqlFile)+" |sed 's/\t/,/g'  > "+str(outFile)
#       print command
	os.system(command)

def mailFiles():
	outPut="/data/Harsh_singal/Shine/CareerPlus/Output/Resume_Leads_"+yd.strftime("%d-%m-%Y")+".csv"
	#outPut1="/data/Harsh_singal/Shine/CareerPlus/Output/CartLeads_Unpaid_"+yd.strftime("%d-%m-%Y")+".csv"
	#outPut2="/data/Harsh_singal/Shine/CareerPlus/Output/CartLeads_Lead_"+yd.strftime("%d-%m-%Y")+".csv"
	#mailing_list = ['prabhat@shine.com,rithik.agarwal@hindustantimes.com','Gitesh.Sharma@hindustantimes.com','Shubhankar.Rai@hindustantimes.com']#,'harshsingal27@gmail.com','Prakhar.Raj@hindustantimes.com','Ashwin.Mahantha@hindustantimes.com','rithik.agarwal@hindustantimes.com','vinod@shine.com']
	mailing_list = ['vinod@shine.com']
	cc_list = ['']
        bcc_list = ['akash.verma@hindustantimes.com']
        subject = "Resume_Leads_"+yd.strftime("%d-%m-%Y")+""
        content = "Hi,\n\nPFA the files.\n\n Regards,\n Harsh\n\n\n*system generated email*"
	attachment = [outPut]
        #attachment = [outPut1,outPut2]
	print attachment
	sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,2)

def moveData(sourceFile,destination):
        if os.path.isfile(sourceFile):
                print "ok , file found"
                checkdir(destination)
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

def checkdir(PATH):
        if os.path.isdir(PATH):
                PATH
        else:
                os.mkdir(PATH)

if __name__=='__main__':
     main()
