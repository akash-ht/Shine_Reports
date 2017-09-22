import os, re, string, ftplib, traceback,sendMailUtil
from datetime import date, datetime
from dateutil.relativedelta import *

td = date.today()
yd = td+relativedelta(days=-2)

def generateReport(sqlFile, outFile, DB, options = ""):
        command="mysql -uroot -h172.22.65.170 -P3311 -S /var/lib/cp_mysql/mysql5.sock "+options+" "+DB+" < "+str(sqlFile)+" |sed 's/\t/,/g'  > "+str(outFile)
        print command
        os.system(command)

def main():
	sqlQuery="/data/Harsh_singal/Shine/CareerPlus/Model/SQL/CareerPlus_DiscountDistribution_Day_V2.sql"	
	outPut="/data/Harsh_singal/Shine/CareerPlus/Output/Discount_Distribution_Daily_"+td.strftime("%d-%m-%Y")+".csv"
	strt_time = datetime.now()
	generateReport(sqlQuery,outPut,"shinecp")
	finish_time = datetime.now()
	print 'Total Time Taken :',str(finish_time - strt_time)
	#mailing_list = []
	mailing_list = ['saunak.ghosh@hindustantimes.com']
	#cc_list = []
	cc_list = ['Bhawna.Mehta@hindustantimes.com','akash.verma@hindustantimes.com','rahul.garg@hindustantimes.com','vinod@shine.com','purnima.ganguly@hindustantimes.com','karmveer.singh@hindustantimes.com','karminder.kaur@hindustantimes.com','sidharth.gupta1@hindustantimes.com','vinod.kumar2@hindustantimes.com','shubhankar.srivastav@hindustantimes.com']

	bcc_list = ['akash.verma@hindustantimes.com']
	outPut="/data/Harsh_singal/Shine/CareerPlus/Output/Discount_Distribution_Daily_"+td.strftime("%d-%m-%Y")+".csv"
	subject = "Daily Order Discount Report "+td.strftime("%d%b%Y")+""
	content = "PFA.\n\nRegards\nAkash\n\n\n\n*system generated email*"
	attachment = [outPut]
	print 'Mailing generated files......'
	print 'File Generated Successfully'
	sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,2)


if __name__=='__main__':
     main()
