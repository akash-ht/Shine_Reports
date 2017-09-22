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
	sqlQuery="/data/Harsh_singal/Shine/CareerPlus/Model/SQL/CareerPlus_DiscountDistribution_Month_V1.sql"	
	outPut="/data/Harsh_singal/Shine/CareerPlus/Output/Discount_Distribution_"+yd.strftime("%B-%Y")+".csv"
	generateReport(sqlQuery,outPut,"shinecp")
	mailing_list = ['vicky.sarin@hindustantimes.com']
	#mailing_list=[]
	cc_list = ['kumar.srivastava@hindustantimes.com','ankul.batra@hindustantimes.com','Ashwin.Mahantha@hindustantimes.com','Prakhar.Raj@hindustantimes.com','rahul.garg@hindustantimes.com']
	#cc_list=[]
	bcc_list = []
	outPut="/data/Harsh_singal/Shine/CareerPlus/Output/Discount_Distribution_"+yd.strftime("%B-%Y")+".csv"
	subject = "Monthly Order Discount Report "+yd.strftime("%B-%Y")+""
	content = "PFA.\n\nRegards\nSaurabh\n\n\n\n*system generated email*"
	attachment = [outPut]
	print 'Mailing generated files......'
	sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,2)


if __name__=='__main__':
     main()
