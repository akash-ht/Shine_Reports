from datetime import date, datetime, time
from dateutil.relativedelta import *
import MySQLdb
import csv
import sys #stdin.readlines
import os
import sendMailUtil

todayDate=date.today()
previousDate=todayDate+relativedelta(days=-1)

def getMySqlConnection(HOST, PORT, DB_USER, DB_PASSWORD, DB):
    return MySQLdb.connect(host = HOST,port = PORT,user =DB_USER,passwd =DB_PASSWORD,db = DB)

def sqldata(sql, outFile):
        #command="mysql -uanalytics -pAnal^tics@11 -h172.22.66.204 -P3306 SumoPlus < "+str(sql)+"| sed 's/\t/,/g; s/\"//g;' > "+str(outFile)
	command="mysql -uanalytics -pAnal^tics@11 -h172.22.65.157 -P3308 SumoPlus < "+str(sql)+"| sed 's/\t/,/g; s/\"//g;' > "+str(outFile)
        print command
        ret = os.system(command)
        return ret

def mail_files(outPutFile):
        mailing_list=['kiran.sharma@hindustantimes.com']
        cc_list=['ashish.jain1@hindustantimes.com','varun.gagneja@hindustantimes.com']
        bcc_list = []#'parul.agarwal@hindustantimes.com']
        subject = "Revenue Per Account Report"
        content = "PFA.\n\nRegards,\nAshish\n\n\n\n*system generated email*"
        attachment = [outPutFile]
        print 'Mailing file......'
        sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)

def main():
        print datetime.now()
	sql = '/data/Shine/SaleReports/Model/SQL/revenue_per_account.sql'
	outFile = '/data/Shine/SaleReports/Output/revenue_per_account_'+str(todayDate)+'.csv'
	sqldata(sql,outFile)
	mail_files(outFile)
        print datetime.now()

if __name__=='__main__':
    main()
	
	
