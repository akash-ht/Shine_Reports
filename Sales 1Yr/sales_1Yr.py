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
        mailing_list=['rohit.chauhan@hindustantimes.com']
        cc_list=['avnish.gotra@hindustantimes.com','giresh.sharma@hindustantimes.com','ashish.jain1@hindustantimes.com','varun.gagneja@hindustantimes.com']
        bcc_list = []#'parul.agarwal@hindustantimes.com']
        subject = "Sales Dump 1 Yr"
        content = "PFA.\n\nRegards,\nParul\n\n\n\n*system generated email*"
        attachment = [outPutFile]
        print 'Mailing file......'
        sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)

def main():
        print datetime.now()
        sql = '/data/Analytics/Utils/FinanceReports/Model/SQL/sales_1Yr.sql'
        outFile = '/data/Analytics/Utils/FinanceReports/Output/sales_dump.csv'
	outFile2 = '/data/Analytics/Utils/FinanceReports/Output/sales_dump.csv.zip'
        sqldata(sql,outFile)
	os.system('zip '+outFile2+' '+outFile)
        mail_files(outFile2)
        print datetime.now()

if __name__=='__main__':
    main()


