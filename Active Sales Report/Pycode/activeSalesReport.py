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
        #command="mysql -uanalytics -p@n@lytics -h172.16.66.64 -P3306 SumoPlus < "+str(sql)+"| sed 's/\t/,/g; s/\"//g;' > "+str(outFile)
	#command="mysql -uanalytics -pAnal^tics@11 -h172.22.66.204 -P3306 SumoPlus < "+str(sql)+"| sed 's/\t/,/g; s/\"//g;' > "+str(outFile)
	command="mysql -uanalytics -pAnal^tics@11 -h172.22.65.157 -P3308 SumoPlus < "+str(sql)+"| sed 's/\t/,/g; s/\"//g;' > "+str(outFile)
        print command
        ret = os.system(command)
        return ret

def mail_files(outPutFile):
        mailing_list=['abhishek.singh1@hindustantimes.com']
        cc_list=['saunak.ghosh@hindustantimes.com','amit.kapoor@hindustantimes.com','r.dhanya@hindustantimes.com','amika.jain@hindustantimes.com','pratheema.m@hindustantimes.com','aravinth.sundar@hindustantimes.com','mandala.reddy@hindustantimes.com','swati.verma@hindustantimes.com']
        bcc_list = ['ashish.jain1@hindustantimes.com','varun.gagneja@hindustantimes.com']
        subject = "Active sales Account Details "
        content = "PFA.\n\nRegards,\nAshish\n\n\n\n*system generated email*"
        attachment = [outPutFile]
        print 'Mailing file......'
        sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)

def main():
        print datetime.now()
	sql = '/data/Shine/SaleReports/Model/SQL/activeSales_v2.sql'
	outFile = '/data/Shine/SaleReports/Output/activeSales_'+str(todayDate)+'.csv'
	sqldata(sql,outFile)
	mail_files(outFile)
        print datetime.now()

if __name__=='__main__':
    main()
	
	
