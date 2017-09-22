import MySQLdb
import csv
import csv
import os
import mysql.connector
from datetime import date, datetime, time
import sendMailUtil

todayDate = datetime.today()
file_date = todayDate
file_date = todayDate.strftime("%Y-%m-%d")
Month = todayDate.strftime("%b-%Y")
Year = todayDate.strftime("%Y")

directory = '/data/Shine/Shine_AdHoc/Output/Email_Tuning_Report' + '/' + str(Year) + '/' + str(Month) + '/'

#print directory

if os.path.isdir(directory):
        print "Directory_Exists"
else:
        os.makedirs(directory)
        print "Directory_Created"


def mail_file(file):
    mailing_list = ['tanvi.arora@hindustantimes.com','ishank.mahna@hindustantimes.com','gaurav.jain@hindustantimes.com']
    cc_list =['akash.verma@hindustantimes.com']
    bcc_list = []
    Subject = 'Daily Emailer Tuner Report'
    Content = 'PFA the Daily Emailer Tuner Report \n\n Regards \n Akash'
    attachment = [file]
    sendMailUtil.send_mail(mailing_list ,cc_list, bcc_list, Subject ,Content,attachment ,0)

        
def getMySqlConnection(HOST, PORT, DB_USER, DB_PASSWORD, DB):
    return MySQLdb.connect(host = HOST,port = PORT,user =DB_USER,passwd =DB_PASSWORD,db = DB)


def getDataFromSQL():
    Output_File = directory + "Email_Tuning_Report_" +str(file_date) + ".csv"
    #Output_File = '/data/Shine/Shine_AdHoc/Output/Email_Tuner_Report_Reject.csv'
    ofile = open(Output_File ,'w')
    writer = csv.writer(ofile)
    writer.writerow(['created_date','TemplateName','status','tuner_highlight','email_body'])
    mysql_conn = getMySqlConnection('172.22.65.157',3308,'analytics','Anal^tics@11','SumoPlus')
    cursor = mysql_conn.cursor()
    query = 'Select created_date,TemplateName,status,tuner_highlight,mailbody  from recruiter_emailtemplate where DATEDIFF( CURDATE(),DATE(created_date)) =1'
    print query
    cursor.execute(query)
    count = 0
    for row in cursor:
        
        created_date = row[0]
        template_name = row[1]
        status = row[2]
        tuner_highlight = str(row[3])
        email_body = str(row[4])
        #reason_to_reject = str(row[4])
        if '\n' in tuner_highlight:
            tuner_highlight = tuner_highlight.replace('\n', '')
            #print tuner_highlight
                                  
        count +=1
        #print row
        writer.writerow([created_date,template_name,status,tuner_highlight,email_body])
    ofile.close()
    print 'Mailing Output File'
    mail_file(Output_File)
    print 'Mailing Done'
    print 'Total Records Fetched :' +str(count)
    
    
    
    
def main():
    print datetime.now()
    getDataFromSQL()
    print datetime.now()
    
if __name__=='__main__':
        main()        
