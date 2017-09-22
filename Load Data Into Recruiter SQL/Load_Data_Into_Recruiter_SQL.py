import MySQLdb
from datetime import date, datetime, time
import csv
import os
import mysql.connector
from os import listdir
from os.path import isfile, join

todayDate = datetime.today()
file_date = todayDate
file_date = todayDate.strftime("%Y-%m-%d")
Month = todayDate.strftime("%b-%Y")
Year = todayDate.strftime("%Y")




directory = '/data/Shine/Shine_AdHoc/Output/Live_Jobs' + '/' + str(Year) + '/' + str(Month) + '/'
if os.path.isdir(directory):
        print "Directory Exists"
else:
        os.makedirs(directory)
        print "Directory_Created"

def getMySqlConnection(HOST, PORT, DB_USER, DB_PASSWORD, DB):
    return MySQLdb.connect(host = HOST,port = PORT,user =DB_USER,passwd =DB_PASSWORD,db = DB)

def uploadDataInRecruiter_SQL():
    Id = 181
    Output_File = directory + "Live_Jobs_Count_" + str(file_date) + ".csv"
    ofile = open(Output_File,'wb')
    writer = csv.writer(ofile)
    writer.writerow(['Date,Total_Live_Jobs,Total_Bo_Jobs,Total_Enterprise_Jobs'])
    mysql_conn = getMySqlConnection('172.22.65.157',3308,'analytics','Anal^tics@11','SumoPlus')
    cursor = mysql_conn.cursor()
    Total_Live_Jobs = 'Select count(*) as Total_Live_Jobs from recruiter_job where jobstatus in (3,9)'
    cursor.execute(Total_Live_Jobs)
    rows_Live_Jobs = cursor.fetchone()
    rows_Live_Jobs = int(rows_Live_Jobs[0])
    print 'LiveJobs:'+str(rows_Live_Jobs)
    Total_BO_Jobs = 'Select count(*) as Total_Bo_Jobs from recruiter_job where jobstatus in (3,9) and isbocreated = 1'
    cursor.execute(Total_BO_Jobs)
    rows_Bo_Jobs = cursor.fetchone()
    rows_Bo_Jobs = int(rows_Bo_Jobs[0])
    print 'BOJobs:'+str(rows_Bo_Jobs)
    Total_ENP_Jobs = 'Select count(*) as Total_Enterprise_Jobs from recruiter_job where jobstatus in (3,9) and isbocreated = 0'
    cursor.execute(Total_ENP_Jobs)
    rows_ENP_Jobs = cursor.fetchone()
    rows_ENP_Jobs = int(rows_ENP_Jobs[0])
    print 'ENPJobs:'+str(rows_ENP_Jobs)
    writer.writerow([file_date,rows_Live_Jobs,rows_Bo_Jobs,rows_ENP_Jobs])
    
    mypath = '/data/Shine/Shine_AdHoc/Output/Live_Jobs/2017/Jan-2017/'
    print mypath
    files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    for file in files:
        input_file = open(mypath + file,'rb')
        reader = csv.reader(input_file)
        reader.next()
        for row in reader:
            #print type (row)
            Date = row[0]
            Live_Jobs = int(row[1].replace(',',''))
            Bo_Jobs = int(row[2].replace(',',''))
            Enp_Jobs = int(row[3].replace(',',''))
            insert_into_table_query = '''INSERT INTO Jobs_Count (Id,Date,Total_Live_Jobs,Total_Bo_Jobs,Total_Enterprise_Jobs) Values ''' '''(%d,"%s",%d,%d,%d)''' % (Id,Date,Live_Jobs,Bo_Jobs,Enp_Jobs)
            print insert_into_table_query
            cursor.execute(insert_into_table_query)
            mysql_conn.commit()
        Id = Id + 1     
    ofile.close()



def main():
    print 'Code Started at:' +str(datetime.now())
    uploadDataInRecruiter_SQL()
    print 'Code Ended at:' +str(datetime.now())
    
if __name__=='__main__':
        main()    
