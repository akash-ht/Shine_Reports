import poplib, sys,  datetime, email, csv
from pymongo import Connection
#from pymongo import ReadPreference
from pymongo.objectid import ObjectId
from datetime import date, datetime, time
from dateutil.relativedelta import *
import re
import sendMailUtil
import os

todayDate = date.today()
previousDate=todayDate+relativedelta(days=-1)
previousDate2=todayDate+relativedelta(days=-2)
ActiveDate_1=todayDate+relativedelta(days=-182)
previousMonths=todayDate+relativedelta(months=-1)
#day1 = datetime.combine(previousDate2, time(0, 0))
#day2 = datetime.combine(previousDate, time(0, 0))

day1 = datetime.combine(previousDate, time(0, 0))
day2 = datetime.combine(todayDate, time(0, 0))

print day1,day2
#sys.exit(0)

file_date = todayDate+relativedelta(days=-1)
file_date = file_date.strftime("%d-%b-%Y")

previousDate_1 = previousDate.strftime("%d-%b-%Y")
previousDate_1 = datetime.strptime(previousDate_1,"%d-%b-%Y")


Month = previousDate_1.strftime("%b-%Y")
Year = previousDate_1.strftime("%Y")

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn


try:
    mongo_conn = getMongoConnection('172.22.65.88',27018, 'sumoplus',True, True)
except:
    mongo_conn = getMongoConnection('172.22.65.157',27018, 'sumoplus',True, True)

collection = getattr(mongo_conn, 'CandidateStatic')


directory = '/data/Shine/Shine_AdHoc/Output/Key_Metrices' + '/' + str(Year) + '/' + str(Month) + '/' 

if os.path.isdir(directory):
        print "Directory Exists"
else:
        os.makedirs(directory)
        print "Directory_Created"

def mail_file(file):
    mailing_list = ['prateek.agarwal1@hindustantimes.com']
    cc_list =['jitender.kumar@hindustantimes.com','ashima.aggarwal@hindustantimes.com',"hemang.diljun@hindustantimes.com"]
    bcc_list = ['akash.verma@hindustantimes.com']
    Subject = 'Key_Metrices'
    Content = 'PFA the Daily Report For Key Metrices \n\n Regards \n Akash'
    attachment = [file]
    sendMailUtil.send_mail(mailing_list ,cc_list, bcc_list, Subject ,Content,attachment ,0)

def getDataFromCandidateMongo():
    
    Output_File = directory + "Key_Metrices" +str(file_date) +".csv"
    ofile = open(Output_File,'wb')
    writer = csv.writer(ofile)
    writer.writerow(['ProfileCompletions','ExitedCandidates','ExitingCandidates','ActiveDB_Old','Pure_Logins'])
    
    ProfileCompletions = collection.find({'red':{'$gte':day1,'$lt':day2},'rm':0,'mo':0,'e':{'$not':re.compile('mailinator.com')}}).count()                                                                                           
    print ProfileCompletions
    
    ExitedCandidates = collection.find({'ll':{'$gte':datetime.combine(todayDate+relativedelta(days=-184), time(0, 0)), '$lt':datetime.combine(todayDate+relativedelta(days=-183), time(0, 0))}, 'rm':0, 'mo':0,'lm':{'$gte':datetime.combine(todayDate+relativedelta(days=-730), time(0, 0))}}).count()
    print ExitedCandidates
    
    ExitingCandidates = collection.find({'ll':{'$gte':datetime.combine(todayDate+relativedelta(days=-183), time(0, 0)), '$lt':datetime.combine(todayDate+relativedelta(days=-182), time(0, 0))}, 'rm':0, 'mo':0,'lm':{'$gte':datetime.combine(todayDate+relativedelta(days=-730), time(0, 0))}}).count()
    print ExitingCandidates
    
    ActiveDB_Old = collection.find({'ll':{'$gte':datetime.combine(todayDate+relativedelta(days=-183), time(0, 0))}, 'rm':0, 'mo':0, 'jp':{'$in':[None,0]},'lm':{'$gte':datetime.combine(todayDate+relativedelta(days=-730), time(0, 0))}}).count()
    print ActiveDB_Old
    
    Pure_Logins = collection.find({'ll':{'$gte':day1, '$lt':day2},'$or':[{'red':{'$lt':day1}},{'red':None,'rsd':{'$lt':day1}}], 'rm':0, 'mo':0,'e':{'$not':re.compile('mailinator.com')}}).count()
    
    writer.writerow([ProfileCompletions,ExitedCandidates,ExitingCandidates,ActiveDB_Old,Pure_Logins])
    ofile.close()
    mail_file(Output_File)

def main():
    print 'Start_Time:' +str(datetime.now())
    getDataFromCandidateMongo()
    print 'Finish_Time:' +str(datetime.now())

if __name__ == '__main__':
    main()
