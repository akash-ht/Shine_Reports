import poplib, sys, sample_utils, datetime, email, os
from pymongo import Connection
from pymongo.objectid import ObjectId
from datetime import date, datetime, time
from dateutil.relativedelta import *
import sendMailUtil

todayDate=date.today()
todayDate = todayDate.strftime("%d-%b-%Y")
todayDate = datetime.strptime(todayDate,"%d-%b-%Y")

previousDate=todayDate+relativedelta(days=-1)
previousDate = previousDate.strftime("%d-%b-%Y")
previousDate = datetime.strptime(previousDate,"%d-%b-%Y")

def mail_files(emailid,addEmailid,outputFile):
        mailing_list=[emailid]
        cc_list=[addEmailid]
        #bcc_list = ['ankur.tiwari@hindustantimes.com']
	bcc_list = ['jitender.kumar@hindustantimes.com','prateek.agarwal1@hindustantimes.com','akash.verma@hindustantimes.com']
        subject = "Daily Report- Shine.com"
        content = "\n\n\n\n*system generated email*\n\nRegards,\nParul Agarwal"
        attachment = [outputFile]
        print 'Mailing file......'
        sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)

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

def getDataFromCandidateMongo(vendorid,emailid,addEmailid,outputFile,vendorFile):
    #print todayDate
    if (todayDate.strftime("%d")=='02'):
	#print "2nd day"
	ofile = open(vendorFile,'wb+')
	ofile.write('date,Registrations\n')
    else:
    	ofile = open(vendorFile,'a')
    vendors = []
    if ';' in vendorid:
            vendors = vendorid.split(';')
            for i in range(len(vendors)):
                vendors.append(int(vendors[i]))
    else:
            vendors.append(vendorid)
            vendors.append(int(vendorid))
    day1 = datetime.combine(previousDate, time(0, 0))
    day2 = datetime.combine(todayDate, time(0, 0))
    Reg = collection.find({'evi':{'$in':vendors},'red':{'$gte':day1,'$lt':day2},'rm':0,'mo':0}).count()
    ofile.write(str(previousDate.strftime("%d-%b-%Y"))+","+str(Reg)+"\n")
    ofile.close()
    os.system('cp '+vendorFile+' '+outputFile)
    #ofile.close()
    mail_files(emailid,addEmailid,outputFile)
    
def main():
    print datetime.now()
    ifile = open('/data/Shine/MarketingReports/Input/Alliances_VID_list.csv','r')
    for line in ifile:
	if (line[0]=='#'):
		continue
	addEmailid = ''
	vendorid = line.split(',')[0]
	emailid = line.split(',')[1]
	addEmailid = line.split(',')[2]
	addEmailid = addEmailid.split('\n')[0]
	if ';' in vendorid:
		vendor = vendorid.split(';')[0]
	else:
		vendor = vendorid
	#vendorFile = '/data/Shine/MarketingReports/Output/Alliances/ShineReport_'+vendorid+'.csv'
	vendorFile = '/data/Shine/MarketingReports/Output/Alliances/ShineReport_'+vendor+'.csv'
	outputFile = '/data/Shine/MarketingReports/Output/Alliances/ShineReport_'+todayDate.strftime("%d-%b-%Y")+'.csv'
	getDataFromCandidateMongo(vendorid,emailid,addEmailid,outputFile,vendorFile)
	#return
    ifile.close()
    #os.system('rm /data/Shine/MarketingReports/Output/Alliances/*')
    print datetime.now()

if __name__=='__main__':
    main()
