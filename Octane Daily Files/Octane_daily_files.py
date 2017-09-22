import base64
import csv
import glob
import os
from datetime import date, datetime, time
import paramiko,sendMailUtil
from datetime import date, datetime, time
from dateutil.relativedelta import *
import sys
import pandas as pd
import ftplib
from Tkconstants import FIRST

todayDate=date.today()
previousDate=todayDate+relativedelta(days=-1)

def run_sas_stmnt(sas_program, sas_log_file):
        sas_temp_file="/data/Analytics/temp"
        sas_query="/data/SAS/sasinstall_STAT/SASFoundation/9.2/sas -work "+str(sas_temp_file)+" -log "+str(sas_log_file)+" -SYSIN "+str(sas_program)+" -nonews -noterminal"
        print str(sas_query)
        os.system(str(sas_query))

def info_mail(previousDate):
    mailing_list=['nandan@octane.co.in']
    cc_list = ['prateek.agarwal1@hindustantimes.com','himanshu.solanki@hindustantimes.com','akash.verma@hindustantimes.com','ashima.aggarwal@hindustantimes.com','niharika.wadhwa@octane.in']
    bcc_list = []
    subject = "Octane Daily File"
    content = "Hi Nandan,\n\nDaily Updation file is transferred with filename Shine_Data_"+previousDate.strftime("%Y%m%d")+'.csv\n\nRegards,\nParul'
    attachment = []
    print 'Mailing file......'
    sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)


def info_mail_1(previousDate):
    mailing_list=['faruq.kazi@netcore.in']
    cc_list = ['prateek.agarwal1@hindustantimes.com','akash.verma@hindustantimes.com','neha.gupta@hindustantimes.com']
    bcc_list = []
    subject = "Netcore Daily File"
    content = "Hi Faruq,\n\nDaily Updation file is transferred in given directory \n\nRegards,\nAkash"
    attachment = []
    print 'Mailing file......'
    sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)


def net_core_data_incremental():
    Input_File = '/data/Analytics/Utils/MarketingReports/Output/Octane/Shine_OctaneData1.csv'
    #Input_File = '/data/Analytics/Utils/MarketingReports/Output/Octane/temp.csv'
    df_netcore = pd.read_csv(Input_File,parse_dates = ['CompletionDate||d.m.Y','RegistrationDate||d.m.Y','LastLoginDate||d.m.Y',
                                                       'LastUpdatedDate||d.m.Y','LatestAppliedJobDate||d.m.Y','CellPhoneVerificationDate||d.m.Y',
                                                       'CVUpdateDate||d.m.Y'] )
    
    df_netcore['emailid'] = df_netcore['emailid'].fillna('')
    df_netcore['first_name'] = df_netcore['first_name'].fillna('')
    df_netcore['last_name'] = df_netcore['last_name'].fillna('')
    df_netcore['first_name'] = df_netcore['first_name'].fillna('')
    df_netcore['mobile'] = df_netcore['mobile'].fillna('')
    df_netcore['Gender'] = df_netcore['Gender'].fillna('')
    df_netcore['first_name'] = df_netcore['first_name'].fillna('')
    df_netcore['City'] = df_netcore['City'].fillna('')
    df_netcore['City'] = df_netcore['City'].fillna('')
    df_netcore['Status'] = df_netcore['Status'].fillna('')
    
    df_netcore['Seg_Totalexperience'] = df_netcore['_Seg_Totalexperience']
    df_netcore['Seg_Totalexperience'] = df_netcore['Seg_Totalexperience'].fillna('')
    df_netcore.drop('_Seg_Totalexperience',1)
    
    df_netcore['Seg_Salary_Lower'] = df_netcore['_Seg_Salary_Lower']  
    df_netcore['Seg_Salary_Lower'] = df_netcore['Seg_Salary_Lower'].fillna('')
    df_netcore.drop('_Seg_Salary_Lower',1)
    
    df_netcore['Seg_Salary_Upper'] = df_netcore['_Seg_Salary_Upper'] 
    df_netcore['Seg_Salary_Upper'] = df_netcore['Seg_Salary_Upper'].fillna('')
    df_netcore.drop('_Seg_Salary_Upper',1)
    
    df_netcore['SubFunction'] = df_netcore['SubFunction'].fillna('')
    df_netcore['Function'] = df_netcore['Function'].fillna('')
    df_netcore['Industry'] = df_netcore['Industry'].fillna('')
    df_netcore['CompanyName'] = df_netcore['CompanyName'].fillna('')
    df_netcore['EducationQualification'] = df_netcore['EducationQualification'].fillna('')
    df_netcore['Specialization'] = df_netcore['Specialization'].fillna('')
    df_netcore['Stream'] = df_netcore['Stream'].fillna('')
    df_netcore['IsReceiveSMS'] = df_netcore['IsReceiveSMS'].fillna('')
    df_netcore['AutoLoginToken'] = df_netcore['AutoLoginToken'].fillna('')
    df_netcore['UserId'] = df_netcore['UserId'].fillna('')
    df_netcore['StartVendorId'] = df_netcore['StartVendorId'].fillna('')
    df_netcore['EndVendorId'] = df_netcore['EndVendorId'].fillna('')
    df_netcore['TotalExperience'] = df_netcore['TotalExperience'].fillna('')
    df_netcore['Experience_Month'] = df_netcore['Experience_Month'].fillna('')
    df_netcore['Salary'] = df_netcore['Salary'].fillna('')
    df_netcore['JobTitle'] = df_netcore['JobTitle'].fillna('')
    df_netcore['SubFIsReceiveSMSunction'] = df_netcore['IsReceiveSMS'].fillna('')
    df_netcore['Institute'] = df_netcore['Institute'].fillna('')
    df_netcore['IsEmailVerified'] = df_netcore['IsEmailVerified'].fillna('')
    df_netcore['IsCellPhoneVerified'] = df_netcore['IsCellPhoneVerified'].fillna('')
    df_netcore['NotificationFrequency'] = df_netcore['NotificationFrequency'].fillna('')
    df_netcore['CQS'] = df_netcore['CQS'].fillna('')
    df_netcore['num_skills'] = df_netcore['num_skills'].fillna('')
    
    
    df_netcore['RegistraionDate'] = df_netcore['RegistrationDate||d.m.Y']
    df_netcore.drop('RegistrationDate||d.m.Y',1)
    
    df_netcore['CompletionDate'] = df_netcore['CompletionDate||d.m.Y']
    df_netcore.drop('CompletionDate||d.m.Y',1)
    
    df_netcore['LastLoginDate'] = df_netcore['LastLoginDate||d.m.Y']
    df_netcore.drop('LastLoginDate||d.m.Y',1)
    
    df_netcore['LastUpdateDate'] = df_netcore['LastUpdatedDate||d.m.Y']
    df_netcore.drop('LastUpdatedDate||d.m.Y',1)
    
    df_netcore['LatestAppliedJobDate'] = df_netcore['LatestAppliedJobDate||d.m.Y']
    df_netcore.drop('LatestAppliedJobDate||d.m.Y',1)
    
    df_netcore['CellPhoneVerificationDate'] = df_netcore['CellPhoneVerificationDate||d.m.Y']
    df_netcore.drop('CellPhoneVerificationDate||d.m.Y',1)
    
    df_netcore['CVUpdateDate'] = df_netcore['CVUpdateDate||d.m.Y']
    df_netcore.drop('CVUpdateDate||d.m.Y',1)
    
    df_netcore['RegistraionDate'] = [d.strftime('%d-%m-%Y') if not pd.isnull(d) else '' for d in df_netcore['RegistraionDate']]
    df_netcore['CompletionDate'] = [d.strftime('%d-%m-%Y') if not pd.isnull(d) else '' for d in df_netcore['CompletionDate']]
    df_netcore['LastLoginDate'] = [d.strftime('%d-%m-%Y') if not pd.isnull(d) else '' for d in df_netcore['LastLoginDate']]
    df_netcore['LastUpdateDate'] = [d.strftime('%d-%m-%Y') if not pd.isnull(d) else '' for d in df_netcore['LastUpdateDate']]
    df_netcore['LatestAppliedJobDate'] = [d.strftime('%d-%m-%Y') if not pd.isnull(d) else '' for d in df_netcore['LatestAppliedJobDate']]
    df_netcore['CellPhoneVerificationDate'] = [d.strftime('%d-%m-%Y') if not pd.isnull(d) else '' for d in df_netcore['CellPhoneVerificationDate']]
    df_netcore['CVUpdateDate'] = [d.strftime('%d-%m-%Y') if not pd.isnull(d) else '' for d in df_netcore['CVUpdateDate']]
    
    
    df_netcore_final = df_netcore[['UserId','emailid','mobile','first_name','last_name',
                                   'Gender','City','Status','Seg_Totalexperience','Seg_Salary_Lower','Seg_Salary_Upper',
                                   'SubFunction','Function','Industry','CompanyName','EducationQualification','Specialization',
                                   'Stream','IsReceiveSMS','AutoLoginToken','StartVendorId','EndVendorId','TotalExperience',
                                   'Experience_Month','Salary','JobTitle','SubFIsReceiveSMSunction','Institute',
                                   'IsEmailVerified','IsCellPhoneVerified','NotificationFrequency','CQS','num_skills','RegistraionDate',
                                   'CompletionDate','LastLoginDate','LastUpdateDate','LatestAppliedJobDate','CellPhoneVerificationDate','CVUpdateDate'
                                   ]]
    
    df_netcore_final.columns = ['USER_ID','EMAIL','MOBILE','first_name','last_name',
                                   'Gender','City','Status','Seg_Totalexperience','Seg_Salary_Lower','Seg_Salary_Upper',
                                   'SubFunction','Function','Industry','CompanyName','EducationQualification','Specialization',
                                   'Stream','IsReceiveSMS','AutoLoginToken','StartVendorId','EndVendorId','TotalExperience',
                                   'Experience_Month','Salary','JobTitle','SubFIsReceiveSMSunction','Institute',
                                   'IsEmailVerified','IsCellPhoneVerified','NotificationFrequency','CQS','num_skills','RegistraionDate',
                                   'CompletionDate','LastLoginDate','LastUpdateDate','LatestAppliedJobDate','CellPhoneVerificationDate','CVUpdateDate']
    
    #print df_netcore_final.count()
    #sys.exit(0)
    row_count = df_netcore_final['EMAIL'].count()
    final_row_count = row_count + 1
    #df_netcore_final.to_csv('/data/Analytics/Utils/MarketingReports/Output/Netcore/temp_netcore_file.csv',index = False)
    df_netcore_final.to_csv('/data/Analytics/Utils/MarketingReports/Output/Netcore/net_core_data' + str(previousDate) + ".csv",index = False)
    
    
    ##################### File Transfer To Netcore ####################
    ###################################################################
    
    daily_file = '/ftpshine/WORK/' +'new_ROWCNT-'+str(final_row_count) + "_" +str(previousDate)+".csv"
    try:
        ftp = ftplib.FTP()
        host = '76.8.51.34'
        port = '21'
        ftp.connect(host,port)
        ftp.login('ftpshine','Luomaib2Co')
        #filename = '/data/Analytics/Utils/MarketingReports/Output/Netcore/net_core_data' + str(previousDate) + ".csv"
        filename = '/data/Analytics/Utils/MarketingReports/Output/Netcore/temp_netcore_file.csv'
        ftp.storbinary("STOR " + daily_file ,open(filename,'rb'))
        ftp.quit()
    except Exception as e:
        print e
    info_mail_1(previousDate)    
    
def main():
    print datetime.now()
    todayDate = date.today()
    previousDate=todayDate+relativedelta(days=-1)
    	
    #run_sas_stmnt('/data/Analytics/Utils/MarketingReports/Model/SASCode/Octane_daily_data.sas','/data/Analytics/Utils/MarketingReports/log/Octane_daily_data.log')
    run_sas_stmnt('/data/Analytics/Utils/MarketingReports/Model/SASCode/Octane_daily_new.sas','/data/Analytics/Utils/MarketingReports/log/Octane_daily_new.log')
    
    command = "sed s/CellPhoneVerificationDate/CellPhoneVerificationDate\|\|d\.m\.Y/g /data/Analytics/Utils/MarketingReports/Output/Octane/Shine_Data.csv > /data/Analytics/Utils/MarketingReports/Output/Octane/Shine_OctaneData1.csv"
    os.system(command)
    
    #gettingEmailIDs(insource,outsource)
    
    daily_file = '/incoming/Shine_Data_'+previousDate.strftime("%Y%m%d")+'.csv'
    bounces_file = '/incoming/bounces_'+previousDate.strftime("%Y%m%d")+'.csv'
    #print daily_file
    #return
    
    #FTP transfer
    try:
    	#transport = paramiko.Transport(('208.43.248.197', 12222))
        transport = paramiko.Transport(('ftp_aa.trackcampaigns.com', 12222))
    	transport.connect(username = 'htmedianew',password = 'H@t@ME@Dia2013')
    
    	sftp = paramiko.SFTPClient.from_transport(transport)
    
    	#sftp.put('/data/Analytics/Utils/MarketingReports/Output/Octane/Shine_OctaneData1.csv',daily_file)
    	sftp.put('/data/Analytics/Utils/MarketingReports/Output/Octane/Shine_OctaneData1.csv',daily_file)
    	sftp.put('/data/Analytics/Utils/MarketingReports/Output/Octane/bounces.csv',bounces_file)
    
    	sftp.close()
    	transport.close()
    
    	info_mail(previousDate)	
    except:
    	print "Exception Raised..."
    	comm = 'cp /data/Analytics/Utils/MarketingReports/Output/Octane/Shine_OctaneData1.csv /data/Analytics/Utils/MarketingReports/Output/Octane/Shine_Data_'+previousDate.strftime("%Y%m%d")+'.csv'
    	os.system(comm)
        comm = 'cp /data/Analytics/Utils/MarketingReports/Output/Octane/Shine_OctaneData1.csv /data/Analytics/Utils/MarketingReports/Output/Octane/Octane_data_'+previousDate.strftime("%Y%m%d")+'.csv'
        os.system(comm)
        
    print datetime.now()
    net_core_data_incremental()
    print datetime.now()

if __name__=='__main__':
    main()
