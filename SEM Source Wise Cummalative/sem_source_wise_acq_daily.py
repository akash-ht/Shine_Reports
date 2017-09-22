import sys, datetime, email, csv
import os, re, sendMailUtil, string, traceback, subprocess
from datetime import date, datetime, time
from dateutil.relativedelta import *
import pandas as pd, numpy as np

todayDate=date.today()
previousDate=todayDate+relativedelta(days=-1)

def fileProcessing():
    source_file = open('/data/Shine/MarketingReports/Input/sources_list_sem.csv','rb+')
    source_file2 = open('/data/Shine/MarketingReports/Input/sem_sources_list_v3.csv','wb+')
    for line in source_file:
        if (str(line).startswith('#')):
            continue
        Network = line.split(',')[0]
        Account = line.split(',')[1]
        Campaign = line.split(',')[2]
        Adgroup = line.split(',')[3]
        #name = line.split(',')[1]
        vendorids = line.split(',')[4].strip()
        vendorids = vendorids.split(';')
        for vendor in vendorids:
            if (str(vendor).startswith('-')):
                svi = evi = vendor
            else:
                svi = vendor.split('-')[0].strip()
                try:
                    evi = vendor.split('-')[1].strip()
                except:
                    evi = svi
            source_file2.write(str(Network)+','+str(Account)+','+ str(Campaign)+ ','+ str(Adgroup) + ',' + str(svi)+','+str(evi)+'\n')
    source_file2.close()
    source_file.close()

def run_sas_query(sas_program, sas_log_file):
    if os.path.isfile(sas_log_file):
            os.system('rm '+sas_log_file)
    sas_temp_file="/data/Analytics/temp"
    sas_query="/data/SAS/sasinstall_STAT/SASFoundation/9.2/sas -work "+str(sas_temp_file)+" -log "+str(sas_log_file)+" -SYSIN "+str(sas_program)+" -nonews -noterminal"
    print str(sas_query)
    os.system(str(sas_query))

def groupingData():
    sas_op_file = '/data/Shine/MarketingReports/Output/sem_source_acq_daily.csv'
    ifile = pd.read_csv(sas_op_file)
    ifile2 = pd.read_csv(sas_op_file)
    cols = list(ifile.columns.values)
    cols.remove('EndVendorId')
    os.system('sed -i s/None/0/g /data/Shine/MarketingReports/Input/sem_sources_list_v3.csv')
    sources = pd.read_csv('/data/Shine/MarketingReports/Input/sem_sources_list_v3.csv')
    sources.drop_duplicates(inplace=True)
    sources.columns.values[3]='svi'
    sources.columns.values[4]='evi'
    df_final = pd.DataFrame()
    for index,row in sources.iterrows():
        Network = row[0]
        Account = row[1]
        Campaign = row[2]
        Adgroup = row[3]
        svi = row[4]
        evi = row[5]
        df2 = ifile.loc[(ifile['EndVendorId'] >= int(svi)) & (ifile['EndVendorId'] <= int(evi))]
        ifile2 = ifile2[~ifile2['EndVendorId'].isin(df2['EndVendorId'])]
        df2.drop('EndVendorId', axis=1, inplace=True)
        for col in cols:
            df2[col] = df2[col].sum()
        df2.drop_duplicates(inplace=True)
        df2 = df2.reset_index()
        df1 = pd.DataFrame({'Network':[Network], 'Account':[Account], 'Campaign':[Campaign],'Adgroup':[Adgroup],'svi':[svi], 'evi':[evi]})
        df = pd.concat([df1,df2], axis=1)
        df_final = df_final.append(df)
    ifile2['source'] = 'Others'
    ifile2['name'] = 'Others'
    ifile2.drop('EndVendorId', axis=1, inplace=True)
    df_final = df_final.append(ifile2)
    #print df_final.head()
    return df_final
        
def pivotingData(df):
    df.drop(['evi','svi','index'],axis=1,inplace=True)
    df_pivot = pd.pivot_table(df,rows=['Network','Account','Campaign','Adgroup'],aggfunc=np.sum).reset_index()
    #print df_pivot.head()
    df_pivot.to_csv('/data/Shine/MarketingReports/Output/sem_sources_acq_report_daily.csv',index=False)

def getDailyRegistrations():
    ifile_new = pd.read_csv('/data/Shine/MarketingReports/Output/sem_sources_acq_report_daily.csv')
    ifile_new = ifile_new[['Network','Account','Campaign','Adgroup','Acquisitions']]
    ifile_new.rename(columns={'Acquisitions':previousDate.strftime("%d-%b")},inplace=True)
    ifile_new.to_csv('/data/Shine/MarketingReports/Output/sem_sources_acq_report_daily.csv',index=False)
    if (todayDate.strftime("%d")=='02'):
        #print 'Hello'
        mail_files('/data/Shine/MarketingReports/Output/sem_sources_acq_report_daily.csv')
    else:
        prev_file = pd.read_csv('/data/Shine/MarketingReports/Output/sem_sources_acq_previousdate.csv')
        daily_file = pd.read_csv('/data/Shine/MarketingReports/Output/sem_sources_acq_report_daily.csv')
        merged_file = pd.merge(prev_file,daily_file,on=['Network','Account','Campaign','Adgroup'],how='outer')
        merged_file.to_csv('/data/Shine/MarketingReports/Output/sem_sources_acq_report_daily.csv',index=False)
        #print daily_file.head()
        mail_files('/data/Shine/MarketingReports/Output/sem_sources_acq_report_daily.csv')
    
def mail_files(*args):
    mailing_list=['prateek.agarwal1@hindustantimes.com']
    cc_list = ['jitender.kumar@hindustantimes.com','dhruv.khatkar@hindustantimes.com']
    bcc_list = ['himanshu.solanki@hindustantimes.com']
    #bcc_list = ['himanshu.solanki@hindustantimes.com']
    subject = "SEM Source Wise Acquisition(Daily Report)"
    content = "PFA.\n\nRegards,\nHimanshu\n\n\n\n*system generated email*"
    attachment = []
    for arg in args:
        attachment.append(arg)
    print 'Mailing file......'
    sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)
    
def main():
    print datetime.now()
    sas_program = '/data/Shine/MarketingReports/Model/SASCode/sem_source_acq_daily.sas'
    sas_log_file = '/data/Shine/MarketingReports/Model/SASCode/sem_source_acq_daily.log'
    file1 = '/data/Shine/MarketingReports/Output/sem_sources_wise_acq_daily.csv'
    file2 = '/data/Shine/MarketingReports/Output/sem_source_acq_daily.csv'
    fileProcessing()
    if (todayDate.strftime("%d")=='02'):
        os.system('rm -f /data/Shine/MarketingReports/Output/sem_sources_acq_report_daily.csv /data/Shine/MarketingReports/Output/sem_sources_acq_previousdate.csv')
    else:
        os.system('mv /data/Shine/MarketingReports/Output/sem_sources_acq_report_daily.csv /data/Shine/MarketingReports/Output/sem_sources_acq_previousdate.csv')
    run_sas_query(sas_program, sas_log_file)
    df_final = groupingData()
    pivotingData(df_final)
    getDailyRegistrations()    
    print datetime.now()    
        
if __name__=='__main__':
    main()   
/opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
  import GoogleSOAPFacade
/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py:65: SettingWithCopyWarning: A value is trying to be set on a copy of a slice from a DataFrame
  df2.drop('EndVendorId', axis=1, inplace=True)
/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py:67: SettingWithCopyWarning: A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_index,col_indexer] = value instead
  df2[col] = df2[col].sum()
/opt/python2.6/lib/python2.6/site-packages/pandas/util/decorators.py:60: SettingWithCopyWarning: A value is trying to be set on a copy of a slice from a DataFrame
  return func(*args, **kwargs)
/opt/python2.6/lib/python2.6/site-packages/pandas/util/decorators.py:53: FutureWarning: rows is deprecated, use index instead
  warnings.warn(msg, FutureWarning)
2017-03-11 11:50:02.045046
/data/SAS/sasinstall_STAT/SASFoundation/9.2/sas -work /data/Analytics/temp -log /data/Shine/MarketingReports/Model/SASCode/sem_source_acq_daily.log -SYSIN /data/Shine/MarketingReports/Model/SASCode/sem_source_acq_daily.sas -nonews -noterminal
Mailing file......
2017-03-11 11:50:17.596182
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
  File "/data/Shine/MarketingReports/Model/Pycode/sem_source_wise_acq_daily.py", line 134
    /opt/python2.6/lib/python2.6/site-packages/pygoogle/google.py:58: DeprecationWarning: SOAPpy not imported. Trying legacy SOAP.py.
    ^
SyntaxError: invalid syntax
