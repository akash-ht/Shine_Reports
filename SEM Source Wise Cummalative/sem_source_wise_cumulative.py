import sys, datetime, email, csv
import os, re, sendMailUtil, string, traceback, subprocess
from datetime import date, datetime, time
import pandas as pd, numpy as np

todayDate=date.today()

def fileProcessing():
    source_file = open('/data/Shine/MarketingReports/Input/sources_list_sem.csv','rb+')
    source_file2 = open('/data/Shine/MarketingReports/Input/sources_list_v3.csv','wb+')
    for line in source_file:
        if (str(line).startswith('#')):
            continue
        source = line.split(',')[2]
        name = line.split(',')[3]
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
            source_file2.write(str(source)+','+str(name)+','+str(svi)+','+str(evi)+'\n')
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
    sas_op_file = '/data/Shine/MarketingReports/Output/source_acq_full.csv'
    ifile = pd.read_csv(sas_op_file)
    ifile2 = pd.read_csv(sas_op_file)
    cols = list(ifile.columns.values)
    cols.remove('EndVendorId')
    os.system('sed -i s/None/0/g /data/Shine/MarketingReports/Input/sources_list_v3.csv')
    sources = pd.read_csv('/data/Shine/MarketingReports/Input/sources_list_v3.csv')
    sources.drop_duplicates(inplace=True)
    sources.columns.values[2]='svi'
    sources.columns.values[3]='evi'
    df_final = pd.DataFrame()
    for index,row in sources.iterrows():
        source = row[0]
        name = row[1]
        svi = row[2]
        evi = row[3]
        df2 = ifile.loc[(ifile['EndVendorId'] >= int(svi)) & (ifile['EndVendorId'] <= int(evi))]
        ifile2 = ifile2[~ifile2['EndVendorId'].isin(df2['EndVendorId'])]
        df2.drop('EndVendorId', axis=1, inplace=True)
        for col in cols:
            df2[col] = df2[col].sum()
        df2.drop_duplicates(inplace=True)
        df2 = df2.reset_index()
        df1 = pd.DataFrame({'source':[source], 'name':[name], 'svi':[svi], 'evi':[evi]})
        df = pd.concat([df1,df2], axis=1)
        df_final = df_final.append(df)
    ifile2['source'] = 'Others'
    ifile2['name'] = 'Others'
    ifile2.drop('EndVendorId', axis=1, inplace=True)
    df_final = df_final.append(ifile2)
    os.system('rm -f /data/Shine/MarketingReports/Output/source_acq_full.csv')
    return df_final
        
def pivotingData(df):
    df.drop(['evi','svi','index'],axis=1,inplace=True)
    df_pivot = pd.pivot_table(df,rows=['source','name'],aggfunc=np.sum).reset_index()
    df_pivot.to_csv('/data/Shine/MarketingReports/Output/sources_acq_report.csv',index=False)

def formatting_files():
    ifile_new = pd.read_csv('/data/Shine/MarketingReports/Output/sources_acq_report.csv')
    ifile_new['IT%'] = (ifile_new['IT']/ifile_new['Acquisitions']*100).round(2)
    ifile_new['cellphone_verified%'] = (ifile_new['cellphone_verified']/ifile_new['Acquisitions']*100).round(2)
    ifile_new['email_verified%'] = (ifile_new['email_verified']/ifile_new['Acquisitions']*100).round(2)
    ifile_new['Midout_rate(%)'] = (ifile_new['Midouts']/(ifile_new['Midouts']+ifile_new['Acquisitions'])*100).round(2)
    ifile_new = ifile_new[['source','name','Acquisitions','Midouts','Midout_rate(%)','Exp1','Exp2','Exp3','IT','IT%','cellphone_verified','cellphone_verified%','email_verified','email_verified%','Applied']]
    ifile_new.to_csv('/data/Shine/MarketingReports/Output/sources_wise_acq.csv',index=False)
    os.system('sed -i s/Exp1/0-1Yr/g /data/Shine/MarketingReports/Output/sources_wise_acq.csv')
    os.system('sed -i s/Exp2/1-3Yrs/g /data/Shine/MarketingReports/Output/sources_wise_acq.csv')
    os.system('sed -i s/Exp3/3Yrs+/g /data/Shine/MarketingReports/Output/sources_wise_acq.csv')    
   
def mail_files(*args):
    mailing_list=[]
    #mailing_list = []
    cc_list = ['jitender.kumar@hindustantimes.com','prateek.agarwal1@hindustantimes.com']
    #cc_list = []
    bcc_list = ['himanshu.solanki@hindustantimes.com']
    #bcc_list = ['himanshu.solanki@hindustantimes.com']
    subject = "Sem Source Wise Acquisition(Cumulative Report)"
    content = "PFA.\n\nRegards,\nHimanshu\n\n\n\n*system generated email*"
    attachment = []
    for arg in args:
        attachment.append(arg)
    print 'Mailing file......'
    sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)
    
def main():
    print datetime.now()
    #sas_program = '/data/Shine/MarketingReports/Model/SASCode/source_acq_full.sas'
    sas_program = '/data/Shine/MarketingReports/Model/SASCode/sem_source_acq_full.sas'
    #sas_program = '/data/Shine/MarketingReports/Model/SASCode/source_acq_full_new.sas'
    sas_log_file = '/data/Shine/MarketingReports/Model/SASCode/source_acq_full_new.log'
    file1 = '/data/Shine/MarketingReports/Output/sources_wise_acq.csv'
    fileProcessing()
    run_sas_query(sas_program, sas_log_file)
    df_final = groupingData()
    pivotingData(df_final)
    formatting_files()
    os.system('cp /data/Shine/MarketingReports/Output/sources_wise_acq.csv /data/Shine/MarketingReports/Output/sem_source_wise_acq.csv ')
    file2 = '/data/Shine/MarketingReports/Output/sem_source_wise_acq.csv'
    mail_files(file2)
    os.system('rm -f '+file1)
    os.system('rm -f '+file2)
    print datetime.now()    
        
if __name__=='__main__':
    main()   
