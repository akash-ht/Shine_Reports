import pandas as pd
import csv
from datetime import date, datetime, time
from dateutil.relativedelta import *
import os

todayDate=date.today()
file_date = todayDate+relativedelta(days=-1)
file_date = file_date.strftime("%d-%b-%Y")

sas_log_file = '/data/Shine/Shine_AdHoc/Model/Pycode/Boxx_Daily_Data/Scripts/Shine_Daily_Data.log' 
temp_log_file = '/data/Shine/Shine_AdHoc/Model/Pycode/Boxx_Daily_Data/Scripts/temp.log'

def run_sas_stmnt(sas_program, sas_log_file):
        sas_temp_file="/data/Analytics/temp"
        sas_query="/data/SAS/sasinstall_STAT/SASFoundation/9.2/sas -work "+str(sas_temp_file)+" -log "+str(sas_log_file)+" -SYSIN "+str(sas_program)+" -nonews -noterminal"
        print str(sas_query)
        os.system(str(sas_query))


def shine_daily_data_boxx():
    df_1 = pd.read_csv('/backup/shine_data.csv')
    df_1.to_csv('/mnt/sftp/writable/shine_profile_data_'  + str(file_date) + ".csv" ,index = False)

def main():
    print 'Running SAS Statement'
    run_sas_stmnt('/data/Shine/Shine_AdHoc/Model/Pycode/Boxx_Daily_Data/Scripts/Shine_Daily_Data.sas',
                  '/data/Shine/Shine_AdHoc/Model/Pycode/Boxx_Daily_Data/Scripts/Shine_Daily_Data.log')
    print 'SAS Statement Executed'
    
    print 'Sending Shine Profile Data to Boxx'
    shine_daily_data_boxx()
    print 'Data Sent'
    
    
if __name__ == '__main__':
    main()
    
    
    