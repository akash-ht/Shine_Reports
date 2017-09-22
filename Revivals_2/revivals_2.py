import base64
import csv
import glob
import os
from datetime import date, datetime, time
import paramiko

def run_sas_stmnt(sas_program, sas_log_file):
        sas_temp_file="/data/Analytics/temp"
        sas_query="/data/SAS/sasinstall_STAT/SASFoundation/9.2/sas -work "+str(sas_temp_file)+" -log "+str(sas_log_file)+" -SYSIN "+str(sas_program)+" -nonews -noterminal"
        print str(sas_query)
        os.system(str(sas_query))

def main():
        print datetime.now()
	todayDate = date.today()

	#run_sas_stmnt('/data/Shine/CareerProgressionTool/Model/SASCode/revivals.sas','/data/Shine/CareerProgressionTool/log/revivals.log')	
	run_sas_stmnt('/data/Shine/CareerProgressionTool/Model/SASCode/revivals_new_2.sas','/data/Shine/CareerProgressionTool/log/revivals_new_2.log')
	print datetime.now()

if __name__=='__main__':
    main()
