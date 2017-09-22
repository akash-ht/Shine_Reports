import xlwt
import glob
import os
import csv
from datetime import datetime, date
from dateutil.relativedelta import *

todayDate=date.today()
previousDate=todayDate+relativedelta(days=-1)

#wbk = xlwt.Workbook();

def run_sas_stmnt(sas_program, sas_log_file):
        sas_temp_file="/data/Analytics/temp"
        sas_query="/data/SAS/sasinstall_STAT/SASFoundation/9.2/sas -work "+str(sas_temp_file)+" -log "+str(sas_log_file)+" -SYSIN "+str(sas_program)+" -nonews -noterminal"
        print str(sas_query)
        os.system(str(sas_query))

def mergeCSVtoExcel(source,destination):
	wbk = xlwt.Workbook()
	for filename in glob.glob(source):
		print filename
		(path,namel)= os.path.split(filename)
		print namel
        	(name,extension) = os.path.splitext(namel)
		print name
	        sheet = wbk.add_sheet(str(name))
	        readr = csv.reader(open(filename,'rb'), delimiter=',')
	        for rowx,row in enumerate(readr):
	           for colx,value in enumerate(row):
	                sheet.write(rowx,colx,value)
	        wbk.save(destination)

def UpdateSASdataset():
        sas_program = '/data/Shine/Shine_AdHoc/Model/SASCode/breakup.sas'
        sas_log_file = '/data/Shine/Shine_AdHoc/log/breakup.log'
        run_sas_stmnt(sas_program, sas_log_file)
    
def CheckDir(directory):
	if os.path.isdir(directory):
                print "Directory Exists"
        else:
                os.mkdir(directory)

def main():  
	starttime = datetime.now(); print '\n'+str(starttime)
	print datetime.now()
	print 'SAS Statements'
	UpdateSASdataset()
	dest = '/data1/apacheRoot/html/shine/ReportArchieve/ActiveDBbreakups/' + todayDate.strftime("%b-%Y")	#previousDate.strftime("%b-%Y")
	CheckDir(dest)
	print datetime.now()
	print 'Active DB breakup'
	destination = dest + '/activeDB_breakup.xls'
	CheckDir('/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup')
	CheckDir('/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Attributes/')	
	CheckDir('/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Industry/')
	CheckDir('/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Function/')
	CheckDir('/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Experience/')
	mergeCSVtoExcel('/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Attributes/*',destination)
	print datetime.now()
        print 'Industry breakup'
	destination = dest + '/industry_wise_breakup.xls'
	mergeCSVtoExcel('/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Industry/*',destination)
	print datetime.now()
        print 'Function breakup'
	destination = dest + '/Function_wise_breakup.xls'
        mergeCSVtoExcel('/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Function/*',destination)
	print datetime.now()
        print 'Experience breakup'
	destination = dest + '/Experience_wise_breakup.xls'
	mergeCSVtoExcel('/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Experience/*',destination)

	os.system('rm /data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Attributes/*')
	os.system('rm /data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Industry/*')
        os.system('rm /data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Function/*')
        os.system('rm /data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Experience/*')

	print datetime.now()
	finishtime = datetime.now(); print '\n'+str(finishtime)

if __name__=='__main__':
    main()
