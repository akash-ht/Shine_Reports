import os, re, string, ftplib, traceback,sendMailUtil
from datetime import date, datetime
from dateutil.relativedelta import *

todayDate=date.today()
td = date.today()
yd = td+relativedelta(days=-1)

def main():
#Code lines to get data from CareerPlus database
	print 1
	#sqlQuery="/data/Harsh_singal/Shine/CareerPlus/Model/SQL/Weekly_Open_Allocables.sql"
	sqlQuery="/data/Harsh_singal/Shine/CareerPlus/Model/SQL/Weekly_Open_Allocables_v1.sql"
	outPut="/data/Harsh_singal/Shine/CareerPlus/Output/Weekly_Open_AllocItems_"+td.strftime("%d-%m-%Y")+".csv"
	generateReport(sqlQuery,outPut,"shinecp")
#Code lines to mail the ouput file generated
	uploadFile()
	
def generateReport(sqlFile, outFile, DB, options = ""):
	print 2
	command="mysql -uroot -h172.22.65.170 -P3311 -S /var/lib/cp_mysql/mysql5.sock "+options+" "+DB+" < "+str(sqlFile)+" |sed 's/\t/,/g'  > "+str(outFile)
	print command
	os.system(command)

def uploadFile():
	print 3
	outPut1="/data/Harsh_singal/Shine/CareerPlus/Output/Weekly_Open_AllocItems_"+td.strftime("%d-%m-%Y")+".csv"
	sourceFile=outPut1
	destination="/data1/apacheRoot/html/shine/ReportArchieve/Open_Allocable_Items/"+todayDate.strftime("%b-%Y")
	moveData(sourceFile,destination)

def moveData(sourceFile,destination):
	print 4
        if os.path.isfile(sourceFile):
                print "ok , file found"
                checkdir(destination)
                copyCommand="mv "+sourceFile+" "+destination
                os.system(copyCommand)
                fileName=sourceFile[sourceFile.rfind("/")+1:]
                destFile=destination+"/"+fileName
                print destFile
                if os.path.isfile(destFile):
                        print "file copied successfull"
                        return 1
                else:
                        return 0

def checkdir(PATH):
        if os.path.isdir(PATH):
                PATH
        else:
                os.mkdir(PATH)

if __name__=='__main__':
     main()
