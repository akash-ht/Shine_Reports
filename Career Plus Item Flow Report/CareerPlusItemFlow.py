import os, re, string, ftplib, traceback,sendMailUtil
from datetime import date, datetime
from dateutil.relativedelta import *

todayDate = date.today()
td = date.today()
yd = td+relativedelta(days=-1)

def main():
#Code line to get and move closure file to reports wiki
	sqlQuery="/data/Harsh_singal/Shine/CareerPlus/Model/SQL/CareerPlus_ItemFlow.sql"
	outPut="/data/Harsh_singal/Shine/CareerPlus/Output/CareerPlus_ItemFlow_Report_"+yd.strftime("%d-%m-%Y")+".csv"
	generateReport(sqlQuery,outPut,"shinecp")
	destination="/data1/apacheRoot/html/shine/ReportArchieve/CareerPlus_ItemFlow/"+(todayDate+relativedelta(days=-1)).strftime("%b-%Y")
	moveData(outPut,destination)

def generateReport(sqlFile, outFile, DB, options = ""):
	command="mysql -uroot -h172.22.65.170 -P3311 -S /var/lib/cp_mysql/mysql5.sock "+options+" "+DB+" < "+str(sqlFile)+" |sed 's/\t/,/g'  > "+str(outFile)
        print command
        os.system(command)

def moveData(sourceFile,destination):
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
