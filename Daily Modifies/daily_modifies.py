import pandas
import os
import datetime
from datetime import date, datetime, time
from dateutil.relativedelta import *
from pymongo import Connection

todayDate=date.today()
previousDate=todayDate+relativedelta(days=-15)
#previousDate=todayDate+relativedelta(days=-1)
previousDate2=todayDate+relativedelta(days=0)
day1 = datetime.combine(previousDate, time(0, 0))
day2 = datetime.combine(previousDate2, time(0, 0))

file = '/data/Shine/CareerProgressionTool/Output/dailyModifies.csv'
outFile = '/data/Shine/CareerProgressionTool/Output/Modifies_'+previousDate.strftime("%d-%m-%Y")+'.csv'

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

def getDataFromMongo(file):
        try:
		mongo_conn = getMongoConnection('172.22.65.88',27018, 'sumoplus',True, True)
	except:
		mongo_conn = getMongoConnection('172.22.65.157',27018, 'sumoplus',True, True)
	collection = getattr(mongo_conn, 'CandidateStatic')
        collection1 = getattr(mongo_conn, 'CandidateJobs')
        collection2 = getattr(mongo_conn, 'CandidateEducation')	
	ofile = open(file,'wb+')
	#ofile2 = open(file2,'wb+')
	count =0
	ofile.write('UserId,Exp_years,Exp_months,Last_Modified,JobTitleId,JobTitleCustom,Ind,Study_Field,Edu_level,course_type,SubFunc,Salary,Salary_Lakhs,Salary_Thousands\n')
        rows = collection.find({'$or':[{'lm':{'$gte':day1,'$lt':day2}},{'red':{'$gte':day1,'$lt':day2}}],'mo':0,'rm':0},{"_id":1,"ex":1,"exm":1,"s":1,"sl":1,"st":1,"lm":1})
        for row in rows:
            count += 1
            UserId = str(row['_id'])
	    if (UserId == ''):
		continue
            try:
                Exp_years = str(row['ex'])
            except:
                Exp_years = ''
            try:
                Exp_months = str(row['exm'])
            except:
                Exp_months = ''
            try:
                Salary = str(row['s'])
            except:
                Salary = ''
            try:
                Salary_Lakhs = str(row['sl'])
            except:
                Salary_Lakhs = ''
            try:
                Salary_Thousands = str(row['st'])
            except:
                Salary_Thousands = ''
            try:
                Last_Modified = str(row['lm'])
            except:
                Last_Modified = ''

            Exp_years = Exp_years.replace('None','')
            Salary = Salary.replace('None','')
            list1 =[]
            list2 = []
            #print UserId
            
            rows1 = collection1.find({'fcu':UserId},{'mr':1})
            c1 =0
            for row1 in rows1:
                c1 +=1
                list1.append(row1['mr'])
            if (c1 == 0):
                JobTitleId = JobTitleCustom = Ind = SubFunc = ''
            else:
                high = max(list1)
                #print high,UserId
                rows1 = collection1.find({'fcu':UserId,'mr':high},{'jti':1,'jtc':1,'i':1,'sf':1})
                for row1 in rows1:
                    try:
                        JobTitleId = str(row1['jti'])
                    except:
                        JobTitleId = ''
                    try:
                        JobTitleCustom = str(row1['jtc'])
                    except:
                        JobTitleCustom = ''
                    try:
                        Ind = str(row1['i'])
                    except:
                        Ind = ''
                    try:
                        SubFunc =str(row1['sf'])
                    except:
                        SubFunc = ''

                    JobTitleId = JobTitleId.replace('None','')
                    JobTitleCustom = JobTitleCustom.replace(',',';')
                    JobTitleCustom = JobTitleCustom.replace('\'','')
                    JobTitleCustom = JobTitleCustom.replace('\"','')
                    Ind = Ind.replace('None','')
                    SubFunc = SubFunc.replace('None','')
                    
            rows2 = collection2.find({'fcu':UserId},{'el':1})
            c2 =0
            for row2 in rows2:
                c2 += 1
                list2.append(row2['el'])
            if (c2 == 0):
                Edu_level = course_type = Study_Field = ''
            else:
                #print UserId
                high_el = max(list2)
                rows2 = collection2.find({'fcu':UserId,'el':high_el},{'el':1,'ct':1,'sf':1})
                for row2 in rows2:
                    try:
                        Edu_level = str(row2['el'])
                    except:
                        Edu_level = ''
                    try:
                        course_type = str(row2['ct'])
                    except:
                        course_type = ''
                    try:
                        Study_Field = str(row2['sf'])
                    except:
                        Study_Field = ''
                        
                    Edu_level = Edu_level.replace('None','')
                    course_type = course_type.replace('None','')
                    Study_Field = Study_Field.replace('None','')
       
	    ''' 
	    rows3 = collection3.find({'fcu':UserId,'t':3},{'fcu':1,'v':1,'vc':1})
	    for row3 in rows3:
		try:
			value = str(row2['v'])
                except:
			value = ''
		try:
			value_custom = str(row2['vc'])
                except:
                        value_custom = ''
	     '''	
                        
                 
            ofile.write(UserId+','+Exp_years+','+Exp_months+','+Last_Modified+','+JobTitleId+','+JobTitleCustom+','+Ind+','+Study_Field+','+Edu_level+','+course_type+','+SubFunc+','+Salary+','+Salary_Lakhs+','+Salary_Thousands+'\n')
            if (count % 10000 == 0):
                print count
                #break
        ofile.close()

def getValues(value1,value2):
	if (value2==''):
		return value1
	else:
		return value2

def main():
	print datetime.now()
	getDataFromMongo(file)
	
	dir = '/data/Analytics/Utils/consolidatedDB/Input/dataFromMongo/Incrementals/'
	dest = '/data/Shine/CareerProgressionTool/Input1/'
	cand_pref = dir + 'CandidatePreferencesInc.csv'
	sal_lookup = dest + 'lookup_salary_avg.csv'
	exp_lookup = dest + 'lookup_experience.csv'
	ct_lookup = dest + 'lookup_course_type.csv'
	el_lookup = dest + 'lookup_education_level.csv'
	sf_lookup = dest + 'lookup_stream.csv'
	subFA_lookup = dest + 'lookup_sub_function.csv'
	industry_lookup  = dest + 'lookup_industry.csv'
	job_title_lookup  = dest + 'lookup_job_title.csv'
		
	#cand_static = '/data/Analytics/Utils/cVVonsolidatedDB/Input/dataFromMongo/Incrementals/CandidateStaticInc_chk.csv'	
	
	'''
	skills_file = pandas.read_csv(cand_pref,sep=',')
        skills_file = skills_file[['fcu','t','v','vc']]
	skills_file = skills_file.loc[(skills_file["t"]==3),('fcu','t','v','vc')]
	
		
	'''
	#file_bk = '/data/Shine/CareerProgressionTool/Output/dailyModifies_04-09-2014.csv'
	ifile = pandas.read_csv(file)
	lookup_industry = pandas.read_csv(industry_lookup)
	merged = ifile.merge(lookup_industry,on='Ind',how='left')
	del ifile
	del lookup_industry
        lookup_subfunction = pandas.read_csv(subFA_lookup)
        merged1 = merged.merge(lookup_subfunction,on='SubFunc',how='left')
	del lookup_subfunction
	del merged
        lookup_exp = pandas.read_csv(exp_lookup)
        merged2 = merged1.merge(lookup_exp,on='Exp_years',how='left')
	del merged1
	del lookup_exp
        lookup_job_title = pandas.read_csv(job_title_lookup)
        merged3 = merged2.merge(lookup_job_title,on='JobTitleId',how='left')
	del lookup_job_title
	del merged2
        lookup_specialization =  pandas.read_csv(sf_lookup)
        merged4 = merged3.merge(lookup_specialization,on='Study_Field')
	del merged3
	del lookup_specialization
	lookup_education_level =  pandas.read_csv(el_lookup)
	merged5 = merged4.merge(lookup_education_level,on='Edu_level',how='left')
	del merged4
	del lookup_education_level
	lookup_course_type =  pandas.read_csv(ct_lookup)
	merged6 = merged5.merge(lookup_course_type,on='course_type',how='left')	
	del lookup_course_type
	del merged5
	lookup_sal = pandas.read_csv(sal_lookup)
	merged7 = merged6.merge(lookup_sal,on='Salary',how='left')
	del merged6
	del lookup_sal
	
        merged7 = merged7[['UserId','Experience_years','Exp_months','Last_Modified','Industry','FA','SubFA','EducationLevel','CourseType','AvgSal','Salary_Lakhs','Salary_Thousands','JobTitleCustom','Job_Title','subject','specialization']]
        merged7.loc[(merged7['JobTitleCustom'].notnull() == True),'JobTitle'] = merged7['JobTitleCustom']
        merged7.loc[(merged7['JobTitleCustom'].notnull() == False),'JobTitle'] = merged7['Job_Title']

        #print merged7['Salary_Lakhs'],merged7['Salary_Thousands']
        #try:
        #merged7['Sal'] = merged7['Salary_Lakhs'] + (merged7['Salary_Thousands']/100)
        #except:
        #print merged7['Salary_Lakhs'],merged7['Salary_Thousands']
                
        #print merged7.loc[(merged7['Sal'].notnull() == False)]
        #merged7.loc[(merged7['Sal'].notnull() == True),'Salary'] = merged7['Sal']
        #merged7.loc[(merged7['Sal'].notnull() == False),'Salary'] = merged7['AvgSal']

        merged7 = merged7[['UserId','Experience_years','Exp_months','Last_Modified','Industry','FA','SubFA','EducationLevel','CourseType','AvgSal','Salary_Lakhs','Salary_Thousands','JobTitle','subject','specialization']]
        merged7.to_csv(outFile,index= False)
        
	print outFile
	print datetime.now()
	
if __name__=='__main__':
	main()
