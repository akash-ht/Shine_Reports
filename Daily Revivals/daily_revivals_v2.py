from pymongo.objectid import ObjectId
import pandas
import os
import datetime
from datetime import date, datetime, time
from dateutil.relativedelta import *
from pymongo import Connection

todayDate=date.today()
previousDate=todayDate+relativedelta(days=-1)
previousDate2=todayDate+relativedelta(days=0)
day1 = datetime.combine(previousDate, time(0, 0))
day2 = datetime.combine(previousDate2, time(0, 0))

file = '/data/Shine/CareerProgressionTool/Output/dailyRevivals.csv'
outFile = '/data/Shine/CareerProgressionTool/Output/dailyRevivals_'+previousDate.strftime("%d-%m-%Y")+'.csv'
ofile = open(file,'wb+')

file2 = '/data/Shine/CareerProgressionTool/Output/dailyRevivals_skills.csv'
outFile2 = '/data/Shine/CareerProgressionTool/Output/dailyRevivals_skills_'+previousDate.strftime("%d-%m-%Y")+'.csv'
ofile2 = open(file2,'wb+')

#ofile.write('UserId,Exp_years,Exp_months,Last_Modified,JobTitleId,JobTitleCustom,Ind,Study_Field,Edu_level,course_type,SubFunc,Salary,Salary_Lakhs,Salary_Thousands\n')
   	
def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

mongo_conn = getMongoConnection('172.22.65.157',27018, 'sumoplus',True, True)
collection = getattr(mongo_conn, 'CandidateStatic')
collection1 = getattr(mongo_conn, 'CandidateJobs')
collection2 = getattr(mongo_conn, 'CandidateEducation')	
collection3 = getattr(mongo_conn, 'CandidatePreferences')

def getDataFromMongo(UserId):
        count =0
        if len(UserId)>20:
            rows = collection.find({'_id':ObjectId(UserId)},{"_id":1,"ex":1,"exm":1,"s":1,"sl":1,"st":1,"lm":1})
        else:
            rows = collection.find({'_id':UserId},{"_id":1,"ex":1,"exm":1,"s":1,"sl":1,"st":1,"lm":1})
        Exp_years = Exp_months = Salary = Salary_Lakhs = Salary_Thousands = Last_Modified = ''
        for row in rows:
            count += 1
            UserId = str(row['_id'])
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
		JobTitleId = JobTitleCustom = Ind = SubFunc = ''
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
                    JobTitleCustom = JobTitleCustom.replace('\'',';')
                    JobTitleCustom = JobTitleCustom.replace('\"',';')
                    Ind = Ind.replace('None','')
                    JobTitleCustom = JobTitleCustom.replace('None','')
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
		Edu_level = course_type = Study_Field = ''
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
       
            ofile.write(UserId+','+Exp_years+','+Exp_months+','+Last_Modified+','+JobTitleId+','+JobTitleCustom+','+Ind+','+Study_Field+','+Edu_level+','+course_type+','+SubFunc+','+Salary+','+Salary_Lakhs+','+Salary_Thousands+'\n')
            if (count % 10000 == 0):
                print count
            rows3 = collection3.find({'fcu':UserId,'t':3},{'v':1,'vc':1})
	    skill_id = skill_custom = ''
            for row3 in rows3:
                try:
                    skill_id = str(row3['v'])
                except:
                    skill_id = ''
                try:
                    skill_custom = str(row3['vc'])
                except:
                    skill_custom = ''
                skill_custom = skill_custom.replace('None','')
                skill_custom = skill_custom.replace(',',';')
                skill_custom = skill_custom.replace('\'','')
                skill_custom = skill_custom.replace('\"',';')
                skill_id = skill_id.replace('None','')
                ofile2.write(UserId+','+skill_id+','+skill_custom+'\n')
            
    #ofile.close()

def main():
	print datetime.now()
	userids_file = '/data/Shine/CareerProgressionTool/Output/revivals_'+previousDate.strftime("%Y-%m-%d")+'.csv'
	#userids_file = '/data/Shine/CareerProgressionTool/Output/revivals_chk_29sep.csv'
        userids = open(userids_file,'rb+')
        header = 'true'
        for line in userids:
            line = line.strip('\r\n')
            UserId = line.split(',')[0]
            if (header == 'true'):
                header = 'false'
                continue
            getDataFromMongo(UserId)
        ofile.close()
        ofile2.close()
	
	file_1 = '/data/Shine/CareerProgressionTool/Output/dailyRevivals_none.csv'
	command = "sed s/'None'/''/g "+file +" > "+file_1
	os.system(command)
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
	skill_lookup = dest + 'lookup_skill.csv'
	
	#file_bk = '/data/Shine/CareerProgressionTool/Output/dailyModifies_04-09-2014.csv'
	column_names = ['UserId','Exp_years','Exp_months','Last_Modified','JobTitleId','JobTitleCustom','Ind','Study_Field','Edu_level','course_type','SubFunc','Salary','Salary_Lakhs','Salary_Thousands']
	ifile = pandas.read_csv(file_1,names=column_names)
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
        del merged7
	print outFile
	#os.system('scp -i /data/Shine/CareerProgressionTool/Analytics.pem /data/Shine/CareerProgressionTool/Output/dailyModifies_29-09-2014.csv ubuntu@172.22.66.198:/data/Projects/SimilarProfiles/Input/DailyCandInput/')

        file2_1 = '/data/Shine/CareerProgressionTool/Output/dailyRevivals_skills_none.csv'
        command = "sed s/'None'/''/g "+file2 +" > "+file2_1
	os.system(command)
        column_names = ['UserId','skill_id','skill_custom']
        ifile2 = pandas.read_csv(file2_1,names=column_names)
        lookup_skill = pandas.read_csv(skill_lookup)
        ifile2.rename(columns={'skill_id':'v'},inplace = True)
	merged = ifile2.merge(lookup_skill,on='v',how='left')
	del ifile2
	del lookup_skill
	merged.loc[(merged['skill_custom'].notnull() == True),'Skill'] = merged['skill_custom']
        merged.loc[(merged['skill_custom'].notnull() == False),'Skill'] = merged['skill_desc']
        merged = merged[['UserId','Skill']]
        merged.to_csv(outFile2,index= False)
        del merged
	print outFile2

        daily_file_1 = '/data/Shine/CareerProgressionTool/Output/dailyModifies_'+previousDate.strftime("%d-%m-%Y")+'.csv'
	os.system('cat /data/Shine/CareerProgressionTool/Output/Modifies_'+previousDate.strftime("%d-%m-%Y")+'.csv '+outFile+' > '+daily_file_1)
	if os.path.isfile(daily_file_1)==True:
		os.system('scp -i /data/Shine/CareerProgressionTool/Analytics.pem '+daily_file_1+' ubuntu@172.22.66.198:/data/Projects/SimilarProfiles/Input/DailyCandInput/')
	else:
		os.system('cp /data/Shine/CareerProgressionTool/Output/Modifies_'+previousDate.strftime("%d-%m-%Y")+'.csv '+daily_file_1)
		os.system('scp -i /data/Shine/CareerProgressionTool/Analytics.pem '+daily_file_1+' ubuntu@172.22.66.198:/data/Projects/SimilarProfiles/Input/DailyCandInput/')
	daily_file_2 = '/data/Shine/CareerProgressionTool/Output/dailyModifies_skills_'+previousDate.strftime("%d-%m-%Y")+'.csv'
	os.system('cat /data/Shine/CareerProgressionTool/Output/Modifies_skills_'+previousDate.strftime("%d-%m-%Y")+'.csv '+outFile2+' > '+daily_file_2)
	if os.path.isfile(daily_file_2)==True:
                os.system('scp -i /data/Shine/CareerProgressionTool/Analytics.pem '+daily_file_2+' ubuntu@172.22.66.198:/data/Projects/SimilarProfiles/Input/DailyCandInput/')
	else:
		os.system('cp /data/Shine/CareerProgressionTool/Output/Modifies_skills_'+previousDate.strftime("%d-%m-%Y")+'.csv '+daily_file_2)
		os.system('scp -i /data/Shine/CareerProgressionTool/Analytics.pem '+daily_file_2+' ubuntu@172.22.66.198:/data/Projects/SimilarProfiles/Input/DailyCandInput/')

	print datetime.now()
	
if __name__=='__main__':
	main()
