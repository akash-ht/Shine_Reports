import pandas
import os,subprocess
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
outFile = '/data/Shine/CareerProgressionTool/Output/Modifies_skills_'+previousDate.strftime("%d-%m-%Y")+'.csv'

def main():
	print datetime.now()

	dir = '/data/Analytics/Utils/consolidatedDB/Input/dataFromMongo/Incrementals/'
	cand_skill = dir + 'CandidatePreferencesInc.csv'
	dest = '/data/Shine/CareerProgressionTool/Input1/'
	cand_pref = dest + 'CandidatePreferencesInc.csv'
	cand_pref_1 = dest + 'CandidatePreferencesInc_v1.csv'
	skill_lookup = dest + 'lookup_skill.csv'
	uniq_userids = dest + 'uniq_userids.csv'
	
	os.system('cp '+cand_skill+' '+cand_pref)
	
	#command = "tail -n+2 /data/Shine/CareerProgressionTool/Output/dailyModifies_"+previousDate.strftime('%d-%m-%Y')+".csv |awk -F, '{print $1}' /data/Shine/CareerProgressionTool/Output/dailyModifies_"+previousDate.strftime('%d-%m-%Y')+".csv |sort |uniq > /data/Shine/CareerProgressionTool/Output/UserIds_"+previousDate.strftime('%d-%m-%Y')+".csv"
	
	command = "awk -F, '{print $1}' /data/Shine/CareerProgressionTool/Output/Modifies_"+previousDate.strftime('%d-%m-%Y')+".csv |sort |uniq > /data/Shine/CareerProgressionTool/Output/UserIds_1"+previousDate.strftime('%d-%m-%Y')+".csv"
	
	os.system(command)
	command = "sed '/UserId/d' /data/Shine/CareerProgressionTool/Output/UserIds_1"+previousDate.strftime('%d-%m-%Y')+".csv > /data/Shine/CareerProgressionTool/Output/UserIds_"+previousDate.strftime('%d-%m-%Y')+".csv"  
        os.system(command)
        
	uniq_userids = "/data/Shine/CareerProgressionTool/Output/UserIds_"+previousDate.strftime('%d-%m-%Y')+".csv"
	command = "sed s/'None'/''/g "+cand_pref +" > "+cand_pref_1
	print command
	os.system(command)
	
	skills_file = pandas.read_csv(cand_pref_1)
        skills_file = skills_file[['fcu','t','v','vc']]
	
	skills_file = skills_file.loc[(skills_file["t"]==3),('fcu','t','v','vc')]
	skills_file.rename(columns={'fcu':'UserId'},inplace = True)
	col_name = ['UserId']
	userids = pandas.read_csv(uniq_userids,names=col_name)
	merged_sk = userids.merge(skills_file,on='UserId',how='left')
	del skills_file
	del userids
	lookup_skill = pandas.read_csv(skill_lookup)
	skill = merged_sk.merge(lookup_skill,on='v',how='left')
	del merged_sk
	del skill_lookup
	skill.loc[(skill['vc'].notnull() == True),'Skill'] = skill['vc']
        skill.loc[(skill['vc'].notnull() == False),'Skill'] = skill['skill_desc']
        skill = skill[['UserId','Skill']]
        skill.to_csv(outFile,index= False)
	print outFile
	    
	print datetime.now()
	
if __name__=='__main__':
	main()
