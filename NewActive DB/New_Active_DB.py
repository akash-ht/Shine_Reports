import poplib, sys,datetime, email, os
from pymongo import Connection
from pymongo.objectid import ObjectId
from datetime import date, datetime, time
from dateutil.relativedelta import *
import sendMailUtil
import csv
from pymongo import *
import re

todayDate=date.today()
file_date = todayDate+relativedelta(days=-1)
file_date = file_date.strftime("%d-%b-%Y")
todayDate = todayDate.strftime("%d-%b-%Y")
todayDate = datetime.strptime(todayDate,"%d-%b-%Y")

previousDate=todayDate+relativedelta(days=-1)
previousDate = previousDate.strftime("%d-%b-%Y")
previousDate = datetime.strptime(previousDate,"%d-%b-%Y")


Month = previousDate.strftime("%b-%Y")
Year = previousDate.strftime("%Y")
day1 = datetime.combine(previousDate, time(0, 0))
day2 = datetime.combine(todayDate, time(0, 0))

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

directory = '/data/Shine/Shine_AdHoc/Output/New_Active_DB_Count' + '/' + str(Year) + '/' + str(Month) + '/' 
if os.path.isdir(directory):
        print "Directory Exists"
else:
        os.makedirs(directory)
        print "Directory_Created"
        
try:
    mongo_conn = getMongoConnection('172.22.65.157',27018, 'sumoplus',True, True)
except:
    mongo_conn = getMongoConnection('172.22.65.88',27018, 'sumoplus',True, True)
    
collection = getattr(mongo_conn, 'CandidateStatic')
collection_1 = getattr(mongo_conn, 'CandidateResumes')

def mail_file(file):
    mailing_list = ['prateek.agarwal1@hindustantimes.com']
    cc_list =['jitender.kumar@hindustantimes.com','akash.verma@hindustantimes.com','ashima.aggarwal@hindustantimes.com']
    bcc_list = []
    Subject = 'Active_DB_Count'
    Content = 'PFA the Count of Active DB  \n\n Regards \n Akash'
    attachment = [file]
    sendMailUtil.send_mail(mailing_list ,cc_list, bcc_list, Subject ,Content,attachment ,0)

def getActive_Db_Count():
    
    Active_DB_New_1_list = []
    Active_DB_New_2_list = []
    Active_DB_New_3_list = []
    Cand_Resume_List = []                                                                                                                                                                                                           
    
    Active_DB_New_1 = collection.find(
                                      {
                                       'll':{'$gte':datetime.combine(todayDate + relativedelta(days = -183),time(0,0))},
                                       '$or':[{'rm':0,'mo':0},{'rm':1,"mo":0},{'rm':0,"mo":2}],
                                       'jp':{'$in':[None,0]}
                                       },{'_id':1}
                                      )
    
    for records in Active_DB_New_1:
        Active_DB_New_1_list.append(str(records['_id']).strip())
    print "Total LL Cand_Picked : " +str(len(Active_DB_New_1_list))
    
    Active_DB_New_2 = collection.find(
                                      {
                                       'em':{'$gte':datetime.combine(todayDate+relativedelta(days = -1095),time(0,0))}
                                       },{'_id':1}
                                      )
    
    Active_DB_New_3 = collection.find(
                                      {
                                       'yeu':{'$gte':datetime.combine(todayDate + relativedelta(days = -1095),time(0,0))},
                                       'slm':{'$gte':datetime.combine(todayDate + relativedelta(days = -1095),time(0,0))}
                                       },{'_id':1}
                                      
                                      )
    for records in Active_DB_New_2:
        Active_DB_New_2_list.append(str(records['_id']).strip())
    print "Total Active_DB_2 Cand Picked :" +str(len(Active_DB_New_2_list))
    
    
    for records in Active_DB_New_3:
        Active_DB_New_3_list.append(str(records['_id']).strip())
    print "Total Active_DB_3 Cand Picked :" +str(len(Active_DB_New_3_list))    
    
    
    Cand_Resume = collection_1.find({'cd':{'$gte':datetime.combine(todayDate+relativedelta(days=-1095), time(0, 0))}},{'fcu':1,'cd':1})
    
    for records in Cand_Resume:
        Cand_Resume_List.append(str(records['fcu']))
        
        
    print "Total Cand_Resume_Picked :" +str(len(Cand_Resume_List))
    
    ################## Putting OR Condition in TWO List ####################
    ########################################################################
    
    First_Condition  = list(set(Active_DB_New_2_list) or set(Active_DB_New_3_list))
    print "First_Condtion :" +str(len(First_Condition))
     
    ################# Taking AND of Two List ###############################
    ########################################################################
    
    Second_Condition = list(set(First_Condition) or  set(Cand_Resume_List))
    
    print "Second_Condition :" +str(len(Second_Condition))
    
    Third_Condition = list(set(Second_Condition) and set(Active_DB_New_1_list))
    New_Active_DB_Count = len(Third_Condition)	
    
    print "Active Db Count :" +str(len(Third_Condition))
    
    ExitedCandidates_Old = collection.find({'ll':{'$gte':datetime.combine(todayDate+relativedelta(days=-184), time(0, 0)), '$lt':datetime.combine(todayDate+relativedelta(days=-183), time(0, 0))}, 
                                            'rm':0, 'mo':0,
                                            'lm':{'$gte':datetime.combine(todayDate+relativedelta(days=-730), time(0, 0))}}).count()
                                            
    ExitingCandidates_Old = collection.find({'ll':{'$gte':datetime.combine(todayDate+relativedelta(days=-183), time(0, 0)), '$lt':datetime.combine(todayDate+relativedelta(days=-182), time(0, 0))},
                                          'rm':0, 'mo':0,
                                          'lm':{'$gte':datetime.combine(todayDate+relativedelta(days=-730), time(0, 0))}}).count()
    
    ActiveDB_Old = collection.find({'ll':{'$gte':datetime.combine(todayDate+relativedelta(days=-183), time(0, 0))}, 
                                    'rm':0, 'mo':0, 
                                    'jp':{'$in':[None,0]},'lm':{'$gte':datetime.combine(todayDate+relativedelta(days=-730), time(0, 0))}}).count()
    
    ProfileCompletions = collection.find({'red':{'$gte':day1,'$lt':day2},'rm':0,'mo':0,'e':{'$not':re.compile('mailinator.com')}}).count()
    
    print 'Total Acquisitions Yesterday : ' +str(ProfileCompletions)
    
    PureLogins = collection.find({'ll':{'$gte':day1, '$lt':day2},'$or':[{'red':{'$lt':day1}},{'red':None,'rsd':{'$lt':day1}}], 
                                  'rm':0, 'mo':0,'e':{'$not':re.compile('mailinator.com')}}).count()
    
    
    print 'Active DB_Old Count :' +str(ActiveDB_Old)
                                  
    print 'Pure Logins Count: ' +str(PureLogins)
    
    Exited_Cand_New_1 = collection.find(
                                      {
                                       'll':{'$gte':datetime.combine(todayDate + relativedelta(days = -184),time(0,0)),'$lte':datetime.combine(todayDate + relativedelta(days = -183),time(0,0))},
                                       '$or':[{'rm':0,'mo':0},{'rm':1,"mo":0},{'rm':0,"mo":2}],
                                       'jp':{'$in':[None,0]}
                                       },{'_id':1}
                                      )
    Exited_Cand_New_list = []
    
    for records in Exited_Cand_New_1:
        Exited_Cand_New_list.append(str(records['_id']).strip())
    print 'Exited_Cand_New_List',len(Exited_Cand_New_list)
   
    Exited_Candiates_Total_New = list(set(Second_Condition) and set(Exited_Cand_New_list))
    Exited_Candidates_New = len(Exited_Candiates_Total_New)
    
    print 'New_Exited',Exited_Candidates_New
    
    Exiting_Cand_New_1 = collection.find(
                                      {
                                       'll':{'$gte':datetime.combine(todayDate + relativedelta(days = -183),time(0,0)),'$lt':datetime.combine(todayDate + relativedelta(days = -182),time(0,0))},
                                       '$or':[{'rm':0,'mo':0},{'rm':1,"mo":0},{'rm':0,"mo":2}],
                                       'jp':{'$in':[None,0]}
                                       },{'_id':1}
                                      )
       
    Exiting_Cand_New_List = []
    
    for records in Exiting_Cand_New_1:
        Exiting_Cand_New_List.append(str(records['_id']).strip())
        
    
    Exiting_1 = collection.find(
                                      {
                                       'em':{'$gte':datetime.combine(todayDate+relativedelta(days = -1094),time(0,0))}
                                       },{'_id':1}
                                      )
    
    Exiting_2 = collection.find(
                                      {
                                       'yeu':{'$gte':datetime.combine(todayDate + relativedelta(days = -1094),time(0,0))},
                                       'slm':{'$gte':datetime.combine(todayDate + relativedelta(days = -1094),time(0,0))}
                                       },{'_id':1}
                                      
                                      )
    
    Exiting_3 = collection_1.find({'cd':{'$gte':datetime.combine(todayDate+relativedelta(days=-1094), time(0, 0))}},{'fcu':1,'cd':1})
    
    print 'Exiting_Cand_New_List',len(Exiting_Cand_New_List)
    
    First_Condition  = list(set(Exiting_1) or set(Exiting_2))
    Second_Condition = list(set(First_Condition) or  set(Exiting_3))
    Third_Condition = list(set(Second_Condition) and set(Exiting_Cand_New_List))
    print 'Exiting_Cands_New',str(len(Third_Condition))
    
    Output_File = directory + "New_Check_List" +str(file_date) + ".csv"
    ofile = open(Output_File,"wb")
    writer = csv.writer(ofile)
    writer.writerow(['Active_DB_Count_New','Active_DB_Old','Exited_Candidates_New','Exited_Candidates_Old',
                     'Exiting_Candidates_New','ExitingCandidates_Old','ProfileCompletions','PureLogins'])
    writer.writerow([New_Active_DB_Count,ActiveDB_Old,Exited_Candidates_New,ExitedCandidates_Old,
                     Exiting_Candidates_New,ExitingCandidates_Old,ProfileCompletions,PureLogins])
    ofile.close()	
    mail_file(Output_File)
        
    
def main():
    
    print datetime.now()
    getActive_Db_Count()
    print datetime.now()
    

if __name__ == '__main__':
    main()
    
