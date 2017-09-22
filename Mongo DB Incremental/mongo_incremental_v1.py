import os
import csv
from pymongo import *
from pymongo.objectid import ObjectId
from datetime import datetime, date, time, timedelta
from dateutil.relativedelta import *
import dateutil.parser
import MySQLdb
import traceback
import sys
from operator import itemgetter
import operator


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


ofile = open("/data/Shine/Shine_AdHoc/Output/Sample.txt","w")


    ############################## Mongo Connector Code #############################
    #################################################################################
def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

    ############################# MySQL Connector Code###############################
    #################################################################################
def getMySqlConnection(HOST, PORT, DB_USER, DB_PASSWORD, DB):
    return MySQLdb.connect(host = HOST,port = PORT,user =DB_USER,passwd =DB_PASSWORD,db = DB)
    


    ####################### Getting Data From Mongo Collections #####################
    ################################################################################# 
def getDatafromMongo():
    try:
        mongo_conn = getMongoConnection('172.22.65.157',27018, 'sumoplus',True, True)
    except:
        mongo_conn = getMongoConnection('172.22.65.88',27018, 'sumoplus',True, True)
    static_collection = getattr(mongo_conn,'CandidateStatic')
    education_collection = getattr(mongo_conn,'CandidateEducation')
    jobs_collection = getattr(mongo_conn,'CandidateJobs')
    educational_level_collection = getattr(mongo_conn,'LookupEducationQualificationLevel')
    course_type_collection = getattr(mongo_conn,'LookupCourseType')
    lookup_institute = getattr(mongo_conn,'LookupEducationInstitute')
    lookup_educationstudy = getattr(mongo_conn,'LookupEducationStream')
    lookup_jobtitle = getattr(mongo_conn,'LookupJobTitle')
    lookup_subfunction = getattr(mongo_conn,'LookupSubFunctionalArea')
    lookup_industry = getattr(mongo_conn,'LookupIndustry')
    lookup_company = getattr(mongo_conn,'LookupCompany')
    lookup_experience = getattr(mongo_conn,'LookupExperience')
    lookup_city = getattr(mongo_conn,'LookupCity')

    mysql_conn = getMySqlConnection('localhost', 3306, 'root', 'mysql321', 'INDExportDB')
    cursor = mysql_conn.cursor()  

    #Static_Data = static_collection.find({'_id':{'$gt':id}},{'_id':1,'e':1,'ev':1,'fn':1,'ln':1,'cp':1,'cpv':1,'ex':1,'exm':1,'rm':1,'mo':1,'ll':1,'lm':1,'lad':1,
    #                                                             'rsd':1,'red':1,'svi':1,'evi':1,'sl':1,'st':1,'ut':1,'ss':1,'jp':1,'bs':1}).sort('_id',1).limit(100)
    n = datetime.now()
    id = 0
    #id = '114052244685223184093244'
    start = 0
    #batch_size = 2
    batch_size = 1000
    a = open('test.csv','w')
    writer = csv.writer(a)
    print "start iteration"
    while True:
        Static_Data = static_collection.find({'$or':[{'ll':{'$gte':day1,'$lt':day2}},{'lm':{'$gte':day1,'$lt':day2}}]},{'_id':1,'e':1,'ev':1,'fn':1,'ln':1,'cp':1,'cpv':1,'ex':1,'exm':1,'rm':1,'mo':1,'ll':1,'lm':1,'lad':1,
                                                                 'rsd':1,'red':1,'svi':1,'evi':1,'sl':1,'st':1,'ut':1,'ss':1,'jp':1,'bs':1,'cl':1,'g':1}).sort('_id',1).skip(start).limit(batch_size)
        start += batch_size
        ######## Picking Data From Candidate Static Collection ##########
        #################################################################
        
        
                                            
        UserId_list =[]
        Data_list = []
        for records in Static_Data:
            writer.writerow([records['_id']])
            UserId_list.append(str(records['_id']))
            Data_list.append(records)

        if not len(UserId_list):
            break
        #print UserId_list
        print "--- \nRecords read upto: %s" % UserId_list[-1]
        #print len(UserId_list)
        
        #########Picking Education Related Details##############
        ########################################################
        #print "Creating Edu Dict"
        education_dict = {}
        Education_Data = education_collection.find({'fcu':{'$in':UserId_list}},{'el':1,'fcu':1,'mr':1,'ins':1,'inc':1,'ct':1,'yop':1,'sf':1})
        #Education_Data = education_collection.find({'fcu':{'$in':UserId_list}},{'el':1,'fcu':1,'mr':1,'ins':1,'inc':1,'ct':1,'yop':1,'sf':1}).sort([('yop',-1),('mr',-1)])
              
        #########Creating Dictionary of Course Type#############
        ########################################################
        Course_Type = course_type_collection.find({},{'v':1,'d':1})
        course_type_dict = {}
        
        for records in Course_Type:
            course_type_dict[records['v']] = records['d']
        
        #########Creating Dictionary of Education Level#########
        ########################################################
        Educational_Level = educational_level_collection.find({},{'eli':1,'tv':1})
        education_level_dict = {}
        
        for records in Educational_Level:
            education_level_dict[records['eli']] = records['tv']
        
        #########Creating Dictionary of Study Field#############
        ########################################################
        Study_Field = lookup_educationstudy.find({},{'si':1,'sd':1})
        study_field_dict = {}
        
        for records in Study_Field:
            study_field_dict[records['si']] = records['sd']
                        
        ######### Creating Dictionary of Institute##############
        ########################################################
        
        institute_dict = {}
        Institute_Name = lookup_institute.find({},{'asi':1,'asd':1})
        for records in Institute_Name:
            institute_dict[records['asi']] = records['asd']
        #print institute_dict

        
        
                
        for records in Education_Data:
            course_type = course_type_dict[records['ct']]
            if records.get('ct') == '':
                course_type_lookup =-1
            else:
                course_type_lookup = records.get('ct',-1)
            education_level = education_level_dict[records['el']]
            if records.get('el') == '':
                education_level_lookup = -1
            else:
                education_level_lookup = records.get('el',-1)
                
            if records.get('sf') == '':
                study_field_lookup = -1
            else:
                study_field_lookup = records.get('sf',-1)
                
            study_field = study_field_dict[records['sf']]
            
            if records.has_key('ins') == True and records.get('ins','') is not None:
                Institute_Name = institute_dict[records['ins']]
            else:
                Institute_Name = records.get('inc')
            
            
            if records['fcu'] in education_dict.keys():
                
                if records.get('ins','') is None:
                    records['ins'] = -1
                else:
                    pass
                            
                if records.has_key('yop') == 'False' or str(records.get('yop','')) == 'null' or str(records.get('yop','')) == '' or records['yop'] == 0 or type(records['yop']) != int : 
                    records['yop'] = 1900
                    #if records['mr'] > education_dict[records['fcu']][1]:
                    education_dict[records['fcu']].append([int(records['yop']),records.get('mr',0),Institute_Name,int(records.get('ins',-1)),course_type,course_type_lookup,
                                                               education_level,education_level_lookup,study_field,int(records.get('sf',-1))])
                        
                else:
                    #if records['yop'] > education_dic
                    education_dict[records['fcu']].append([int(records['yop']),records.get('mr',0),Institute_Name,int(records.get('ins',-1)),course_type,course_type_lookup,
                                                               education_level,education_level_lookup,study_field,int(records.get('sf',-1))])
                                       
            else:
                if records.has_key('yop') == 'False' or str(records.get('yop','')) == 'null' or str(records.get('yop','')) == '' or records['yop'] == 0 or type(records['yop']) != int : 
                    records['yop'] = 1900
                if records.get('ins','') is None:
                    records['ins'] = -1
                else:
                    pass
                education_dict[records['fcu']] = [[int(records.get('yop',1900)),records.get('mr',0),Institute_Name,int(records.get('ins',-1)),course_type,course_type_lookup,
                                                   education_level,education_level_lookup,study_field,int(records.get('sf',-1))]]
        
        ######### Picking Jobs Related Details###############
        #####################################################
        #print "Creating Job DIct"
        jobs_dict = {}
        
        #functional_area_dict = {}
        
        ############### Creating Dictionary of Sub-FA and FA###############
        ###################################################################
        sub_func_dict = {}
        SubFunction_Functional_Area = lookup_subfunction.find({},{'sfi':1,'sfe':1,'fi': 1,'fe':1})
        for records in  SubFunction_Functional_Area:
            sub_func_dict[records['sfi']] = (records['sfe'],records['fi'],records['fe'])
        #print sub_func_dict
        
        ############### Creating Dictionary of Years of Experience#########
        ###################################################################
        Experience_Yr = lookup_experience.find({},{'v':1,'d':1})
        experience_dict = {}
        for records in Experience_Yr:
            experience_dict[records['v']] = records['d']
        #print experience_dict
        
        ############### Creating Dictionary of Company Name################
        ###################################################################
        company_dict = {}
        Company_Name = lookup_company.find({},{'v':1,'d':1})
        for records in Company_Name:
            company_dict[records['v']] = records['d']
        
        ############### Creating Dictionary of Industry Name###############
        ###################################################################
        industry_dict = {}
        Industry_Name = lookup_industry.find({},{'ii':1,'idesc':1})
        for records in Industry_Name:
            industry_dict[records['ii']] = records['idesc']

        ############## Creating Dictionary of City Name####################
        ###################################################################
               
        city_dict = {}
        City_Name = lookup_city.find({},{'ci':1,'cd':1,'cgd':1,'cgi':1})
        for records in City_Name:
            city_dict[records['ci']] = (str(records['cd']),records['cgi'],str(records.get('cgd','N/A')))            
            
        ############# Creating Dictionary of Job Title#####################
        ###################################################################
        
        job_title_desc_dict = {}
        job_title_lookup_dict = {}
        Job_Title = lookup_jobtitle.find({},{'jti':1,'jtd':1})
        #Job_Title_Desc = lookup_jobtitle.find({},{'jtd':1,'jti':1})
        
        
        for records in Job_Title:
            #print records
            job_title_desc_dict[records['jti']] = records['jtd']
        Jobs_Data = jobs_collection.find({'fcu':{'$in':UserId_list}},{'fcu':1,'jti':1,'jtc':1,'sf':1,'ex':1,'i':1,'cn':1,'cnc':1,'em':1,'mr':1,'cr':1}).sort([('cr',-1),('mr',-1)])   
        
        for records in Jobs_Data:
            if records.get("jti",'') is None:
                records['jti'] = -1
            else:
                pass
            job_title = records.get('jti',-1)
            
            if (records.has_key('jtc') == False and records.has_key('jti') == True ) or (records.get('jtc','') == '' and records['jti'] != -1) :
                job_title_description = job_title_desc_dict[int(records['jti'])]  
            else:
                job_title_description = records.get('jtc','N/A')    
            
            
            if int(records.get('sf')) not in sub_func_dict.keys():
                sub_functional_area = 'Not Present'
                sub_functional_area_lookup = -1
                functional_area = 'Not Present'
                functional_area_lookup = -1
            else:
                if records.get("sf",'') is None:
                    sub_functional_area_lookup = -1
                    functional_area_lookup = -1
                    sub_functional_area = 'Not Present'
                    functional_area = 'Not Present'
                else:   
                    sub_functional_area = sub_func_dict[int(records['sf'])][0]
                    functional_area = sub_func_dict[int(records['sf'])][2]
                    sub_functional_area_lookup = records['sf']
                    functional_area_lookup = sub_func_dict[int(records['sf'])][1]
            
            
            if records.get('i') not in industry_dict.keys():
                    industry = 'No Industry'
                    industry_lookup = -1
                    
            else:
                if records.get("i") is None:
                    industry = 'No Industry'
                    industry_lookup = -1
                else:
                    industry = industry_dict[records['i']]
                    industry_lookup = records['i']
            if records.get('ex') not in experience_dict.keys():
                experience_years = 'Not Present'
            else:
                experience_years = experience_dict[int(records['ex'])]
            #experience_months = records.get('exm','0')
            experience_months = records.get('exm','0')
            if records.get('cn') not in company_dict.keys():
                    company_name = 'Not Present'
            
            elif records.has_key('cn') and records.get('cn')!= 'null' and records.get('cn')!= None and records.get('cn') !=766 and records.get('cn') != 9707233426 and records.get('cn') !=0 and records.get('cn')!= 214001002082409 and  records.get('cn') != 9667524957:
                company_name = company_dict[records['cn']]
            else:
                company_name = records['cnc']    
            
            if records['fcu'] in jobs_dict.keys():
                if records.has_key('cr') == 'False' or str(records.get('cr','')) == '' or str(records.get('cr','')) == 'null':
                    records['cr'] = False
                    jobs_dict[records['fcu']].append([records.get('cr',False),records.get('mr',0),job_title_description,records.get('jti',-1),sub_functional_area,
                                                 sub_functional_area_lookup,functional_area,functional_area_lookup,industry,
                                                 industry_lookup,company_name])
                else:
                    jobs_dict[records['fcu']].append([records.get('cr',False),records.get('mr',0),job_title_description,records.get('jti',-1),sub_functional_area,
                                                 sub_functional_area_lookup,functional_area,functional_area_lookup,industry,
                                                 industry_lookup,company_name])
                #print jobs_dict[records['fcu']]
                
            else:
                jobs_dict[records['fcu']] = [[records.get('cr',False),records.get('mr',0),job_title_description,records.get('jti',-1),sub_functional_area,
                                                 sub_functional_area_lookup,functional_area,functional_area_lookup,industry,
                                                 industry_lookup,company_name]]
        
        
        
        query_values= ''
        for records in Data_list:
            #if records['_id'] == '514052244685223184093244':
            #    continue
            User_Id  = str(records['_id'])
            Email_Id = records['e']
            
            if records.get('ev') is None:
                Email_Verified = 0
            else:
                Email_Verified = records.get('ev',0)
            First_Name = records.get('fn',"N/A").replace('"',"'").encode('ascii','ignore').decode('ascii')
            Last_Name = records.get('ln',"N/A").replace('"',"'").encode('ascii','ignore').decode('ascii')
            CellPhone = records.get('cp',"N/A")
            if records.get('cpv') is None:
                CellPhone_Verified = 0
            else:
                CellPhone_Verified = records.get('cpv',0)
            Resume_Midout = records.get('rm',1)
            Midout = records.get('mo',1)
            if Resume_Midout == 1 and Midout == 1:
                Status = 'Parser Flow Midout Without Resume'
            elif Resume_Midout == 1 and Midout == 2:
                Status = 'Registration Midout Without Resume'
            elif Resume_Midout == 1 and Midout == 0:
                Status = 'Full Profile Without Resume'
            elif Resume_Midout == 0 and Midout == 1:
                 Status = 'Parser Flow Midout With Resume'
            elif Resume_Midout == 0 and Midout == 2:
                Status = 'Registration Midout With Resume'
            else:
                Status = 'Full Profile With Resume'
            #print Status
            
            if records.get('ll','') is None:
                records['ll'] = "1900-01-01 00:00:00"
            else:
                pass
            
            if records.get('lm','') is None:
                records['lm'] = "1900-01-01 00:00:00"
            else:
                pass
            
            if records.get('lad','') is None:
                records['lad'] = "1900-01-01 00:00:00"
            else:
                pass
            
            if records.get('rsd','') is None:
                records['rsd'] = "1900-01-01 00:00:00"
            else:
                pass
            
            if records.get('red','') is None:
                records['red'] = "1900-01-01 00:00:00"
            else:
                pass
            
            if records.get('svi','') is None:
                records['svi'] = "-100"
            else:
                pass
            
            if records.get('evi','') is None:
                records['evi'] = "-100"
            else:
                pass
            
            if records.get('ut','') is None:
                records['ut'] = 0
            else:
                pass
            
            if records.get('sl','') is None:
                records['sl'] = -1
            else:
                pass
            
            if records.get('st','') is None:
                records['st'] = -1
            else:
                pass
            
            if records.get('ss','') is None:
                records['ss'] = 0
            else:
                pass
            
            if records.get('jp','') is None:
                records['jp'] = 0
            else:
                pass
            
            if records.get('bs','') is None:
                records['bs'] = 0
            else:
                pass
            LastLogin_Date = records.get('ll',"1900-01-01 00:00:00")
            LastModified_Date = records.get('lm',"1900-01-01 00:00:00")
            LastApplied_Date = records.get('lad',"1900-01-01 00:00:00")
            RegistrationStart_Date = records.get('rsd',"1900-01-01 00:00:00")
            RegistrationEnd_Date = records.get('red',"1900-01-01 00:00:00")
            StartVendor_Id = records.get('svi','-100')
            EndVendor_Id = records.get('evi','-100')
            User_Source = records.get('ut',0)
            Salary_Lakhs = records.get('sl',-1)
            Salary_Thousand = records.get('st',-1)
            Spam_Status = records.get('ss',0)
            Junk_Profile = records.get('jp',0)
            Bounce_Status = records.get('bs',0)
            Email_Alert_Status = records.get('eas',1)
            SMS_Alert_Status = records.get('saf',1)        
            City_Lookup = str(records.get('cl','-1'))
            Gender = str(records.get('g',-1))
            Experience_Months = str(records.get('exm','0'))

            if  records.has_key('cl') == False:
                City = 'N/A'
                State_Lookup = -1
                State = 'N/A'
            else:
                if records.get('cl','') is None or records['cl'] == 'null' or records['cl'] == -1 or records['cl'] == 'None':
                    City = 'N/A'
                    State_Lookup = -1
                    State = 'N/A'

                else:
                    try:
                        City = city_dict[int(records['cl'])][0]
                        State_Lookup = city_dict[int(records['cl'])][1]
                        State = city_dict[int(records['cl'])][2]
                    except:
                        pass

                if records.get('cl') == 522:
                    City = 'Navi Mumbai'
                    State_Lookup = 1028
                    State = 'Maharashtra'

                if records.get('cl') == 523:
                    City = 'Thane'
                    State_Lookup = 1028
                    State = 'Maharashtra'
                    
            if records.has_key('ex') == False:
                Experience_Years = 'N/A'
            else:
                if records.get('ex','') is None or records['ex'] == 'null' or records['ex'] == -1 or records['ex'] == 'None' :
                    Experience_Years = 'N/A'
                else:
                    try:
                        Experience_Years = experience_dict[int(records['ex'])]
                    except:
                        pass            
             
            try:
                value = education_dict[User_Id]
                education_details = sorted(value,key = operator.itemgetter(0,1),reverse = True)
                Institute_Name = education_details[0][2].replace('"',"'").encode('ascii','ignore').decode('ascii')
                Institute_Name_Lookup = education_details[0][3]
                Course_Type = education_details[0][4]
                Course_Type_Lookup = education_details[0][5]
                Education_Level = education_details[0][6]
                Education_Level_Lookup = education_details[0][7]
                Study_Field = education_details[0][8]
                Study_Field_Lookup = education_details[0][9]    
            except:
                Institute_Name = 'Not Present'
                Institute_Name_Lookup = -1
                Course_Type = 'Not Present'
                Course_Type_Lookup = -1
                Education_Level = 'Not Present'
                Education_Level_Lookup = -1
                Study_Field = 'Not Present'
                Study_Field_Lookup = -1
            
            try:
                value = jobs_dict[User_Id]
                job_detail = sorted(value,key = operator.itemgetter(0,1),reverse = True)
                Job_Title_Description = job_detail[0][2].replace('"',"'").encode('ascii','ignore').decode('ascii')
                Job_Title_Lookup = job_detail[0][3]
                Sub_Functional_Area = job_detail[0][4]
                Sub_Functional_Area_Lookup = job_detail[0][5]
                Functional_Area = job_detail[0][6]
                Functional_Area_Lookup = job_detail[0][7]
                Industry = job_detail[0][8]
                Industry_Lookup = job_detail[0][9]
                Company_Name = job_detail[0][10].replace('"',"'").encode('ascii','ignore').decode('ascii')
            except:
                Job_Title_Description = 'Not Present'
                Job_Title_Lookup =-1
                Sub_Functional_Area = 'Not Present'
                Sub_Functional_Area_Lookup = -1
                Functional_Area = 'Not Present'
                Functional_Area_Lookup = -1
                Industry = 'Not Present'
                Industry_Lookup = -1
                Experience_Years = 'Not Present'
                Experience_Months = 0
                Company_Name = 'Not Present'       
            
            try :
                """
                print "User_Id:" +str(User_Id)
                print  "Email_Id " + str(Email_Id)
                print "Email_Verified" +str(Email_Verified)
                print "First_Name" +str(First_Name)
                print "LastName" +str(Last_Name)
                print "CellPhone" +str(CellPhone)
                print "CellPhone_Verified" +str(CellPhone_Verified)
                print "Resume_Midout" +str(Resume_Midout)
                print "Midout" +str(Midout)
                print "Status" +str(Status)
                print "LastLogin_Date" +str(LastLogin_Date)
                print "LastModified_Date" +str(LastModified_Date)
                print "LastApplied_Date" +str(LastApplied_Date)
                print "RegistrationStart_Date" +str(RegistrationStart_Date)
                print "RegistrationEnd_Date" +str(RegistrationEnd_Date)
                print "StartVendor_Id" +str(StartVendor_Id)
                print "EndVendor_Id" +str(EndVendor_Id)
                print "User_Source" +str(User_Source)
                print "Salary_Lakhs" +str(Salary_Lakhs)
                print "Salary_Thousand" +str(Salary_Thousand)
                print "Spam_Status" +str(Spam_Status)
                print "Junk_Profile" +str(Junk_Profile)
                print "Bounce_Status" +str(Bounce_Status)
                print "Institute_Name" +str(Institute_Name)
                print "Institute_Name_Lookup" +str(Institute_Name_Lookup)
                print "Course_Type" +str(Course_Type)
                print "Course_Type_Lookup" +str(Course_Type_Lookup)
                print "Education_Level" +str(Education_Level)
                print "Education_Level_Lookup" +str(Education_Level_Lookup)
                print "Study_Field" +str(Study_Field)
                print "Study_Field_Lookup" +str(Study_Field_Lookup)
                print "Job_Title_Description" +str(Job_Title_Description)
                print "Job_Title_Lookup" +str(Job_Title_Lookup)
                print "Sub_Functional_Area" +str(Sub_Functional_Area)
                print "Sub_Functional_Area_Lookup" +str(Sub_Functional_Area_Lookup)
                print "Functional_Area" +str(Functional_Area)
                print "Functional_Area_Lookup" +str(Functional_Area_Lookup)
                print "Industry" +str(Industry)
                print "Industry_Lookup" +str(Industry_Lookup)
                print "Experience_Years" +str(Experience_Years)
                print "Experience_Months" +str(Experience_Months)
                print "Company_Name" +str(Company_Name)
                """
                insert_into_table_query = '''INSERT INTO INDExportDB.Candidates_Data_New_V4 VALUES '''
                #query_values = query_values + '''("%s", "%s", %d, "%s", "%s", "%s", %d, %d, %d, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", %d, %d, %d, %d, %d, %d,"%s",%d,"%s",%d,"%s",%d,"%s",%d,"%s",%d,"%s",%d,"%s","%s","%s","%s","%s","%s","%s")''' %  (User_Id , Email_Id , Email_Verified , First_Name.encode('ascii','ignore').decode('ascii') , Last_Name.encode('ascii','ignore').decode('ascii') , CellPhone , CellPhone_Verified , Resume_Midout , Midout , Status , LastLogin_Date , LastModified_Date , LastApplied_Date , RegistrationStart_Date , RegistrationEnd_Date , StartVendor_Id , EndVendor_Id , User_Source , Salary_Lakhs , Salary_Thousand , Spam_Status , Junk_Profile , Bounce_Status , Institute_Name.encode('ascii','ignore').decode('ascii') , Institute_Name_Lookup , Course_Type , Course_Type_Lookup , Education_Level , Education_Level_Lookup , Study_Field , Study_Field_Lookup , Job_Title_Description.encode('ascii','ignore').decode('ascii') , Job_Title_Lookup , Sub_Functional_Area , Sub_Functional_Area_Lookup , Functional_Area , Functional_Area_Lookup , Industry , Industry_Lookup , Experience_Years , Experience_Months , Company_Name.encode('ascii','ignore').decode('ascii')) +','
                query_values = '''("%s","%s", %d, "%s", "%s", "%s", %d, %d, %d,  "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", %d, %d, %d, %d, %d, %d,"%s",%d,"%s",%d,"%s",%d,"%s",%d,"%s",%d,"%s",%d,"%s","%s","%s","%s","%s","%s","%s",%d,%d,"%s","%s","%s",%s,%s)''' %  (User_Id ,Email_Id , Email_Verified , First_Name.encode('ascii','ignore').decode('ascii') , Last_Name.encode('ascii','ignore').decode('ascii') , CellPhone , CellPhone_Verified , Resume_Midout , Midout , Status , LastLogin_Date , LastModified_Date , LastApplied_Date , RegistrationStart_Date , RegistrationEnd_Date , StartVendor_Id , EndVendor_Id , User_Source , Salary_Lakhs , Salary_Thousand , Spam_Status , Junk_Profile , Bounce_Status , Institute_Name.encode('ascii','ignore').decode('ascii') , Institute_Name_Lookup , Course_Type , Course_Type_Lookup , Education_Level , Education_Level_Lookup , Study_Field , Study_Field_Lookup , Job_Title_Description.encode('ascii','ignore').decode('ascii') , Job_Title_Lookup , Sub_Functional_Area , Sub_Functional_Area_Lookup , Functional_Area , Functional_Area_Lookup , Industry , Industry_Lookup , Experience_Years , Experience_Months , Company_Name.encode('ascii','ignore').decode('ascii'),Email_Alert_Status,SMS_Alert_Status,City_Lookup,City.encode('ascii','ignore').decode('ascii'),State.encode('ascii').decode('ascii'),State_Lookup,Gender) +','
                abc = '''ON DUPLICATE KEY UPDATE ''' + ''' Email_Verified= %d, First_Name= "%s", Last_Name= "%s", CellPhone= "%s", CellPhone_Verified= %d, Resume_Midout= %d, Midout= %d,  Status= "%s", LastLogin_Date= "%s", LastModified_Date= "%s", LastApplied_Date= "%s", RegistrationStart_Date= "%s", RegistrationEnd_Date= "%s", StartVendor_Id= "%s", EndVendor_Id= "%s", User_Source= %d, Salary_Lakhs= %d, Salary_Thousand= %d, Spam_Status= %d, Junk_Profile= %d, Bounce_Status= %d,Institute_Name= "%s",Institute_Name_Lookup= %d,Course_Type= "%s",Course_Type_Lookup= %d,Education_Level= "%s",Education_Level_Lookup= %d,Study_Field= "%s",Study_Field_Lookup= %d,Job_Title_Description= "%s",Job_Title_Lookup= %d,Sub_Functional_Area= "%s",Sub_Functional_Area_Lookup= %d,Functional_Area= "%s",Functional_Area_Lookup= "%s",Industry= "%s",Industry_Lookup= "%s",Experience_Years= "%s",Experience_Months= "%s",Company_Name= "%s",Email_Alert_Status= %d,SMS_Alert_Status= %d,City_Lookup = "%s",City = "%s",State = "%s",State_Lookup = "%s",Gender = "%s"''' %  (Email_Verified , First_Name.encode('ascii','ignore').decode('ascii') , Last_Name.encode('ascii','ignore').decode('ascii') , CellPhone , CellPhone_Verified , Resume_Midout , Midout , Status , LastLogin_Date , LastModified_Date , LastApplied_Date , RegistrationStart_Date , RegistrationEnd_Date , StartVendor_Id , EndVendor_Id , User_Source , Salary_Lakhs , Salary_Thousand , Spam_Status , Junk_Profile , Bounce_Status , Institute_Name.encode('ascii','ignore').decode('ascii') , Institute_Name_Lookup , Course_Type , Course_Type_Lookup , Education_Level , Education_Level_Lookup , Study_Field , Study_Field_Lookup , Job_Title_Description.encode('ascii','ignore').decode('ascii') , Job_Title_Lookup , Sub_Functional_Area , Sub_Functional_Area_Lookup , Functional_Area , Functional_Area_Lookup , Industry , Industry_Lookup , Experience_Years , Experience_Months , Company_Name.encode('ascii','ignore').decode('ascii'),Email_Alert_Status,SMS_Alert_Status,City_Lookup,City.encode('ascii','ignore').decode('ascii'),State.encode('ascii').decode('ascii'),State_Lookup,Gender) +',' 
        #print insert_into_table_query + query_values[:-1] + abc[:-1]
                cursor.execute(insert_into_table_query + query_values[:-1] + abc[:-1])
            
            except Exception as e:
                print e
                #print traceback.print_exc()
                
        
        try:
            pass
            #print insert_into_table_query + query_values[:-1] + abc[:-1]
            #cursor.execute(insert_into_table_query + query_values[:-1] + abc[:-1])
            
        except Exception as e:
            print e
            #print query_values
            insert_into_table_query + query_values[:-1] + abc[:-1]
            try:
                ofile.write(insert_into_table_query + query_values[:-1] + abc[:-1])
            except:
                pass
            
            
        mysql_conn.commit()
        id = UserId_list[-1]
        #print id
        education_dict = {}
        jobs_dict = {} 
        print len(UserId_list)
        print 'Last Record Inserted :' +str(id) 
        print "Records Processed : " + str(start)
        o = datetime.now()
        p = o - n
        print p
        #if start == 2000:
        #break                  

def main():
    getDatafromMongo()

if __name__ == '__main__':
    main()
    
